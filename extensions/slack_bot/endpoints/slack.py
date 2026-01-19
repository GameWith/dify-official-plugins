import json
import os
from urllib.parse import urlparse, urlunparse
from collections.abc import Mapping

from dify_plugin import Endpoint
from markdown_to_mrkdwn import SlackMarkdownConverter
from slack_sdk import WebClient
from werkzeug import Request, Response

from slack_socket_mode import default_socket_mode_manager

_converter = SlackMarkdownConverter()


class SlackEndpoint(Endpoint):
    def _invoke(self, r: Request, values: Mapping, settings: Mapping) -> Response:
        """
        Socket Mode bootstrap endpoint AND internal invoke endpoint.

        - GET or empty body: Bootstrap Socket Mode connection
        - POST with JSON body: Internal invoke from Socket Mode handler
        """
        # Check if this is an internal invoke request
        try:
            body = r.get_data(as_text=True)
            if body:
                data = json.loads(body)
                if data.get("_internal_invoke"):
                    return self._handle_internal_invoke(data, settings)
        except Exception:
            pass

        # Bootstrap Socket Mode connection
        try:
            # Build invoke URL (POST /invoke) from the current bootstrap URL.
            bootstrap_url = r.url
            if not bootstrap_url:
                host = r.headers.get("Host") or r.headers.get("host") or "localhost"
                scheme = "https" if r.is_secure else "http"
                bootstrap_url = f"{scheme}://{host}{r.path}"
            invoke_url = bootstrap_url.rstrip("/") + "/invoke"

            # Capture routing/auth headers from the bootstrap request so the plugin daemon can
            # route the internal invoke to an available plugin runtime node.
            internal_headers: dict[str, str] = {}
            for k, v in r.headers.items():
                lk = k.lower()
                if lk in {"authorization", "cookie"}:
                    internal_headers[k] = v
                    continue
                if lk.startswith("dify-") or lk.startswith("x-dify-"):
                    internal_headers[k] = v
                    continue
                if lk.startswith("x-forwarded-"):
                    internal_headers[k] = v
                    continue

            # If Dify provided a hook URL, prefer it.
            hook_url = internal_headers.get("Dify-Hook-Url") or internal_headers.get("dify-hook-url")
            if hook_url:
                target = hook_url
                if not target.endswith("/invoke"):
                    target = target.rstrip("/") + "/invoke"
                invoke_url = target

            # Prefer plugin daemon host for internal invoke (SERVER_PORT), not inner API.
            daemon_base = os.environ.get("DIFY_PLUGIN_DAEMON_URL")
            if not daemon_base:
                server_port = os.environ.get("SERVER_PORT") or "5002"
                daemon_base = f"http://localhost:{server_port}"
            if daemon_base:
                try:
                    parsed_target = urlparse(invoke_url)
                    parsed_base = urlparse(daemon_base)
                    invoke_url = urlunparse(
                        (
                            parsed_base.scheme or parsed_target.scheme,
                            parsed_base.netloc or parsed_target.netloc,
                            parsed_target.path,
                            parsed_target.params,
                            parsed_target.query,
                            parsed_target.fragment,
                        )
                    )
                except Exception:
                    pass

            config = default_socket_mode_manager.upsert_from_settings(
                session=self.session,
                settings=settings,
                endpoint_url=invoke_url,
                internal_headers=internal_headers,
            )
            status = default_socket_mode_manager.get_status(config.endpoint_id)
            payload = {
                "ok": True,
                "endpoint_id": config.endpoint_id,
                "socket_mode": "running",
                "status": status,
            }
            return Response(status=200, response=json.dumps(payload), content_type="application/json")
        except Exception as exc:
            payload = {"ok": False, "error": str(exc)}
            return Response(status=400, response=json.dumps(payload), content_type="application/json")

    def _handle_internal_invoke(self, data: dict, settings: Mapping) -> Response:
        """
        Handle internal invoke request from Socket Mode handler.
        This runs in the proper endpoint context where session.app.chat.invoke works.
        """
        query = data.get("query", "")
        channel = data.get("channel", "")
        thread_ts = data.get("thread_ts", "")
        bot_token = (settings.get("bot_token") or "").strip()

        app = settings.get("app") if isinstance(settings.get("app"), Mapping) else {}
        dify_app_id = (app.get("app_id") or "").strip()

        if not query or not channel or not dify_app_id or not bot_token:
            return Response(
                status=400,
                response=json.dumps({"ok": False, "error": "missing required fields"}),
                content_type="application/json",
            )

        # Invoke Dify App using the proper session context
        answer = ""
        try:
            default_socket_mode_manager._push_log(
                self.session.endpoint_id or "default",
                f"internal_invoke: start (app_id={dify_app_id}, query_len={len(query)})",
            )
            try:
                response = self.session.app.chat.invoke(
                    app_id=dify_app_id,
                    query=query,
                    inputs={},
                    response_mode="blocking",
                )
                answer = (response or {}).get("answer") or ""
                default_socket_mode_manager._push_log(
                    self.session.endpoint_id or "default",
                    f"internal_invoke: ok (blocking, answer_len={len(answer)})",
                )
            except Exception as exc:
                # Agent Chat App does not support blocking mode -> use streaming
                if "does not support blocking mode" in str(exc):
                    default_socket_mode_manager._push_log(
                        self.session.endpoint_id or "default",
                        "internal_invoke: blocking not supported, retry streaming",
                    )
                    answer = self._invoke_app_streaming(app_id=dify_app_id, query=query)
                    default_socket_mode_manager._push_log(
                        self.session.endpoint_id or "default",
                        f"internal_invoke: ok (streaming, answer_len={len(answer)})",
                    )
                else:
                    raise
        except Exception as exc:
            default_socket_mode_manager._push_log(
                self.session.endpoint_id or "default",
                f"internal_invoke: failed ({type(exc).__name__}: {exc})",
            )
            answer = f"エラーが発生しました: {exc}"

        # Post to Slack
        try:
            if len(answer) > 2800:
                answer = answer[:2800] + "…"
            formatted = _converter.convert(answer)
            if len(formatted) > 2800:
                formatted = formatted[:2800] + "…"
            blocks = [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": formatted,
                    },
                }
            ]

            web_client = WebClient(token=bot_token)
            web_client.chat_postMessage(
                channel=channel,
                text=formatted,
                blocks=blocks,
                mrkdwn=True,
                thread_ts=thread_ts or None,
            )
            default_socket_mode_manager._push_log(
                self.session.endpoint_id or "default",
                "internal_invoke: slack post ok",
            )
            return Response(
                status=200,
                response=json.dumps({"ok": True, "answer_len": len(answer)}),
                content_type="application/json",
            )
        except Exception as exc:
            default_socket_mode_manager._push_log(
                self.session.endpoint_id or "default",
                f"internal_invoke: slack post failed ({type(exc).__name__}: {exc})",
            )
            return Response(
                status=500,
                response=json.dumps({"ok": False, "error": str(exc)}),
                content_type="application/json",
            )

    def _invoke_app_streaming(self, *, app_id: str, query: str) -> str:
        """
        Invoke Dify App in streaming mode and aggregate answer.
        This is required for Agent Chat Apps which do not support blocking mode.
        """
        answer_parts: list[str] = []
        response = self.session.app.chat.invoke(
            app_id=app_id,
            query=query,
            inputs={},
            response_mode="streaming",
        )
        for data in response:
            if not isinstance(data, dict):
                continue
            event = data.get("event")
            if event in ("agent_message", "message"):
                chunk = data.get("answer") or ""
                if chunk:
                    answer_parts.append(str(chunk))
            elif event == "message_end":
                break
        return "".join(answer_parts).strip()
