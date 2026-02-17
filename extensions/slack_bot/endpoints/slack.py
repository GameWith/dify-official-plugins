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

# Storage key prefix for thread_ts -> conversation_id mapping
_THREAD_MAPPING_PREFIX = "slack_thread_mapping:"


class SlackEndpoint(Endpoint):
    def _get_thread_mapping_key(self, thread_ts: str) -> str:
        """Generate storage key for thread_ts -> conversation_id mapping."""
        endpoint_id = self.session.endpoint_id or "default"
        return f"{_THREAD_MAPPING_PREFIX}{endpoint_id}:{thread_ts}"

    def _get_conversation_id(self, thread_ts: str) -> str | None:
        """Get conversation_id for a thread_ts from storage."""
        if not thread_ts:
            return None
        key = self._get_thread_mapping_key(thread_ts)
        try:
            if self.session.storage.exist(key):
                data = self.session.storage.get(key).decode("utf-8", errors="replace")
                return data if data else None
        except Exception:
            pass
        return None

    def _save_conversation_id(self, thread_ts: str, conversation_id: str) -> None:
        """Save conversation_id for a thread_ts to storage."""
        if not thread_ts or not conversation_id:
            return
        key = self._get_thread_mapping_key(thread_ts)
        try:
            self.session.storage.set(key, conversation_id.encode("utf-8"))
            default_socket_mode_manager._push_log(
                self.session.endpoint_id or "default",
                f"thread_mapping: saved {thread_ts} -> {conversation_id}",
            )
        except Exception as exc:
            default_socket_mode_manager._push_log(
                self.session.endpoint_id or "default",
                f"thread_mapping: save failed ({type(exc).__name__}: {exc})",
            )

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

        # Get existing conversation_id for this thread (for context continuity)
        conversation_id = self._get_conversation_id(thread_ts)
        default_socket_mode_manager._push_log(
            self.session.endpoint_id or "default",
            f"thread_context: thread_ts={thread_ts}, existing_conversation_id={conversation_id}",
        )

        # Invoke Dify App using the proper session context
        answer = ""
        new_conversation_id: str | None = None
        try:
            default_socket_mode_manager._push_log(
                self.session.endpoint_id or "default",
                f"internal_invoke: start (app_id={dify_app_id}, query_len={len(query)}, conversation_id={conversation_id})",
            )
            try:
                response = self.session.app.chat.invoke(
                    app_id=dify_app_id,
                    query=query,
                    inputs={},
                    response_mode="blocking",
                    conversation_id=conversation_id or "",
                )
                answer = (response or {}).get("answer") or ""
                new_conversation_id = (response or {}).get("conversation_id")
                default_socket_mode_manager._push_log(
                    self.session.endpoint_id or "default",
                    f"internal_invoke: ok (blocking, answer_len={len(answer)}, new_conversation_id={new_conversation_id})",
                )
            except Exception as exc:
                # Agent Chat App does not support blocking mode -> use streaming
                if "does not support blocking mode" in str(exc):
                    default_socket_mode_manager._push_log(
                        self.session.endpoint_id or "default",
                        "internal_invoke: blocking not supported, retry streaming",
                    )
                    answer, new_conversation_id = self._invoke_app_streaming(
                        app_id=dify_app_id, query=query, conversation_id=conversation_id
                    )
                    default_socket_mode_manager._push_log(
                        self.session.endpoint_id or "default",
                        f"internal_invoke: ok (streaming, answer_len={len(answer)}, new_conversation_id={new_conversation_id})",
                    )
                else:
                    raise

            # Save conversation_id for thread context continuity
            if new_conversation_id and thread_ts:
                self._save_conversation_id(thread_ts, new_conversation_id)

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

    def _invoke_app_streaming(
        self, *, app_id: str, query: str, conversation_id: str | None = None
    ) -> tuple[str, str | None]:
        """
        Invoke Dify App in streaming mode and aggregate answer.
        This is required for Agent Chat Apps which do not support blocking mode.

        Returns:
            tuple[str, str | None]: (answer, conversation_id)
        """
        answer_parts: list[str] = []
        new_conversation_id: str | None = None
        response = self.session.app.chat.invoke(
            app_id=app_id,
            query=query,
            inputs={},
            response_mode="streaming",
            conversation_id=conversation_id or "",
        )
        for data in response:
            if not isinstance(data, dict):
                continue
            event = data.get("event")
            if event in ("agent_message", "message"):
                chunk = data.get("answer") or ""
                if chunk:
                    answer_parts.append(str(chunk))
                # conversation_id is included in message events
                if not new_conversation_id:
                    new_conversation_id = data.get("conversation_id")
            elif event == "message_end":
                # conversation_id is also in message_end event
                if not new_conversation_id:
                    new_conversation_id = data.get("conversation_id")
                break
        return "".join(answer_parts).strip(), new_conversation_id
