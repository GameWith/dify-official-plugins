from __future__ import annotations

import json
import os
import logging
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Callable, Mapping
from urllib.parse import urlparse, urlunparse
import os

import httpx
from dify_plugin.core.entities.invocation import InvokeType
from dify_plugin.config.logger_format import plugin_logger_handler
from dify_plugin.core.runtime import Session
from markdown_to_mrkdwn import SlackMarkdownConverter
from slack_sdk import WebClient
from slack_sdk.socket_mode import SocketModeClient
from slack_sdk.socket_mode.request import SocketModeRequest
from slack_sdk.socket_mode.response import SocketModeResponse

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(plugin_logger_handler)

_converter = SlackMarkdownConverter()

_INDEX_KEY = "slack_socket_mode:endpoint_ids"
_CONFIG_PREFIX = "slack_socket_mode:config:"


@dataclass(frozen=True)
class SlackSocketModeConfig:
    endpoint_id: str
    app_token: str
    bot_token: str
    dify_app_id: str

    def to_json(self) -> str:
        return json.dumps(
            {
                "endpoint_id": self.endpoint_id,
                "app_token": self.app_token,
                "bot_token": self.bot_token,
                "dify_app_id": self.dify_app_id,
            },
            ensure_ascii=False,
        )

    @staticmethod
    def from_json(data: str) -> "SlackSocketModeConfig":
        obj = json.loads(data)
        return SlackSocketModeConfig(
            endpoint_id=str(obj["endpoint_id"]),
            app_token=str(obj["app_token"]),
            bot_token=str(obj["bot_token"]),
            dify_app_id=str(obj["dify_app_id"]),
        )


class SlackSocketModeRunner:
    def __init__(
        self,
        *,
        config: SlackSocketModeConfig,
        session_factory: Callable[[], Session],
        endpoint_url: str | None = None,
        settings: Mapping[str, Any] | None = None,
        internal_headers: Mapping[str, str] | None = None,
        get_latest_metadata: Callable[[str], dict[str, Any] | None] | None = None,
        on_event: Callable[[str], None] | None = None,
        on_error: Callable[[str, Exception], None] | None = None,
        on_log: Callable[[str, str], None] | None = None,
    ) -> None:
        self._config = config
        self._session_factory = session_factory
        self._endpoint_url = endpoint_url
        self._settings = settings or {}
        self._internal_headers = {str(k): str(v) for k, v in (internal_headers or {}).items()}
        self._get_latest_metadata = get_latest_metadata
        self._on_event = on_event
        self._on_error = on_error
        self._on_log = on_log

        self._lock = threading.RLock()
        self._closed = False

        self._web_client = WebClient(token=config.bot_token)
        self._socket_client = SocketModeClient(
            app_token=config.app_token,
            web_client=self._web_client,
        )
        self._socket_client.socket_mode_request_listeners.append(self._on_socket_mode_request)

        # Execute Dify invocation / Slack posting outside the SocketMode listener thread
        self._executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="slack_socket_mode")
        # Dedicated executor for Dify invocations (avoid deadlock by waiting on same pool)
        self._dify_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="dify_invoke")

    @property
    def config(self) -> SlackSocketModeConfig:
        return self._config

    def start(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._socket_client.connect()
            if self._on_log is not None:
                try:
                    self._on_log(
                        self._config.endpoint_id,
                        f"socket: connected (socket_session_id={self._socket_client.session_id()})",
                    )
                except Exception:
                    pass
            logger.info(
                "Slack Socket Mode connected "
                f"(endpoint_id={self._config.endpoint_id}, session_id={self._socket_client.session_id()})"
            )

    def stop(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            if self._on_log is not None:
                try:
                    self._on_log(self._config.endpoint_id, "socket: stopping")
                except Exception:
                    pass
            try:
                self._socket_client.close()
            except Exception:
                logger.exception("Failed to close SocketModeClient")
            self._executor.shutdown(wait=False, cancel_futures=True)
            self._dify_executor.shutdown(wait=False, cancel_futures=True)

    def _on_socket_mode_request(self, client: SocketModeClient, req: SocketModeRequest) -> None:
        # Always acknowledge immediately
        try:
            client.send_socket_mode_response(SocketModeResponse(envelope_id=req.envelope_id))
            if self._on_log is not None:
                try:
                    self._on_log(self._config.endpoint_id, f"socket: ack ok (type={req.type})")
                except Exception:
                    pass
        except Exception:
            # Even if ack fails, don't crash the process
            logger.exception("Failed to acknowledge Socket Mode request")
            if self._on_log is not None:
                try:
                    self._on_log(self._config.endpoint_id, f"socket: ack failed (type={req.type})")
                except Exception:
                    pass

        # Basic request visibility (helps diagnose "no events")
        try:
            payload_type = None
            if isinstance(req.payload, dict):
                payload_type = req.payload.get("type")
            logger.info(
                "Socket Mode request received "
                f"(endpoint_id={self._config.endpoint_id}, req_type={req.type}, payload_type={payload_type})"
            )
            if self._on_log is not None:
                try:
                    self._on_log(
                        self._config.endpoint_id,
                        f"socket: request received (req_type={req.type}, payload_type={payload_type})",
                    )
                except Exception:
                    pass
        except Exception:
            pass

        # Handle Events API payloads
        if req.type != "events_api":
            return

        payload = req.payload if isinstance(req.payload, dict) else {}
        if payload.get("type") != "event_callback":
            return

        event = payload.get("event") if isinstance(payload.get("event"), dict) else {}
        if event.get("type") != "app_mention":
            return

        # Ignore bot messages / loops
        if event.get("bot_id") or event.get("subtype") == "bot_message":
            return

        if self._on_event is not None:
            try:
                self._on_event(self._config.endpoint_id)
            except Exception:
                # never fail request processing
                pass

        logger.info(
            "Slack app_mention received "
            f"(endpoint_id={self._config.endpoint_id}, channel={event.get('channel')}, user={event.get('user')})"
        )
        if self._on_log is not None:
            try:
                self._on_log(
                    self._config.endpoint_id,
                    f"handler: submit (channel={event.get('channel')}, user={event.get('user')})",
                )
            except Exception:
                pass
        self._executor.submit(self._handle_app_mention, event)

    def _handle_app_mention(self, event: Mapping[str, Any]) -> None:
        if self._on_log is not None:
            try:
                self._on_log(self._config.endpoint_id, "handler: start")
            except Exception:
                pass

        text = (event.get("text") or "").strip()
        if text.startswith("<@"):
            text = text.split("> ", 1)[1] if "> " in text else text
            text = text.strip()
        if not text:
            if self._on_log is not None:
                try:
                    self._on_log(self._config.endpoint_id, "handler: skip (empty text)")
                except Exception:
                    pass
            return

        channel = (event.get("channel") or "").strip()
        if not channel:
            if self._on_log is not None:
                try:
                    self._on_log(self._config.endpoint_id, "handler: skip (missing channel)")
                except Exception:
                    pass
            return
        thread_ts = (event.get("thread_ts") or event.get("ts") or "").strip()

        if self._on_log is not None:
            try:
                self._on_log(
                    self._config.endpoint_id,
                    f"handler: parsed (channel={channel}, thread_ts={thread_ts}, text_len={len(text)})",
                )
            except Exception:
                pass

        # Use internal endpoint invoke (proper session context)
        # Dynamically get endpoint_url from manager in case it was updated after runner started
        endpoint_url = self._endpoint_url
        internal_headers = self._internal_headers
        if self._get_latest_metadata is not None:
            try:
                latest = self._get_latest_metadata(self._config.endpoint_id)
                if latest:
                    endpoint_url = latest.get("endpoint_url") or endpoint_url
                    internal_headers = latest.get("internal_headers") or internal_headers
            except Exception:
                pass

        # If endpoint_url is not ready yet, wait briefly for bootstrap to complete.
        if not endpoint_url and self._get_latest_metadata is not None:
            for _ in range(10):
                time.sleep(0.5)
                try:
                    latest = self._get_latest_metadata(self._config.endpoint_id)
                    if latest and latest.get("endpoint_url"):
                        endpoint_url = latest.get("endpoint_url")
                        internal_headers = latest.get("internal_headers") or internal_headers
                        break
                except Exception:
                    pass

        # If Dify provided a hook URL, prefer it.
        hook_url = None
        if internal_headers:
            hook_url = (
                internal_headers.get("Dify-Hook-Url")
                or internal_headers.get("dify-hook-url")
            )
        if hook_url:
            target = hook_url
            if not target.endswith("/invoke"):
                target = target.rstrip("/") + "/invoke"
            endpoint_url = target

        # Prefer plugin daemon host for internal invoke (SERVER_PORT), not inner API.
        daemon_base = os.environ.get("DIFY_PLUGIN_DAEMON_URL")
        if not daemon_base:
            server_port = os.environ.get("SERVER_PORT") or "5002"
            daemon_base = f"http://localhost:{server_port}"
        if daemon_base and endpoint_url:
            try:
                parsed_target = urlparse(endpoint_url)
                parsed_base = urlparse(daemon_base)
                endpoint_url = urlunparse(
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

        if not endpoint_url:
            if self._on_log is not None:
                try:
                    self._on_log(
                        self._config.endpoint_id,
                        "handler: skip (endpoint_url missing; open the endpoint URL to bootstrap)",
                    )
                except Exception:
                    pass
            # Notify Slack user that bootstrap is required
            try:
                self._web_client.chat_postMessage(
                    channel=channel,
                    text="初期化が必要です。DifyのエンドポイントURLを一度開いてから再度メンションしてください。",
                    mrkdwn=True,
                    thread_ts=thread_ts or None,
                )
            except Exception:
                pass
            return

        # Always prefer internal invoke. Do NOT fall back to legacy background invocation.
        if endpoint_url:
            try:
                if self._on_log is not None:
                    try:
                        header_keys = sorted(list((internal_headers or {}).keys()))
                        self._on_log(
                            self._config.endpoint_id,
                            f"handler: internal_invoke to {endpoint_url} headers={header_keys}",
                        )
                    except Exception:
                        pass

                payload = {
                    "_internal_invoke": True,
                    "query": text,
                    "channel": channel,
                    "thread_ts": thread_ts,
                }
                timeout = httpx.Timeout(120.0, connect=5.0, read=120.0, write=10.0)
                with httpx.Client(timeout=timeout) as client:
                    # Reuse routing/auth headers captured from the bootstrap call.
                    # Ensure we always include the bound session id for daemon routing.
                    headers = dict(internal_headers)
                    try:
                        session = self._session_factory()
                        headers.setdefault("Dify-Plugin-Session-ID", session.session_id)
                    except Exception:
                        pass
                    headers.setdefault("Content-Type", "application/json")
                    resp = client.post(endpoint_url, json=payload, headers=headers)
                    if self._on_log is not None:
                        try:
                            self._on_log(
                                self._config.endpoint_id,
                                f"handler: internal_invoke response {resp.status_code} {resp.text[:100]}",
                            )
                        except Exception:
                            pass
                    # Always stop here (no legacy fallback)
                    if resp.status_code == 200:
                        return
                    # Surface the failure to Slack to avoid silent legacy fallback
                    try:
                        self._web_client.chat_postMessage(
                            channel=channel,
                            text=f"内部呼び出しに失敗しました (status={resp.status_code}): {resp.text[:200]}",
                            mrkdwn=True,
                            thread_ts=thread_ts or None,
                        )
                    except Exception:
                        pass
                    return
            except Exception as exc:
                if self._on_log is not None:
                    try:
                        self._on_log(
                            self._config.endpoint_id,
                            f"handler: internal_invoke error ({type(exc).__name__}: {exc})",
                        )
                    except Exception:
                        pass
                try:
                    self._web_client.chat_postMessage(
                        channel=channel,
                        text=f"内部呼び出し中にエラーが発生しました: {type(exc).__name__}: {exc}",
                        mrkdwn=True,
                        thread_ts=thread_ts or None,
                    )
                except Exception:
                    pass
                return

        # If we couldn't internal-invoke, do not attempt legacy background invocation.
        return

    def _handle_app_mention_legacy(self, text: str, channel: str, thread_ts: str) -> None:
        """Legacy handler using direct Dify invocation (fallback)."""
        answer = ""
        try:
            session = self._session_factory()
            if self._on_log is not None:
                try:
                    self._on_log(
                        self._config.endpoint_id,
                        f"dify: invoke start (legacy, session_id={session.session_id})",
                    )
                except Exception:
                    pass
            t0 = time.monotonic()
            response: dict[str, Any] | None = None
            try:
                response = self._invoke_dify_via_daemon_http(
                    session=session,
                    app_id=self._config.dify_app_id,
                    query=text,
                )
            except Exception as exc:
                if self._on_log is not None:
                    try:
                        self._on_log(
                            self._config.endpoint_id,
                            f"dify: daemon_http failed ({type(exc).__name__}: {exc}) -> fallback to plugin api",
                        )
                    except Exception:
                        pass
                future = self._dify_executor.submit(
                    session.app.chat.invoke,
                    self._config.dify_app_id,
                    text,
                    {},
                    "blocking",
                )
                try:
                    response = future.result(timeout=60)
                except Exception as exc2:
                    try:
                        future.cancel()
                    except Exception:
                        pass
                    raise exc2
            dt = time.monotonic() - t0
            answer = (response or {}).get("answer") or ""
            if self._on_log is not None:
                try:
                    self._on_log(
                        self._config.endpoint_id,
                        f"dify: invoke ok (legacy, elapsed={dt:.2f}s, answer_len={len(answer)})",
                    )
                except Exception:
                    pass
        except Exception as exc:
            logger.exception("Failed to invoke Dify app")
            if self._on_error is not None:
                try:
                    self._on_error(self._config.endpoint_id, exc)
                except Exception:
                    pass
            if self._on_log is not None:
                try:
                    self._on_log(
                        self._config.endpoint_id,
                        f"dify: invoke failed ({type(exc).__name__}: {exc})",
                    )
                except Exception:
                    pass
            answer = f"エラーが発生しました: {exc}"

        # Slack block text limit is 3000 chars (mrkdwn). Keep some margin.
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

        try:
            if self._on_log is not None:
                try:
                    self._on_log(self._config.endpoint_id, "slack: post start (with blocks)")
                except Exception:
                    pass
            self._web_client.chat_postMessage(
                channel=channel,
                text=formatted,
                blocks=blocks,
                mrkdwn=True,
                thread_ts=thread_ts or None,
            )
            logger.info(
                "Slack message sent "
                f"(endpoint_id={self._config.endpoint_id}, channel={channel}, thread_ts={thread_ts})"
            )
            if self._on_log is not None:
                try:
                    self._on_log(self._config.endpoint_id, "slack: post ok (with blocks)")
                except Exception:
                    pass
        except Exception:
            logger.exception("Failed to post message to Slack")
            if self._on_log is not None:
                try:
                    self._on_log(self._config.endpoint_id, "slack: post failed (with blocks)")
                except Exception:
                    pass

    def _invoke_dify_via_daemon_http(self, *, session: Session, app_id: str, query: str) -> dict[str, Any]:
        """
        Invoke Dify App via the plugin daemon HTTP backwards-invocation API.

        - No user-provided URLs (uses session.dify_plugin_daemon_url)
        - Still an App invocation (InvokeType.App)
        - Designed to be safe in background threads
        """
        base = (session.dify_plugin_daemon_url or "").strip()
        if not base:
            raise RuntimeError("missing dify_plugin_daemon_url")

        headers = {"Dify-Plugin-Session-ID": session.session_id}
        # In Dify on AWS, the inner API key may be required for internal endpoints.
        inner_key = os.environ.get("DIFY_INNER_API_KEY") or os.environ.get("SERVER_KEY")
        if inner_key:
            headers["Authorization"] = f"Bearer {inner_key}"
            headers["X-Dify-Inner-Api-Key"] = inner_key
            headers["X-API-Key"] = inner_key
        base = self._resolve_daemon_base(base=base, headers=headers)

        backwards_request_id = uuid.uuid4().hex
        request_payload = {
            "app_id": app_id,
            "query": query,
            "inputs": {},
            "response_mode": "blocking",
            "conversation_id": None,
        }

        payload = session.writer.session_message_text(
            session_id=session.session_id,
            data=session.writer.stream_invoke_object(
                data={
                    "type": InvokeType.App.value,
                    "backwards_request_id": backwards_request_id,
                    "request": request_payload,
                }
            ),
        )

        candidates = self._discover_daemon_transaction_paths(base=base, headers=headers) + [
            # Heuristic fallbacks (older/newer daemon versions / different prefixes)
            "backwards-invocation/transaction",
            "backwards-invocation/transactions",
            "backwards-invocations/transaction",
            "backwards-invocations/transactions",
            "backwards_invocation/transaction",
            "backwards_invocation/transactions",
            "backwards_invocations/transaction",
            "backwards_invocations/transactions",
            "backward-invocation/transaction",
            "backward-invocations/transaction",
            "backward_invocation/transaction",
            "api/backwards-invocation/transaction",
            "api/v1/backwards-invocation/transaction",
            "v1/backwards-invocation/transaction",
        ]

        # AWS Dify: Plugin daemon runs on port 5002 (SERVER_PORT), not 5001 (DIFY_INNER_API_URL)
        # Try port 5002 as well if base is localhost:5001
        daemon_bases = [base]
        parsed = urlparse(base)
        if parsed.hostname in {"localhost", "127.0.0.1"} and parsed.port == 5001:
            daemon_base_5002 = urlunparse((parsed.scheme, f"{parsed.hostname}:5002", "", "", "", ""))
            daemon_bases.append(daemon_base_5002)
            if self._on_log is not None:
                try:
                    self._on_log(self._config.endpoint_id, f"dify: also trying plugin daemon at {daemon_base_5002}")
                except Exception:
                    pass

        last_exc: Exception | None = None
        timeout = httpx.Timeout(30.0, connect=5.0, read=30.0, write=10.0)

        # Try all daemon bases (5001 and 5002 on AWS)
        for daemon_base in daemon_bases:
            for path in candidates:
                url = daemon_base.rstrip("/") + "/" + path.lstrip("/")
                try:
                    if self._on_log is not None:
                        try:
                            self._on_log(self._config.endpoint_id, f"dify: daemon_http try {url}")
                        except Exception:
                            pass

                    with httpx.Client(timeout=timeout) as client:
                        with client.stream("POST", url, headers=headers, content=payload) as resp:
                            if resp.status_code == 404:
                                body = ""
                                try:
                                    body = resp.read().decode("utf-8", errors="replace")[:120]
                                except Exception:
                                    pass
                                raise RuntimeError(f"404 {body}".strip())
                            if resp.status_code >= 400:
                                body = ""
                                try:
                                    body = resp.read().decode("utf-8", errors="replace")[:200]
                                except Exception:
                                    pass
                                raise RuntimeError(f"http_error status={resp.status_code} body={body}")

                            result: dict[str, Any] | None = None
                            for line in resp.iter_lines():
                                if not line:
                                    continue
                                outer = json.loads(line)
                                data = outer.get("data")
                                if not isinstance(data, dict):
                                    continue
                                if data.get("backwards_request_id") != backwards_request_id:
                                    continue

                                event = data.get("event")
                                if event == "error":
                                    raise RuntimeError(str(data.get("message") or "daemon_error"))
                                if event == "end":
                                    break
                                if event == "response":
                                    inner = data.get("data")
                                    if isinstance(inner, dict):
                                        result = inner
                                    break

                            if result is None:
                                raise RuntimeError("no_response_from_daemon")
                            return result

                except Exception as exc:
                    last_exc = exc
                    continue

        # Fallback: try Inner API /chat-messages directly (Dify on AWS style)
        inner_chat_paths = self._inner_api_chat_paths_cache.get(base, [])
        if not inner_chat_paths:
            # Default Inner API paths if swagger didn't find any
            inner_chat_paths = [
                "v1/chat-messages",
                "api/chat-messages",
                "chat-messages",
            ]

        for chat_path in inner_chat_paths:
            url = base.rstrip("/") + "/" + chat_path.lstrip("/")
            try:
                if self._on_log is not None:
                    try:
                        self._on_log(self._config.endpoint_id, f"dify: inner_api try {url}")
                    except Exception:
                        pass

                # Inner API uses different payload format (direct App API)
                inner_payload = {
                    "inputs": {},
                    "query": query,
                    "response_mode": "blocking",
                    "conversation_id": "",
                    "user": "slack_socket_mode",
                }

                with httpx.Client(timeout=timeout) as client:
                    # Inner API requires app_id in URL path or header
                    # Try with X-App-Id header first
                    inner_headers = dict(headers)
                    inner_headers["X-App-Id"] = app_id
                    inner_headers["Content-Type"] = "application/json"

                    resp = client.post(url, headers=inner_headers, json=inner_payload)

                    if resp.status_code == 404:
                        body = resp.text[:120] if resp.text else ""
                        if self._on_log is not None:
                            try:
                                self._on_log(self._config.endpoint_id, f"dify: inner_api 404 {url}")
                            except Exception:
                                pass
                        continue

                    if resp.status_code >= 400:
                        body = resp.text[:200] if resp.text else ""
                        if self._on_log is not None:
                            try:
                                self._on_log(self._config.endpoint_id, f"dify: inner_api error {resp.status_code} {body[:60]}")
                            except Exception:
                                pass
                        continue

                    result = resp.json()
                    if isinstance(result, dict) and "answer" in result:
                        if self._on_log is not None:
                            try:
                                self._on_log(self._config.endpoint_id, f"dify: inner_api ok {url}")
                            except Exception:
                                pass
                        return result

            except Exception as exc:
                if self._on_log is not None:
                    try:
                        self._on_log(self._config.endpoint_id, f"dify: inner_api failed {type(exc).__name__}: {exc}")
                    except Exception:
                        pass
                last_exc = exc
                continue

        raise RuntimeError(f"daemon_http_all_failed: {last_exc}")

    _daemon_path_cache: dict[str, list[str]] = {}
    _daemon_base_cache: dict[str, str] = {}
    _inner_api_chat_paths_cache: dict[str, list[str]] = {}

    def _resolve_daemon_base(self, *, base: str, headers: dict[str, str]) -> str:
        """
        Resolve the correct daemon base URL.

        Some environments do not use the default port (5002). When the configured base returns
        only 404s, we try common localhost ports (5001/5002/5003) and select the first base
        that returns a non-404 response for known probe paths.
        """
        base = base.rstrip("/")
        if base in self._daemon_base_cache:
            return self._daemon_base_cache[base]

        def log(msg: str) -> None:
            if self._on_log is not None:
                try:
                    self._on_log(self._config.endpoint_id, msg)
                except Exception:
                    pass

        parsed = urlparse(base)
        scheme = parsed.scheme or "http"
        host = parsed.hostname or ""
        port = parsed.port

        # If not localhost, don't guess ports.
        is_local = host in {"localhost", "127.0.0.1", "0.0.0.0"} or host.endswith(".local")
        if not is_local:
            self._daemon_base_cache[base] = base
            return base

        probe_paths = ["/openapi.json", "/api/openapi.json", "/v1/openapi.json", "/api/v1/openapi.json", "/health", "/"]
        timeout = httpx.Timeout(2.0, connect=1.0, read=2.0, write=1.0)

        candidate_ports = []
        if port is not None:
            candidate_ports.append(port)
        # Common Dify plugin daemon / sidecar ports
        for p in (5001, 5002, 5003, 8080):
            if p not in candidate_ports:
                candidate_ports.append(p)

        for p in candidate_ports:
            cand = urlunparse((scheme, f"{host}:{p}", "", "", "", ""))
            ok = False
            for path in probe_paths:
                url = cand + path
                try:
                    with httpx.Client(timeout=timeout) as client:
                        r = client.get(url, headers=headers)
                        # Treat any non-404 as "alive" (could be 200/401/403/405)
                        if r.status_code != 404:
                            ok = True
                            log(f"daemon: base resolved -> {cand} (probe {path} -> {r.status_code})")
                            break
                except Exception:
                    continue
            if ok:
                self._daemon_base_cache[base] = cand
                return cand

        # Fall back to the original base if none matched
        log(f"daemon: base unresolved, keep {base}")
        self._daemon_base_cache[base] = base
        return base

    def _discover_daemon_transaction_paths(self, *, base: str, headers: dict[str, str]) -> list[str]:
        """
        Best-effort discovery for daemon back-invoke endpoint.

        - Tries openapi.json for route listing
        - Logs probe results into logs_tail
        """
        base = base.rstrip("/")
        if base in self._daemon_path_cache:
            return self._daemon_path_cache[base]

        discovered: list[str] = []
        inner_api_chat_paths: list[str] = []
        timeout = httpx.Timeout(2.0, connect=1.0, read=2.0, write=1.0)

        def log(msg: str) -> None:
            if self._on_log is not None:
                try:
                    self._on_log(self._config.endpoint_id, msg)
                except Exception:
                    pass

        prefixes = ["", "api", "v1", "api/v1"]

        # quick probes (with session header)
        for prefix in prefixes:
            for p in ["", "health", "openapi.json", "docs", "swagger.json"]:
                try:
                    u = base
                    if prefix:
                        u += "/" + prefix
                    if p:
                        u += "/" + p
                    with httpx.Client(timeout=timeout) as client:
                        r = client.get(u, headers=headers)
                        body = ""
                        try:
                            body = r.text[:60].replace("\n", " ")
                        except Exception:
                            body = ""
                        server = r.headers.get("server") or r.headers.get("Server") or ""
                        log(f"daemon: probe GET {u} -> {r.status_code} server={server} body='{body}'")
                except Exception as exc:
                    log(f"daemon: probe GET {base}/{prefix}/{p} failed ({type(exc).__name__})")

        # openapi discovery (with prefix variants)
        for prefix in prefixes:
            try:
                url = base + ("" if not prefix else "/" + prefix) + "/openapi.json"
                with httpx.Client(timeout=timeout) as client:
                    r = client.get(url, headers=headers)
                    if r.status_code != 200:
                        continue
                    data = r.json()
                    paths = data.get("paths") if isinstance(data, dict) else None
                    if isinstance(paths, dict):
                        for path in paths.keys():
                            if not isinstance(path, str):
                                continue
                            # find likely transaction endpoint
                            if "backward" in path and "transaction" in path:
                                discovered.append(path.lstrip("/"))
                        if discovered:
                            log(f"daemon: discovered paths {discovered[:5]}")
                            break
            except Exception as exc:
                log(f"daemon: openapi parse failed ({type(exc).__name__}: {exc})")

        # swagger discovery (Dify on AWS exposes swagger.json)
        swagger_endpoints = [
            "/swagger.json",
            "/api/swagger.json",
            "/v1/swagger.json",
            "/api/v1/swagger.json",
        ]

        def add_swagger_paths(swagger: dict) -> None:
            base_path = swagger.get("basePath") if isinstance(swagger.get("basePath"), str) else ""
            paths = swagger.get("paths") if isinstance(swagger.get("paths"), dict) else {}
            found: list[str] = []
            chat_paths: list[str] = []
            for p, methods in paths.items():
                if not isinstance(p, str) or not isinstance(methods, dict):
                    continue
                p_low = p.lower()
                # backwards-invocation style paths
                if "backward" in p_low and ("invocation" in p_low or "transaction" in p_low):
                    if any(m.lower() == "post" for m in methods.keys() if isinstance(m, str)):
                        full = (base_path.rstrip("/") + p).lstrip("/")
                        found.append(full)
                # Inner API chat-messages / completion-messages paths
                if "chat-messages" in p_low or "completion-messages" in p_low:
                    if any(m.lower() == "post" for m in methods.keys() if isinstance(m, str)):
                        full = (base_path.rstrip("/") + p).lstrip("/")
                        chat_paths.append(full)
            for f in found:
                discovered.append(f)
            if found:
                log(f"daemon: swagger discovered backwards paths {found[:5]}")
            if chat_paths:
                log(f"daemon: swagger discovered chat paths {chat_paths[:5]}")
                # Store chat paths for direct Inner API invocation
                nonlocal inner_api_chat_paths
                inner_api_chat_paths.extend(chat_paths)

        for ep in swagger_endpoints:
            try:
                url = base + ep
                with httpx.Client(timeout=timeout) as client:
                    r = client.get(url, headers=headers, follow_redirects=True)
                    if r.status_code != 200:
                        continue
                    data = r.json()
                    if isinstance(data, dict):
                        add_swagger_paths(data)
                        # stop early if we found anything
                        if any("backward" in x.lower() and "transaction" in x.lower() for x in discovered):
                            break
            except Exception as exc:
                log(f"daemon: swagger parse failed ({type(exc).__name__}: {exc})")

        # de-dupe while keeping order
        seen: set[str] = set()
        uniq: list[str] = []
        for x in discovered:
            if x not in seen:
                seen.add(x)
                uniq.append(x)

        # Store inner API chat paths for direct invocation fallback
        if inner_api_chat_paths:
            self._inner_api_chat_paths_cache[base] = inner_api_chat_paths

        self._daemon_path_cache[base] = uniq
        return uniq

    # NOTE: do not add Dify public API calls (user requirement: App invocation only)


class SlackSocketModeManager:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._runners: dict[str, SlackSocketModeRunner] = {}
        self._last_error: dict[str, str] = {}
        self._last_event_at: dict[str, float] = {}
        self._base_session: dict[str, dict[str, Any]] = {}
        self._logs: dict[str, list[str]] = {}
        self._log_limit = 200
        # Runtime-level IO (global reader/writer) for background invocation
        self._runtime_reader: Any | None = None
        self._runtime_writer: Any | None = None
        self._runtime_executor: Any | None = None
        self._runtime_install_method: Any | None = None
        self._runtime_dify_plugin_daemon_url: str | None = None

    def configure_runtime(
        self,
        *,
        reader: Any,
        writer: Any,
        executor: Any,
        install_method: Any,
        dify_plugin_daemon_url: str | None,
    ) -> None:
        """
        Configure global IO for background invocation.

        IMPORTANT: Do not use per-request reader/writer in background threads.
        """
        with self._lock:
            self._runtime_reader = reader
            self._runtime_writer = writer
            self._runtime_executor = executor
            self._runtime_install_method = install_method
            self._runtime_dify_plugin_daemon_url = dify_plugin_daemon_url
            self._push_log("runtime", f"runtime configured (daemon_url={dify_plugin_daemon_url})")

    def _push_log(self, endpoint_id: str, message: str) -> None:
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        line = f"{ts} {message}"
        buf = self._logs.setdefault(endpoint_id, [])
        buf.append(line)
        if len(buf) > self._log_limit:
            del buf[: len(buf) - self._log_limit]

    def upsert_from_settings(
        self,
        *,
        session: Session,
        settings: Mapping[str, Any],
        endpoint_url: str | None = None,
        internal_headers: Mapping[str, str] | None = None,
    ) -> SlackSocketModeConfig:
        endpoint_id = (session.endpoint_id or "default").strip()
        app_token = (settings.get("app_token") or "").strip()
        bot_token = (settings.get("bot_token") or "").strip()
        app = settings.get("app") if isinstance(settings.get("app"), Mapping) else {}
        dify_app_id = (app.get("app_id") or "").strip()

        if not app_token or not bot_token or not dify_app_id:
            raise ValueError("missing app_token/bot_token/app.app_id")

        config = SlackSocketModeConfig(
            endpoint_id=endpoint_id,
            app_token=app_token,
            bot_token=bot_token,
            dify_app_id=dify_app_id,
        )

        # Bind a "known-good" session for backwards invocation.
        # In local/remote install modes, backwards invocation responses are routed by session_id.
        # Background threads must reuse a session_id that Dify already recognizes.
        # NOTE: Do NOT store session.reader/session.writer here. Those are often per-request wrappers.
        # We only keep the last bound session_id for observability.
        self._base_session[endpoint_id] = {
            "session_id": session.session_id,
            "endpoint_url": endpoint_url,
            "settings": dict(settings),
            "internal_headers": dict(internal_headers or {}),
        }
        hdr_keys = sorted(list((internal_headers or {}).keys()))
        self._push_log(
            endpoint_id,
            f"bind: bound_dify_session_id={session.session_id}, endpoint_url={endpoint_url}, header_keys={hdr_keys}",
        )

        self._persist_config(session=session, config=config)
        try:
            self._ensure_running(session=session, config=config)
        except Exception as exc:
            self._last_error[endpoint_id] = f"{type(exc).__name__}: {exc}"
            raise
        return config

    def bootstrap_from_storage(
        self,
        *,
        reader: Any,
        writer: Any,
        executor: Any,
        install_method: Any,
        dify_plugin_daemon_url: str | None,
    ) -> None:
        """
        Restore previously saved configs and start Socket Mode connections.

        This requires at least one prior successful configuration save (via endpoint invocation).
        """
        if writer is None:
            logger.info("No default writer found; skipping Socket Mode bootstrap")
            return

        session = Session(
            session_id="slack_socket_mode_bootstrap",
            executor=executor,
            reader=reader,
            writer=writer,
            install_method=install_method,
            dify_plugin_daemon_url=dify_plugin_daemon_url,
            context=None,
        )

        try:
            if not session.storage.exist(_INDEX_KEY):
                return
            raw = session.storage.get(_INDEX_KEY).decode("utf-8", errors="replace")
            endpoint_ids = json.loads(raw)
            if not isinstance(endpoint_ids, list):
                return
        except Exception:
            logger.exception("Failed to load Socket Mode config index from storage")
            return

        for endpoint_id in endpoint_ids:
            try:
                key = _CONFIG_PREFIX + str(endpoint_id)
                if not session.storage.exist(key):
                    continue
                raw_cfg = session.storage.get(key).decode("utf-8", errors="replace")
                cfg = SlackSocketModeConfig.from_json(raw_cfg)
                self._ensure_running(session=session, config=cfg)
            except Exception:
                self._last_error[str(endpoint_id)] = "bootstrap_failed (see logs)"
                logger.exception(f"Failed to bootstrap Socket Mode config for endpoint_id={endpoint_id}")

    def get_status(self, endpoint_id: str) -> dict[str, Any]:
        with self._lock:
            runner = self._runners.get(endpoint_id)
            connected = False
            session_id = None
            if runner:
                try:
                    connected = runner._socket_client.is_connected()  # noqa: SLF001
                    session_id = runner._socket_client.session_id()  # noqa: SLF001
                except Exception:
                    connected = False
            return {
                "endpoint_id": endpoint_id,
                "connected": connected,
                "socket_session_id": session_id,
                "bound_dify_session_id": (self._base_session.get(endpoint_id) or {}).get("session_id"),
                "runtime_configured": bool(self._runtime_reader and self._runtime_writer),
                "dify_plugin_daemon_url": self._runtime_dify_plugin_daemon_url,
                "last_error": self._last_error.get(endpoint_id),
                "last_event_at": self._last_event_at.get(endpoint_id),
                "logs_tail": (self._logs.get(endpoint_id) or [])[-50:],
            }

    def get_latest_metadata(self, endpoint_id: str) -> dict[str, Any] | None:
        """Get the latest endpoint_url and internal_headers for dynamic lookup by runners."""
        with self._lock:
            base_info = self._base_session.get(endpoint_id)
            if not base_info:
                return None
            return {
                "endpoint_url": base_info.get("endpoint_url"),
                "internal_headers": base_info.get("internal_headers") or {},
            }

    def _persist_config(self, *, session: Session, config: SlackSocketModeConfig) -> None:
        try:
            key = _CONFIG_PREFIX + config.endpoint_id
            session.storage.set(key, config.to_json().encode("utf-8"))

            endpoint_ids: list[str] = []
            if session.storage.exist(_INDEX_KEY):
                raw = session.storage.get(_INDEX_KEY).decode("utf-8", errors="replace")
                obj = json.loads(raw)
                if isinstance(obj, list):
                    endpoint_ids = [str(x) for x in obj]

            if config.endpoint_id not in endpoint_ids:
                endpoint_ids.append(config.endpoint_id)
                session.storage.set(_INDEX_KEY, json.dumps(endpoint_ids, ensure_ascii=False).encode("utf-8"))
        except Exception:
            # Don't block runtime if persistence fails; user can still use current connection
            logger.exception("Failed to persist Socket Mode config into storage")

    def _ensure_running(self, *, session: Session, config: SlackSocketModeConfig) -> None:
        def on_event(endpoint_id: str) -> None:
            self._last_event_at[endpoint_id] = time.time()
            self._push_log(endpoint_id, "event: app_mention received")

        def on_error(endpoint_id: str, exc: Exception) -> None:
            self._last_error[endpoint_id] = f"{type(exc).__name__}: {exc}"
            self._push_log(endpoint_id, f"error: {type(exc).__name__}: {exc}")

        def on_log(endpoint_id: str, message: str) -> None:
            self._push_log(endpoint_id, message)

        def session_factory() -> Session:
            # Create a session for background invocation using GLOBAL IO.
            # This avoids per-request reader/writer wrappers that stop receiving events after request completion.
            if not self._runtime_reader or not self._runtime_writer or not self._runtime_executor:
                raise RuntimeError("runtime IO is not configured yet")

            # Prefer bound (Dify-recognized) session id for routing, fallback to stable id.
            bound = (self._base_session.get(config.endpoint_id) or {}).get("session_id")
            session_id = str(bound) if bound else f"slack_socket_mode:{config.endpoint_id}"
            return Session(
                session_id=session_id,
                executor=self._runtime_executor,
                reader=self._runtime_reader,
                writer=self._runtime_writer,
                install_method=self._runtime_install_method,
                dify_plugin_daemon_url=self._runtime_dify_plugin_daemon_url,
                context=None,
            )

        with self._lock:
            current = self._runners.get(config.endpoint_id)

            if current and current.config == config:
                connected = False
                try:
                    connected = current._socket_client.is_connected()  # noqa: SLF001
                except Exception:
                    connected = False

                if connected:
                    # Do not restart on bootstrap; metadata is fetched dynamically on each event.
                    self._push_log(config.endpoint_id, "runner: already running (skip restart)")
                    return

                self._push_log(config.endpoint_id, "runner: restarting (socket disconnected)")
                current.stop()

            if current:
                self._push_log(config.endpoint_id, "runner: restarting (config changed)")
                current.stop()

            # Get endpoint URL and settings for internal invoke
            base_info = self._base_session.get(config.endpoint_id) or {}
            endpoint_url = base_info.get("endpoint_url")
            stored_settings = base_info.get("settings") or {}
            internal_headers = base_info.get("internal_headers") or {}

            runner = SlackSocketModeRunner(
                config=config,
                session_factory=session_factory,
                endpoint_url=endpoint_url,
                settings=stored_settings,
                internal_headers=internal_headers,
                get_latest_metadata=self.get_latest_metadata,
                on_event=on_event,
                on_error=on_error,
                on_log=on_log,
            )
            self._push_log(config.endpoint_id, "runner: starting Socket Mode connection")
            runner.start()
            self._runners[config.endpoint_id] = runner


default_socket_mode_manager = SlackSocketModeManager()


