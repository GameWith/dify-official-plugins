import os
import threading
import time

from dify_plugin import DifyPluginEnv, Plugin

from slack_socket_mode import default_socket_mode_manager

plugin = Plugin(DifyPluginEnv())

_INNER_API_URL = os.environ.get("DIFY_INNER_API_URL")

# Configure global IO for Socket Mode background invocations
default_socket_mode_manager.configure_runtime(
    reader=plugin.request_reader,
    writer=plugin.default_writer,
    executor=plugin.executer,
    install_method=plugin.config.INSTALL_METHOD,
    # In Dify on AWS, prefer the inner API URL (default: http://localhost:5001)
    dify_plugin_daemon_url=_INNER_API_URL or plugin.config.DIFY_PLUGIN_DAEMON_URL,
)


def _bootstrap_socket_mode() -> None:
    """
    Best-effort bootstrap:
    - wait for plugin IO loop to be ready
    - load stored configs from Dify storage
    - start Socket Mode connections
    """
    try:
        time.sleep(2)
        default_socket_mode_manager.bootstrap_from_storage(
            reader=plugin.request_reader,
            writer=plugin.default_writer,
            executor=plugin.executer,
            install_method=plugin.config.INSTALL_METHOD,
            dify_plugin_daemon_url=_INNER_API_URL or plugin.config.DIFY_PLUGIN_DAEMON_URL,
        )
    except Exception:
        # Never block plugin startup because of Socket Mode bootstrap failures
        pass


if __name__ == '__main__':
    threading.Thread(target=_bootstrap_socket_mode, daemon=True).start()
    plugin.run()
