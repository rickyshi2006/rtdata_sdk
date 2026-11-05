"""自动故障迁移和首选节点归位观察示例。

使用默认服务发现和自动重连，被动观察连接状态，不制造故障或强制迁移。
客户端无需重启或重复订阅。
"""

import time

import rtdata


if __package__:
    from ._common import connection_options
else:
    from _common import connection_options

SYMBOLS = ["601919.SH"]
OBSERVE_SECONDS = 600.0  # 设为 0 表示持续运行


def main() -> None:
    api = rtdata.API(
        **connection_options(),
    )

    @api.on_connect
    def on_connect() -> None:
        print(
            ">> connected",
            f"node={api.current_node_id}",
            f"rehome={api.session_rehome_negotiated}",
            flush=True,
        )

    @api.on_disconnect
    def on_disconnect(reason: str) -> None:
        print(f">> disconnected: {reason}", flush=True)

    try:
        api.connect()
        print(
            "session capability:",
            f"state={api.session_capability_state}",
            f"negotiated={api.session_rehome_negotiated}",
            f"fallback={api.session_capability_fallback_reason!r}",
        )
        api.subscribe(SYMBOLS)
        deadline = (
            None
            if OBSERVE_SECONDS <= 0
            else time.monotonic() + OBSERVE_SECONDS
        )
        while deadline is None or time.monotonic() < deadline:
            time.sleep(5)
            print(
                f"session state={api.session_capability_state} "
                f"negotiated={api.session_rehome_negotiated} "
                f"fallback={api.session_capability_fallback_reason!r}",
                flush=True,
            )
    except KeyboardInterrupt:
        print("\nStopping", flush=True)
    finally:
        api.close()


if __name__ == "__main__":
    main()
