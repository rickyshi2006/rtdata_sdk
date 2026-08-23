"""自动故障迁移和首选节点归位观察示例。

必须使用 discovery 和 auto reconnect。测试期间由运维侧控制首选节点故障与恢复；
客户端无需重启或重复订阅。
"""

import time

import rtdata


TOKEN = "your_token"
API_URL = "https://api.fengv2ray.tk"
SYMBOLS = ["601919.SH"]
OBSERVE_SECONDS = 600.0  # 设为 0 表示持续运行


def main() -> None:
    api = rtdata.API(
        token=TOKEN,
        api_url=API_URL,
        session_rehome_advertise=True,
    )

    @api.on_connect
    def on_connect() -> None:
        print(
            ">> connected",
            f"node={api.current_node_id}",
            f"endpoint={api.current_endpoint}",
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
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping", flush=True)
    finally:
        api.close()


if __name__ == "__main__":
    main()
