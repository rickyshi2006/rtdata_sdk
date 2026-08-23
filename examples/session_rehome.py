"""自动故障迁移和首选节点归位观察示例。

用法:
    python examples/session_rehome.py --token your_token

必须使用 discovery 和 auto reconnect。测试期间由运维侧控制首选节点故障与恢复；
客户端无需重启或重复订阅。
"""

import argparse
import time

import rtdata


def main() -> None:
    parser = argparse.ArgumentParser(description="观察 session rehome")
    parser.add_argument("--token", required=True, help="客户端 token")
    parser.add_argument(
        "--api-url",
        default=rtdata.api.DEFAULT_API_URL,
        help="服务发现 API 地址",
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=["601919.SH"],
        help="需要在迁移后恢复的订阅代码",
    )
    parser.add_argument(
        "--seconds",
        type=float,
        default=600.0,
        help="观察时长；0 表示持续运行",
    )
    args = parser.parse_args()

    api = rtdata.API(
        token=args.token,
        api_url=args.api_url,
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
        api.subscribe(args.symbols)
        deadline = None if args.seconds <= 0 else time.monotonic() + args.seconds
        while deadline is None or time.monotonic() < deadline:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping", flush=True)
    finally:
        api.close()


if __name__ == "__main__":
    main()
