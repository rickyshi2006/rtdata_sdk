"""监听 Cloud Gateway 推送的 Token 状态。

用法:
    python examples/token_status.py --token your_token

该示例只建立认证连接，不订阅行情。需要 Cloud Gateway 开启
TOKEN_STATUS 推送；旧版网关不会发送状态消息，此时 token_status 会保持为 None。
"""

import argparse
import logging
import time
from datetime import timezone

import rtdata
from rtdata import TokenStatus


def format_expires_at(status: TokenStatus) -> str:
    if status.expires_at is None:
        return "never"
    return status.expires_at.astimezone(timezone.utc).isoformat()


def main() -> None:
    parser = argparse.ArgumentParser(description="监听 Token 状态通知")
    parser.add_argument("--token", required=True, help="客户端 Token")
    parser.add_argument(
        "--api-url",
        default=rtdata.api.DEFAULT_API_URL,
        help="服务发现 API 地址",
    )
    parser.add_argument(
        "--seconds",
        type=float,
        default=300.0,
        help="监听时长，默认 300 秒；设为 0 表示持续监听",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    api = rtdata.API(token=args.token, api_url=args.api_url)

    @api.on_connect
    def on_connect() -> None:
        print(">> Connected", flush=True)

    @api.on_disconnect
    def on_disconnect(reason: str) -> None:
        print(f">> Disconnected: {reason}", flush=True)

    @api.on_token_status
    def on_token_status(status: TokenStatus) -> None:
        remaining = status.remaining_ms
        remaining_text = "never" if remaining is None else f"{remaining} ms"
        print(
            ">> Token status: "
            f"status={status.status} "
            f"severity={status.severity} "
            f"reason={status.reason} "
            f"expires_at={format_expires_at(status)} "
            f"remaining={remaining_text} "
            f"message={status.message!r}",
            flush=True,
        )

    try:
        api.connect()

        deadline = None if args.seconds <= 0 else time.monotonic() + args.seconds
        while deadline is None or time.monotonic() < deadline:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping", flush=True)
    finally:
        latest = api.token_status
        if latest is None:
            print(">> No TOKEN_STATUS received", flush=True)
        else:
            print(
                ">> Last token status: "
                f"{latest.status}, expires_at={format_expires_at(latest)}",
                flush=True,
            )
        api.close()


if __name__ == "__main__":
    main()
