"""监听 Token 状态。

该示例只建立认证连接，不订阅行情。未收到状态通知时，token_status 保持为 None。
"""

import logging
import time
from datetime import timezone

import rtdata
from rtdata import TokenStatus


TOKEN = "your_token"
API_URL = "https://api.fengv2ray.tk"
LISTEN_SECONDS = 300.0  # 设为 0 表示持续监听


def format_expires_at(status: TokenStatus) -> str:
    if status.expires_at is None:
        return "never"
    return status.expires_at.astimezone(timezone.utc).isoformat()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    api = rtdata.API(token=TOKEN, api_url=API_URL)

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

        deadline = (
            None
            if LISTEN_SECONDS <= 0
            else time.monotonic() + LISTEN_SECONDS
        )
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
