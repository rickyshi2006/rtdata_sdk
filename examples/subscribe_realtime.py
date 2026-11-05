"""多市场实时快照、完整字段和市场当地时间显示。

从环境变量 RTDATA_TOKEN 读取凭据；需要 rtdata v0.3.8 或更高版本，见 README.md。
数量显示为原始整数，不假设股票、外汇、加密货币使用相同单位。
合约到期或目录没有该代码时，按 api.symbols 选择可用品种。
"""

import logging

import rtdata

if __package__:
    from ._common import connection_options, format_quote, require_datetime, wait
else:
    from _common import connection_options, format_quote, require_datetime, wait


SYMBOLS = [
    "601919.SH",
    "000001.SH",
    "AAPL",
    "EUR.USD",
    "BTCUSDT.BINANCE",
    "BTCUSDT.OKX",
    "00700.HK",
    "rb2701.SHF",
    "au2612.SHF",
]
PRINT_FULL_FIELDS = False
LISTEN_SECONDS = 0.0  # 0: 持续监听，Ctrl+C 退出。


def main(seconds=None):
    require_datetime()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    api = rtdata.API(**connection_options())

    @api.on_quote
    def on_quote(q):
        print(format_quote(q))
        if PRINT_FULL_FIELDS:
            print(dict(q.fields))

    @api.on_connect
    def on_connect():
        print(">> Connected")

    @api.on_disconnect
    def on_disconnect(reason):
        print(f">> Disconnected: {reason}")

    @api.on_error
    def on_error(error):
        print(f">> Error: {error}")

    @api.on_realtime_status
    def on_realtime_status(status):
        print(f">> Realtime phase={status.phase} reason={status.reason}")

    try:
        print(f"Subscribing: {SYMBOLS}")
        api.subscribe(SYMBOLS)
        wait(LISTEN_SECONDS if seconds is None else seconds)
    except KeyboardInterrupt:
        pass
    finally:
        print("Confirmed:", api.last_subscribe_confirmed)
        print("Rejected:", api.last_subscribe_rejected)
        print("Warning:", api.last_subscribe_warning)
        api.close()


if __name__ == "__main__":
    main()
