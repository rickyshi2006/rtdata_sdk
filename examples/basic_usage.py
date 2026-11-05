"""最小订阅：环境变量 RTDATA_TOKEN，监听 60 秒；需要当前 main 源码。"""

import rtdata

if __package__:
    from ._common import connection_options, format_quote, require_datetime, wait
else:
    from _common import connection_options, format_quote, require_datetime, wait


SYMBOLS = ["601919.SH", "000001.SZ"]
LISTEN_SECONDS = 60.0


def main():
    require_datetime()
    api = rtdata.API(**connection_options())

    @api.on_quote
    def on_quote(q):
        print(format_quote(q))

    try:
        api.subscribe(SYMBOLS)
        wait(LISTEN_SECONDS)
    except KeyboardInterrupt:
        pass
    finally:
        api.close()


if __name__ == "__main__":
    main()
