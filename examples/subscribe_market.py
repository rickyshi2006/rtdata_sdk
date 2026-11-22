"""分类订阅开发示例；显式选择分类，导入不会联网。"""

import argparse
import rtdata

if __package__:
    from ._common import connection_options, wait
else:
    from _common import connection_options, wait


def main():
    parser = argparse.ArgumentParser(description="按分类订阅当前目录全部品种")
    parser.add_argument('--market', required=True, choices=(
        'cn_equity', 'hk_equity', 'futures', 'us_equity', 'fx', 'overseas_futures', 'crypto'))
    args = parser.parse_args()
    api = rtdata.API(**connection_options())
    try:
        @api.on_market_feed_status
        def supply_changed(status):
            print('market_feed_status:', status)

        print(api.get_markets())
        sub = api.subscribe_market(args.market)
        print(sub.wait_applied(timeout=30))
        wait(60)
        print(api.market_subscription_status)
        print(api.market_feed_status)
    except KeyboardInterrupt:
        pass
    finally:
        api.close()


if __name__ == '__main__':
    main()
