"""旧示例名的兼容入口；正式示例见 subscribe_realtime.py，不是自动化测试。"""

if __package__:
    from .subscribe_realtime import main as subscribe
else:
    from subscribe_realtime import main as subscribe


def main():
    subscribe(seconds=15.0)


if __name__ == "__main__":
    main()
