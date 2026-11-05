# 市场品种分类与代码实例

本文整理 rtdata 的公开市场分类、代码格式及实例，适用于实时订阅和历史查询时选择证券。
时间转换单独见 [时间与市场代码](TIME-AND-MARKETS.md)，字段见 [实时全字段说明](REALTIME-FULL-FIELDS.md)。

**这是代码参考，不是实时可用品种清单。** 实际代码以连接后 `api.symbols` 返回的目录为准：
目录存在不等于账号有权限、正在交易或每种历史周期均可查询。
本文代码来自既有命名约定和已核对的品种资料，并非本次逐一联网验证的结果。

## 一、分类总览

| 市场分类 | 典型品种 | 代码实例 | 实时公开类别 |
| --- | --- | --- | --- |
| 沪深股票 | 主板、科创板、创业板 | `601919.SH`、`688981.SH`、`000001.SZ`、`300750.SZ` | `cn_equity` |
| 北京股票 | 北交所股票 | 从目录筛选 `.BJ`，不按旧号段自行改码 | `cn_equity` |
| 境内指数、场内基金 | 股票指数、ETF | `000001.SH`、`510300.SH`、`159919.SZ` | 对应境内行情类别，非独立交易所 |
| 香港证券 | 港股、港股 ETF | `00700.HK`、`09988.HK`、`02800.HK` | `hk_equity` |
| 美国证券 | 美股、ETF、不同股份类别 | `AAPL`、`MSFT`、`SPY`、`BRK.B` | `us_equity` |
| 国内期货 | 商品、股指、国债期货 | `rb2701.SHF`、`au2612.SHF` | `futures` |
| 国内期权 | 商品期权等 | `y2605-P-9700.DCE`、`fu2609C4800.SHF`（历史格式实例） | 不凭代码保证实时支持 |
| 外汇 | 货币对、贵金属报价、汇率指数 | `EUR.USD`、`USD.JPY`、`XAUUSD.FX`、`USDindex.FX` | `fx` |
| 海外期货 | 股指、能源、金属、农产品等 | `CNmain.SGX`、`CLmain.NYMEX`、`GCmain.COMEX` | `overseas_futures` |
| 加密货币 | 指定平台报价、综合报价 | `BTCUSDT.BINANCE`、`BTCUSDT.OKX`、`BTC.CRYPTO` | `crypto` |
| 加密资产参考指数 | 比特币参考指数 | `BRTI.CFBM` | 单独核对服务能力，不冒充现货或期货 |

公开类别是行情字段分类，不是证券代码后缀，也不是账号授权代码。
指数、ETF、股票可能共享同一交易所后缀；仅凭后缀不能判定证券具体类型。
当前 `api.symbols` 仅提供 `{symbol_id: symbol_code}`，不包含名称、证券类型或合约乘数。

## 二、股票、指数与基金

| 代码 | 含义 | 格式要点 |
| --- | --- | --- |
| `601919.SH` | 中远海控 | 上海证券代码加 `.SH` |
| `600519.SH` | 贵州茅台 | 六位数字保留原样 |
| `688981.SH` | 中芯国际 A 股 | 科创板仍使用 `.SH`，不另加板块后缀 |
| `000001.SH` | 上证指数 | 与 `000001.SZ` 不是同一品种 |
| `510300.SH` | 沪深 300 ETF（上海） | 指数和跟踪它的 ETF 不混用 |
| `000001.SZ` | 平安银行 | 深圳证券代码加 `.SZ` |
| `300750.SZ` | 宁德时代 | 创业板仍使用 `.SZ` |
| `159919.SZ` | 沪深 300 ETF（深圳） | 与上海 ETF 是不同证券 |
| `00700.HK` | 腾讯控股 | 保留港股前导零，不改为 `700.HK` |
| `09988.HK` | 阿里巴巴港股 | 与美股证券不是同一身份 |
| `02800.HK` | 盈富基金 | 港股 ETF 仍用 `.HK` |
| `AAPL` | 苹果 | 沿用目录中的美股代码，不强加 `.US` |
| `MSFT` | 微软 | 与美股 ETF 共用市场代码约定 |
| `SPY` | 标普 500 ETF | 不能当作标普 500 指数本身 |
| `BRK.B` | 伯克希尔 B 类股 | 中间点号属于代码，不是外汇货币分隔的通用标志 |

北交所使用目录中的完整 `代码.BJ`；证券号段可能调整，本手册不制造固定别名。
如果服务目录列出的是 `AAPL.US` 或 `BRK.B.US`，应使用该完整代码；
SDK 能识别其时区不代表会自动给 `AAPL` 和 `AAPL.US` 建立查询别名。

## 三、国内期货与期权

### 交易所后缀

| 交易所 | rtdata 后缀 | 格式实例 | 说明 |
| --- | --- | --- | --- |
| 上海期货交易所 | `.SHF` | `rb2701.SHF`、`au2612.SHF` | 螺纹钢、黄金 |
| 大连商品交易所 | `.DCE` | `i2701.DCE`、`m2701.DCE` | 铁矿石、豆粕 |
| 郑州商品交易所 | `.CZC` | `MA701.CZC` | 三位月份形式仅作格式示例，必须按目录核对 |
| 中国金融期货交易所 | `.CFE` | `IF2612.CFE`、`T2612.CFE` | 沪深 300 股指、十年国债 |
| 上海国际能源交易中心 | `.INE` | `sc2612.INE` | 原油 |
| 广州期货交易所 | `.GFE` | `si2701.GFE`、`lc2701.GFE` | 工业硅、碳酸锂 |

上表月份仅说明格式，不保证对应合约目前挂牌、有活跃行情或历史可查。
保留目录中的品种大小写和月份长度：不要把 `rb` 改为 `RB`，不要自行把三位月份补为四位。
客户端后缀不是交易所长名：例如不要把 `.SHF` 改成 `.SHFE`。

### 期权代码

已有代码可能采用不同的连接符格式：

| 历史格式实例 | 含义 |
| --- | --- |
| `y2605-P-9700.DCE` | 豆油 2605 合约、看跌、行权价 9700 |
| `fu2609C4800.SHF` | 燃料油 2609 合约、看涨、行权价 4800 |

这些是历史代码示例，不应直接当作当前活跃合约订阅。
保留原连接符、`C/P`、行权价文本和月份，不通过字符串拼接生成“统一格式”。
股票期权及其他期权也必须从目录取完整代码；SDK 没有统一的期权代码生成接口。
目录中有期权不代表全部实时通道或历史周期都已支持，非股票品种不要请求前后复权。

## 四、海外期货

格式为 `原始合约代码.交易所后缀`，保留 `main/current/next`、月份和大小写。

| 交易所后缀 | 市场 | 具体代码实例 | 品种说明 |
| --- | --- | --- | --- |
| `.NYMEX` | 纽约商业交易所 | `CL2612.NYMEX`、`CLmain.NYMEX` | WTI 原油 |
| `.COMEX` | 纽约商品交易所 | `GCmain.COMEX`、`SImain.COMEX` | 黄金、白银 |
| `.CBOT` | 芝加哥期货交易所 | `ZCmain.CBOT`、`YMmain.CBOT` | 玉米、道指期货 |
| `.CME` | 芝加哥商业交易所 | `ES2612.CME`、`NQmain.CME`、`BTCmain.CME` | 标普 500、纳指、比特币期货 |
| `.CBOE` | Cboe 波动率期货市场 | `VXmain.CBOE` | VIX 期货 |
| `.HKFE` | 香港期货市场 | `HSImain.HKFE`、`A50main.HKFE` | 恒指、安硕富时 A50 ETF 期货 |
| `.SGX` | 新加坡交易所 | `CNmain.SGX`、`FEFmain.SGX` | 富时中国 A50、铁矿石期货 |
| `.OSE` | 大阪交易所 | `JNImain.OSE` | 日经 225 期货 |
| `.BMD` | 马来西亚衍生品交易所 | `FCPOmain.BMD` | 棕榈油期货 |

需要特别区分：

- `CNmain.SGX` 是新加坡富时中国 A50 期货，不能写成 `A50main.SGX`。
- `A50main.HKFE` 是香港市场的 ETF 期货，不是上一个品种的别名。
- `.CFE` 已用于国内中金所；Cboe 波动率期货使用 `.CBOE`，不能混用。
- `main` 是服务目录中的连续/主力系列标签，不是一个永远不换月的可交易合约。
  不自行改成 `888/999`，也不假设连续序列的换月或价格衔接规则与其他服务相同。
- 国家、标的名称不能推导挂牌交易所。未列出的市场不按经验自行构造后缀。

## 五、外汇、贵金属报价与汇率指数

| 代码实例 | 含义 | 注意 |
| --- | --- | --- |
| `EUR.USD` | 欧元兑美元 | 沿用点分代码，不传 `EURUSD` |
| `USD.JPY` | 美元兑日元 | 基础币与报价币顺序不能交换 |
| `GBP.USD` | 英镑兑美元 | 是否可查以目录为准 |
| `XAUUSD.FX` | 黄金兑美元报价 | 不等于 COMEX 黄金期货 |
| `USDindex.FX` | 美元指数报价 | 保留 `index` 的实际大小写 |

普通货币对沿用 `基础币.报价币`。其他报价使用目录里的原代码，
不机械地截取六字符进行转换。例如目录已有 `XAU.USD` 时保持该代码，
不强制改为 `XAUUSD.FX`；两种形式是否代表相同身份不能由客户端猜测。
外汇盘口量不自动解释为股票“股数”或期货“手数”。

## 六、加密货币与参考指数

### 常见代码实例

| 代码 | 分类与含义 |
| --- | --- |
| `BTCUSDT.BINANCE` | Binance 平台的 BTC/USDT 报价 |
| `BTCUSDT.OKX` | OKX 平台的 BTC/USDT 报价 |
| `BTCUSD.COINBASE` | Coinbase 平台的 BTC/USD 报价 |
| `BTCUSD.HASHKEY` | HashKey 平台的 BTC/USD 报价 |
| `BTC.CRYPTO` | 综合行情中原始代码为 BTC 的条目 |
| `BTCUSD.CRYPTO` | 综合 BTC/USD 报价 |
| `BTCUSDT.CRYPTO` | 综合 BTC/USDT 报价 |
| `BRTI.CFBM` | CME CF 比特币参考指数，不是交易对 |
| `BTCmain.CME` | 比特币期货，属于海外期货，不是加密现货 |

不同平台、不同报价币和不同产品不能合并。特别是：

- `BTCUSDT.BINANCE` 与 `BTCUSDT.OKX` 是两个身份，不能自动合成一个 `BTC/USDT`。
- `USD/USDT/USDC` 不是同一报价币，不能互换。
- `BTC.CRYPTO` 不自动补成 `BTCUSD.CRYPTO`。
- 平台后缀只标识报价归属，不能单凭后缀推断它是现货、永续还是交割合约。
- `.CRYPTO` 表示综合行情，不代表某家撮合交易所；`.CFBM` 表示参考指数系列。

### 已识别的平台后缀

以下名称是当前代码/时区约定可识别的标签，不承诺每个平台当前有可用推送：

| 后缀 | 名称或类别 |
| --- | --- |
| `.BITFLYER` | bitFlyer |
| `.BITFINEX` | Bitfinex |
| `.BITSTAMP` | Bitstamp |
| `.BTCTURK` | BtcTurk |
| `.GEMINI` | Gemini |
| `.HUOBI` | 火币既有代码标签，保留目录命名 |
| `.KUCOIN` | KuCoin |
| `.LBANK` | LBank |
| `.OKX` | OKX |
| `.BINANCE` | Binance |
| `.BINANCEUS` | Binance.US，与 Binance 分开 |
| `.HASHKEY` | HashKey |
| `.COINBASE` | Coinbase |
| `.BITGO` | BitGo 报价标签，不一律视为撮合交易所 |
| `.CRYPTO` | 综合行情 |
| `.CFBM` | 加密资产参考指数 |

未确认平台不根据缩写猜名称，也不在文档中声明已开放。

## 七、从实际目录核对代码

下面只建立连接并读取证券目录，不订阅、不查询历史，也不把内部 symbol_id 写死：

```python
import os
from itertools import islice
import rtdata

wanted = ["601919.SH", "AAPL", "EUR.USD", "BTCUSDT.BINANCE", "BTCUSDT.OKX", "CNmain.SGX"]

with rtdata.API(token=os.environ["RTDATA_TOKEN"]) as api:
    codes = set(api.symbols.values())
    present = [code for code in wanted if code in codes]
    missing = [code for code in wanted if code not in codes]
    print("目录存在:", present)
    print("当前目录缺少:", missing)

    # 只预览有限条目；筛后缀不等于证券类型分类或权限检查。
    for suffix in (".BJ", ".SHF", ".SGX", ".BINANCE"):
        sample = list(islice((code for code in codes if code.endswith(suffix)), 20))
        print(suffix, sample)
```

目录来自当前连接/服务发现及 SDK 缓存，可能随服务目录更新而变化。
目录暂缺时按 [订阅恢复说明](SDK_USAGE.md) 检查，不强制改用看似相似的代码。
正式订阅后继续查看 `last_subscribe_confirmed/last_subscribe_rejected/last_subscribe_warning`，
再等待实际行情；不能把目录预检当作服务端确认。

## 八、代码与时区不是同一件事

上海/深圳/北京/国内期货采用上海时区，香港证券采用香港时区；
美国证券采用纽约时区并处理夏令时，海外期货按交易所映射；
上述已识别外汇、加密报价及参考指数使用 UTC。

这里描述客户端时间输入和显示约定，不改变代码、合约身份或收到的 UTC 毫秒。
详细规则和跨市场显示实例见 [时间说明](TIME-AND-MARKETS.md)。
