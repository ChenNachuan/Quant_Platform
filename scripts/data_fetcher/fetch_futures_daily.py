# -*- coding: utf-8 -*-
"""
期货数据拉取（通过米筐本地部署服务器）

使用方式：
    export RQDATAC_URI="tcp://USER:PASSWORD@10.50.0.232:16010"
    python fetch_futures_daily.py

数据存储结构：
    DATA_DIR/
    ├── dominant/                     # 主力连续合约
    │   ├── tick/year=YYYY/          # tick 数据
    │   ├── 1m/year=YYYY/            # 1分钟线
    │   ├── 5m/year=YYYY/            # 5分钟线
    │   ├── 15m/year=YYYY/           # 15分钟线
    │   ├── 30m/year=YYYY/           # 30分钟线
    │   ├── 60m/year=YYYY/           # 60分钟线
    │   └── 1d/year=YYYY/            # 日线
    ├── contracts/                    # 具体合约
    │   ├── tick/year=YYYY/
    │   ├── 1m/year=YYYY/
    │   ├── 5m/year=YYYY/
    │   ├── 15m/year=YYYY/
    │   ├── 30m/year=YYYY/
    │   ├── 60m/year=YYYY/
    │   └── 1d/year=YYYY/
    └── reference/                    # 参考数据
        └── contracts/
            └── contracts_YYYYMMDD.parquet

依赖：
    - rqdatac (米筐 SDK)
    - pandas
    - pyarrow
"""

import os
import logging
import time
from pathlib import Path
from datetime import datetime

import pandas as pd

# ============================================================
# 配置区域 - 根据实际情况修改
# ============================================================

# 米筐服务器地址（优先使用环境变量）
RQDATAC_URI = os.getenv(
    "RQDATAC_URI",
    "tcp://USER:PASSWORD@10.50.0.232:16010",  # 请修改为实际值
)

# 数据存储根目录
DATA_DIR = Path("./data_lake/futures")

# 数据时间范围
START_DATE = "20170101"
END_DATE = "20251231"

# 需要拉取的频率（可按需注释掉不需要的）
FREQUENCIES = [
    # "tick",   # tick 数据（数据量大，按需开启）
    "1m",  # 1分钟线
    "1d",  # 日线
]

# 品种列表（可根据需要增删）
FUTURES_SYMBOLS = [
    # 股指期货
    "IF",  # 沪深300
    "IC",  # 中证500
    "IH",  # 上证50
    "IM",  # 中证1000
    # 商品期货 - 金属
    "CU",  # 铜
    "AL",  # 铝
    "ZN",  # 锌
    "PB",  # 铅
    "NI",  # 镍
    "SN",  # 锡
    "AU",  # 黄金
    "AG",  # 白银
    # 商品期货 - 黑色系
    "RB",  # 螺纹钢
    "HC",  # 热卷
    "SS",  # 不锈钢
    "I",  # 铁矿石
    "J",  # 焦炭
    "JM",  # 焦煤
    # 商品期货 - 能源化工
    "SC",  # 原油
    "FU",  # 燃料油
    "BU",  # 沥青
    "PP",  # 聚丙烯
    "L",  # 塑料
    "V",  # PVC
    "TA",  # PTA
    "MA",  # 甲醇
    "EG",  # 乙二醇
    # 商品期货 - 农产品
    "A",  # 豆一
    "B",  # 豆二
    "M",  # 豆粕
    "Y",  # 豆油
    "P",  # 棕榈油
    "C",  # 玉米
    "CS",  # 淀粉
    "CF",  # 棉花
    "SR",  # 白糖
    "OI",  # 菜油
    "RM",  # 菜粕
    "AP",  # 苹果
    "CJ",  # 红枣
]

# ============================================================
# 日志配置
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ============================================================
# 数据拉取函数
# ============================================================


def init_rqdatac():
    """初始化米筐数据连接"""
    import rqdatac

    if not RQDATAC_URI or "USER:PASSWORD" in RQDATAC_URI:
        raise ValueError(
            "请设置正确的 RQDATAC_URI\n格式：tcp://USER:PASSWORD@10.50.0.232:16010"
        )

    rqdatac.init(RQDATAC_URI)
    server_addr = RQDATAC_URI.split("@")[-1] if "@" in RQDATAC_URI else RQDATAC_URI
    logger.info(f"已连接米筐服务器: {server_addr}")
    return rqdatac


def fetch_dominant_data(rq, symbols, start_date, end_date, frequency="1d"):
    """
    获取主力连续合约数据

    Args:
        rq: rqdatac 模块
        symbols: 品种代码列表
        start_date: 开始日期
        end_date: 结束日期
        frequency: 数据频率 (tick/1m/5m/15m/30m/60m/1d)

    Returns:
        DataFrame
    """
    logger.info(f"获取主力连续合约 {frequency} 数据: {len(symbols)} 个品种")
    all_data = []

    for symbol in symbols:
        try:
            t0 = time.time()
            df = rq.futures.get_dominant_price(
                underlying_symbols=symbol,
                start_date=start_date,
                end_date=end_date,
                frequency=frequency,
                fields=None,
                adjust_type="pre",
                adjust_method="prev_close_spread",
                rule=0,
                rank=1,
            )
            t1 = time.time()

            if df is not None and not df.empty:
                df = df.reset_index()
                df["underlying_symbol"] = symbol
                all_data.append(df)
                logger.info(f"  {symbol}: {len(df)} 行 ({t1 - t0:.1f}s)")
            else:
                logger.warning(f"  {symbol}: 无数据")

        except Exception as e:
            logger.error(f"  {symbol} 失败: {e}")

    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()


def fetch_exchange_data(rq, order_book_ids, start_date, end_date, frequency="1d"):
    """
    获取具体合约数据

    Args:
        rq: rqdatac 模块
        order_book_ids: 合约代码列表
        start_date: 开始日期
        end_date: 结束日期
        frequency: 数据频率

    Returns:
        DataFrame
    """
    logger.info(f"获取具体合约 {frequency} 数据: {len(order_book_ids)} 个合约")
    batch_size = 50
    all_data = []

    for i in range(0, len(order_book_ids), batch_size):
        batch = order_book_ids[i : i + batch_size]
        try:
            t0 = time.time()
            df = rq.futures.get_price(
                order_book_ids=batch,
                start_date=start_date,
                end_date=end_date,
                frequency=frequency,
                fields=None,
                adjust_type="none",
                market="cn",
            )
            t1 = time.time()

            if df is not None and not df.empty:
                df = df.reset_index()
                all_data.append(df)
                logger.info(
                    f"  批次 {i // batch_size + 1}: {len(df)} 行 ({t1 - t0:.1f}s)"
                )

        except Exception as e:
            logger.error(f"  批次 {i // batch_size + 1} 失败: {e}")

    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()


def fetch_contracts(rq, symbols, date):
    """
    获取指定期货品种可交易的合约列表

    Args:
        rq: rqdatac 模块
        symbols: 品种代码列表
        date: 查询日期

    Returns:
        dict: {品种代码: [合约代码列表]}
    """
    logger.info(f"获取可交易合约列表: {len(symbols)} 个品种")
    contracts_dict = {}

    for symbol in symbols:
        try:
            contracts = rq.futures.get_contracts(symbol, date=date)
            contracts_dict[symbol] = contracts
            logger.info(f"  {symbol}: {len(contracts)} 个合约")
        except Exception as e:
            logger.error(f"  {symbol} 失败: {e}")
            contracts_dict[symbol] = []

    return contracts_dict


def get_date_col_name(df, frequency):
    """根据频率返回日期列名"""
    if frequency == "tick":
        return "datetime"
    elif frequency in ["1m", "5m", "15m", "30m", "60m"]:
        return "datetime"
    else:
        return "date"


def save_to_parquet(df, output_dir, frequency="1d"):
    """
    保存 DataFrame 到 Parquet 文件（按年份分区）

    Args:
        df: 要保存的 DataFrame
        output_dir: 输出目录
        frequency: 数据频率
    """
    if df.empty:
        logger.warning("无数据可保存")
        return

    # 确定日期列名
    date_col = get_date_col_name(df, frequency)
    if date_col not in df.columns:
        # 尝试其他常见列名
        for col in ["date", "datetime", "trading_date"]:
            if col in df.columns:
                date_col = col
                break

    # 转换日期列
    if date_col in df.columns:
        df[date_col] = pd.to_datetime(df[date_col])
        df["_year"] = df[date_col].dt.year
    else:
        logger.warning("未找到日期列，跳过分区")
        df["_year"] = 2024  # 默认年份

    # 按年份分组保存
    for year, year_df in df.groupby("_year"):
        year_dir = output_dir / f"year={year}"
        year_dir.mkdir(parents=True, exist_ok=True)
        output_file = year_dir / "data.parquet"

        # 如果文件已存在，合并数据
        if output_file.exists():
            existing = pd.read_parquet(output_file)
            dedup_cols = [
                c
                for c in ["order_book_id", "underlying_symbol", date_col]
                if c in year_df.columns
            ]
            if dedup_cols:
                merged = pd.concat([existing, year_df], ignore_index=True)
                merged = merged.drop_duplicates(subset=dedup_cols, keep="last")
            else:
                merged = pd.concat([existing, year_df], ignore_index=True)
        else:
            merged = year_df

        # 删除辅助列
        if "_year" in merged.columns:
            merged = merged.drop(columns=["_year"])

        merged.to_parquet(output_file, engine="pyarrow", index=False)
        logger.info(f"  {year}: {len(year_df)} 行 -> {output_file}")


# ============================================================
# 主函数
# ============================================================


def main():
    """主函数"""
    # 初始化米筐连接
    rq = init_rqdatac()

    try:
        logger.info(f"数据范围: {START_DATE} ~ {END_DATE}")
        logger.info(f"拉取频率: {', '.join(FREQUENCIES)}")

        # 创建输出目录
        dominant_base = DATA_DIR / "dominant"
        contracts_base = DATA_DIR / "contracts"
        ref_dir = DATA_DIR / "reference" / "contracts"
        ref_dir.mkdir(parents=True, exist_ok=True)

        # 获取最新可交易合约列表（用于具体合约数据）
        latest_date = datetime.now().strftime("%Y%m%d")
        contracts_dict = fetch_contracts(rq, FUTURES_SYMBOLS, latest_date)

        # 收集所有合约代码
        all_contracts = []
        for symbol, contracts in contracts_dict.items():
            all_contracts.extend(contracts)
        logger.info(f"共 {len(all_contracts)} 个合约")

        # 保存合约参考数据
        records = []
        for symbol, contracts in contracts_dict.items():
            for contract in contracts:
                records.append(
                    {
                        "underlying_symbol": symbol,
                        "order_book_id": contract,
                        "query_date": latest_date,
                    }
                )
        if records:
            contracts_df = pd.DataFrame(records)
            output_file = ref_dir / f"contracts_{latest_date}.parquet"
            contracts_df.to_parquet(output_file, engine="pyarrow", index=False)
            logger.info(f"合约列表保存完成: {output_file}")

        # 按频率拉取数据
        for freq in FREQUENCIES:
            logger.info("=" * 60)
            logger.info(f"开始拉取 {freq} 数据")
            logger.info("=" * 60)

            # 1. 主力连续合约数据
            logger.info(f"[{freq}] 获取主力连续合约数据...")
            dominant_dir = dominant_base / freq
            dominant_df = fetch_dominant_data(
                rq, FUTURES_SYMBOLS, START_DATE, END_DATE, frequency=freq
            )
            if not dominant_df.empty:
                save_to_parquet(dominant_df, dominant_dir, frequency=freq)
                logger.info(f"[{freq}] 主力连续合约数据保存完成: {len(dominant_df)} 行")

            # 2. 具体合约数据
            logger.info(f"[{freq}] 获取具体合约数据...")
            contracts_dir = contracts_base / freq
            if all_contracts:
                exchange_df = fetch_exchange_data(
                    rq, all_contracts, START_DATE, END_DATE, frequency=freq
                )
                if not exchange_df.empty:
                    save_to_parquet(exchange_df, contracts_dir, frequency=freq)
                    logger.info(f"[{freq}] 具体合约数据保存完成: {len(exchange_df)} 行")

        logger.info("=" * 60)
        logger.info("所有数据拉取完成！")
        logger.info("=" * 60)

    finally:
        # 断开连接
        try:
            rq.quit()
            logger.info("已断开米筐服务器连接")
        except Exception:
            pass


if __name__ == "__main__":
    main()
