# -*- coding: utf-8 -*-
"""
数据拉取脚本公共工具模块

提供增量下载、断点续传、安全批处理等通用能力，供各 fetch_*.py 脚本复用。
"""

import json
import logging
from datetime import datetime
from calendar import monthrange

import pandas as pd

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────
# 月份迭代
# ──────────────────────────────────────────────


def month_range(start_year, start_month, end_year, end_month, today_int):
    """生成 (year, month, begin_date_int, end_date_int) 序列

    当月的 end_date 精确到今天，避免请求未来数据。
    """
    y, m = start_year, start_month
    while (y, m) <= (end_year, end_month):
        _, days_in_month = monthrange(y, m)
        begin = y * 10000 + m * 100 + 1
        month_end = y * 10000 + m * 100 + days_in_month
        end = (
            min(month_end, today_int) if (y, m) == (end_year, end_month) else month_end
        )
        yield y, m, begin, end
        m += 1
        if m > 12:
            m = 1
            y += 1


# ──────────────────────────────────────────────
# 增量标记 (_SUCCESS)
# ──────────────────────────────────────────────


def is_month_complete(data_dir, y, m):
    """检查该月是否已标记为完整下载（存在 _SUCCESS 文件）"""
    year_dir = data_dir / f"year={y}"
    return (year_dir / f"month={m:02d}_SUCCESS").exists()


def mark_month_complete(data_dir, y, m):
    """标记该月已完整下载"""
    year_dir = data_dir / f"year={y}"
    year_dir.mkdir(parents=True, exist_ok=True)
    (year_dir / f"month={m:02d}_SUCCESS").touch()


# ──────────────────────────────────────────────
# 失败代码记录
# ──────────────────────────────────────────────


def save_failed_codes(failed_dir, y, m, codes):
    """将失败的股票代码落盘，便于后续针对性重试"""
    if not codes:
        return
    failed_dir.mkdir(parents=True, exist_ok=True)
    out = failed_dir / f"failed_codes_{y}-{m:02d}.json"
    existing = []
    if out.exists():
        existing = json.loads(out.read_text())
    merged = sorted(set(existing) | set(codes))
    out.write_text(json.dumps(merged, ensure_ascii=False, indent=2))
    logger.warning(f"[{y}-{m:02d}] {len(codes)} 个批次失败，失败代码已记录到 {out}")


def load_failed_codes(failed_dir, y, m):
    """读取上次运行的失败代码列表，用于优先重试"""
    out = failed_dir / f"failed_codes_{y}-{m:02d}.json"
    if not out.exists():
        return []
    try:
        codes = json.loads(out.read_text())
        if codes:
            logger.info(f"[{y}-{m:02d}] 发现 {len(codes)} 个历史失败代码，优先重试")
        return codes
    except Exception as e:
        logger.warning(f"[{y}-{m:02d}] 读取失败记录出错: {e}")
        return []


def clear_failed_codes(failed_dir, y, m):
    """该月全部成功后清除失败记录文件"""
    out = failed_dir / f"failed_codes_{y}-{m:02d}.json"
    if out.exists():
        out.unlink()
        logger.info(f"[{y}-{m:02d}] 已清除失败记录")


# ──────────────────────────────────────────────
# 安全解析 query_kline 返回值
# ──────────────────────────────────────────────


def safe_parse_batch_result(batch_result, batch):
    """将 query_kline 返回值安全转换为 {code: DataFrame} 字典。

    正常情况 SDK 返回 dict；若返回单一 DataFrame，按 code 列拆分；
    若无 code 列，视为批次失败返回空字典。
    """
    if isinstance(batch_result, dict):
        return batch_result

    if isinstance(batch_result, pd.DataFrame) and not batch_result.empty:
        if "code" in batch_result.columns:
            return {code: group for code, group in batch_result.groupby("code")}
        else:
            logger.error(
                f"  query_kline 返回非 dict 且无 code 列，"
                f"无法拆分 {len(batch)} 个代码的数据，视为批次失败"
            )
            return {}

    logger.warning(f"  query_kline 返回非预期类型: {type(batch_result)}")
    return {}


# ──────────────────────────────────────────────
# 带重试的批次拉取
# ──────────────────────────────────────────────


def fetch_batch_with_retry(
    market_data,
    batch,
    begin,
    end,
    period,
    batch_idx,
    total_batches,
    max_retries=3,
    retry_delay=5,
    request_timeout=60,
):
    """带重试机制的批次数据拉取

    Returns:
        dict: 批次结果，失败返回空字典
    """
    for attempt in range(max_retries):
        try:
            logger.info(
                f"  批次 {batch_idx + 1}/{total_batches}: 拉取 {len(batch)} 个股票 (尝试 {attempt + 1}/{max_retries})"
            )

            import time

            start_time = time.time()

            batch_result = market_data.query_kline(
                batch,
                begin_date=begin,
                end_date=end,
                period=period,
            )

            elapsed = time.time() - start_time
            if elapsed > request_timeout:
                logger.warning(
                    f"  批次 {batch_idx + 1}: 请求耗时 {elapsed:.1f}s 超过超时阈值 {request_timeout}s"
                )

            return safe_parse_batch_result(batch_result, batch)

        except Exception as e:
            elapsed = time.time() - start_time
            logger.warning(
                f"  批次 {batch_idx + 1}: 失败 (尝试 {attempt + 1}/{max_retries}, 耗时 {elapsed:.1f}s): {e}"
            )

            if attempt < max_retries - 1:
                logger.info(f"  批次 {batch_idx + 1}: 等待 {retry_delay}s 后重试...")
                time.sleep(retry_delay)
            else:
                logger.error(f"  批次 {batch_idx + 1}: 达到最大重试次数，跳过此批次")
                return {}


# ──────────────────────────────────────────────
# 通用月份拉取主循环
# ──────────────────────────────────────────────


def _process_and_flush(result, y, m, out_path):
    """将 result 字典中的数据处理后追加写入 parquet 文件。

    处理逻辑：重命名时间列、过滤月份、拼接、去重后写入。
    返回本次写入的行数。
    """
    frames = []
    for code, df in result.items():
        if df is None or df.empty:
            continue

        if "kline_time" in df.columns:
            df.rename(columns={"kline_time": "timestamp"}, inplace=True)
        elif "trade_time" in df.columns:
            df.rename(columns={"trade_time": "timestamp"}, inplace=True)
        elif "timestamp" not in df.columns:
            if isinstance(df.index, pd.DatetimeIndex):
                df = df.reset_index()

        if "timestamp" not in df.columns:
            logger.warning(f"[{code}] 缺少时间列，跳过")
            continue

        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=False)
        if df["timestamp"].dt.tz is not None:
            df["timestamp"] = df["timestamp"].dt.tz_convert("UTC").dt.tz_localize(None)
        df["timestamp"] = df["timestamp"].astype("datetime64[ns]")

        mask = (df["timestamp"].dt.year == y) & (df["timestamp"].dt.month == m)
        df = df[mask]
        if df.empty:
            continue

        if "code" not in df.columns:
            df["code"] = code

        frames.append(df)

    if not frames:
        return 0

    new_data = pd.concat(frames, ignore_index=True)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists():
        existing = pd.read_parquet(out_path)
        merged = pd.concat([existing, new_data], ignore_index=True).drop_duplicates(
            subset=["code", "timestamp"], keep="last"
        )
    else:
        merged = new_data.drop_duplicates(subset=["code", "timestamp"], keep="last")

    merged.to_parquet(out_path, engine="pyarrow", index=False)
    return len(new_data)


def fetch_months(
    market_data,
    codes,
    period,
    data_dir,
    failed_dir,
    start_year,
    start_month,
    batch_size,
    max_retries=3,
    retry_delay=5,
    request_timeout=60,
    flush_every=50,
):
    """通用的按月增量拉取主循环（每 flush_every 个批次写盘一次，避免 OOM）。

    Args:
        market_data: MarketData 实例
        codes: 股票/ETF/期货代码列表
        period: 数据周期（ad.constant.Period.xxx.value）
        data_dir: 数据输出目录（用于 _SUCCESS 标记）
        failed_dir: 失败记录目录
        start_year, start_month: 数据起始年月
        batch_size: 批次大小
        max_retries, retry_delay, request_timeout: 重试配置
        flush_every: 每处理多少个批次就写盘一次（默认 50）
    """
    import time
    import gc

    if not codes:
        logger.error("未获取到任何代码，退出")
        return

    now = datetime.now()
    end_year, end_month = now.year, now.month
    today_int = int(now.strftime("%Y%m%d"))
    logger.info(
        f"数据范围: {start_year}-{start_month:02d} 到 {end_year}-{end_month:02d} (今天: {today_int})"
    )

    data_dir.mkdir(parents=True, exist_ok=True)
    failed_dir.mkdir(parents=True, exist_ok=True)
    total_rows = 0
    total_batches = 0
    failed_batches = 0
    skipped_months = 0

    for y, m, begin, end in month_range(
        start_year, start_month, end_year, end_month, today_int
    ):
        is_current_month = (y, m) == (end_year, end_month)
        if not is_current_month and is_month_complete(data_dir, y, m):
            skipped_months += 1
            continue

        logger.info(f"[{y}-{m:02d}] 拉取 {begin}~{end} ...")
        month_start_time = time.time()
        out_path = data_dir / f"year={y}" / f"month={m:02d}.parquet"

        try:
            result = {}
            batch_failed_codes = []
            prev_failed = load_failed_codes(failed_dir, y, m)
            already_fetched = set()
            month_rows = 0
            flush_count = 0

            def _flush():
                nonlocal result, month_rows, flush_count
                if not result:
                    return
                t1 = time.time()
                rows = _process_and_flush(result, y, m, out_path)
                month_rows += rows
                flush_count += 1
                t2 = time.time()
                logger.info(
                    f"[{y}-{m:02d}] 第 {flush_count} 次写盘: {rows} 行, 累计 {month_rows} 行 (处理 {t2 - t1:.1f}s)"
                )
                result = {}
                gc.collect()

            # 优先重试上次失败的代码
            if prev_failed:
                retry_batches = (len(prev_failed) + batch_size - 1) // batch_size
                logger.info(
                    f"[{y}-{m:02d}] 优先重试 {len(prev_failed)} 个历史失败代码，分 {retry_batches} 批次"
                )
                for i in range(0, len(prev_failed), batch_size):
                    batch = prev_failed[i : i + batch_size]
                    batch_idx = i // batch_size
                    total_batches += 1
                    batch_result = fetch_batch_with_retry(
                        market_data,
                        batch,
                        begin,
                        end,
                        period,
                        batch_idx,
                        retry_batches,
                        max_retries,
                        retry_delay,
                        request_timeout,
                    )
                    if not batch_result:
                        failed_batches += 1
                        batch_failed_codes.extend(batch)
                    else:
                        result.update(batch_result)
                        already_fetched.update(batch_result.keys())
                    if batch_idx < retry_batches - 1:
                        time.sleep(1)

                _flush()

            # 全量拉取，跳过已在重试阶段成功获取的代码
            remaining_codes = [c for c in codes if c not in already_fetched]
            num_batches = (len(remaining_codes) + batch_size - 1) // batch_size
            logger.info(
                f"[{y}-{m:02d}] 全量拉取 {len(remaining_codes)} 个代码（跳过已重试 {len(already_fetched)} 个），分 {num_batches} 批次处理"
            )

            for i in range(0, len(remaining_codes), batch_size):
                batch = remaining_codes[i : i + batch_size]
                batch_idx = i // batch_size
                total_batches += 1

                batch_result = fetch_batch_with_retry(
                    market_data,
                    batch,
                    begin,
                    end,
                    period,
                    batch_idx,
                    num_batches,
                    max_retries,
                    retry_delay,
                    request_timeout,
                )

                if not batch_result:
                    failed_batches += 1
                    batch_failed_codes.extend(batch)
                else:
                    result.update(batch_result)

                # 每 flush_every 个批次写盘一次，释放内存
                if (batch_idx + 1) % flush_every == 0:
                    _flush()

                if batch_idx < num_batches - 1:
                    time.sleep(1)

            # 最终写盘：处理剩余数据
            _flush()

            # 更新失败代码记录
            if batch_failed_codes:
                save_failed_codes(failed_dir, y, m, batch_failed_codes)
            else:
                clear_failed_codes(failed_dir, y, m)

            total_rows += month_rows
            network_time = time.time() - month_start_time
            logger.info(
                f"[{y}-{m:02d}] 完成: 共写入 {month_rows} 行 | 总耗时 {network_time:.1f}s"
            )

            # 历史月份无失败且有数据写出时标记完成
            if not is_current_month and not batch_failed_codes and month_rows > 0:
                mark_month_complete(data_dir, y, m)

        except Exception as e:
            logger.error(f"[{y}-{m:02d}] 失败: {e}", exc_info=True)

    # 输出统计信息
    logger.info(f"\n{'=' * 60}")
    logger.info(f"完成 | 总写入: {total_rows} 行 | 跳过已完成月份: {skipped_months}")
    logger.info(f"总批次数: {total_batches} | 失败批次: {failed_batches}")
    if total_batches > 0:
        logger.info(
            f"成功率: {(total_batches - failed_batches) / total_batches * 100:.1f}%"
        )
    logger.info(f"{'=' * 60}")
