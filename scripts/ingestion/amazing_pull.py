"""AmazingData 原始数据拉取脚本（运行于 ``amazingdata`` Docker 容器内）。

容器镜像仅预装标准库、``pandas``、``pyarrow`` 与 ``AmazingData`` SDK，无宿主机 ``.venv``、
``pandera`` 或 ``duckdb``，因此本脚本严格保持零项目内部依赖；
拉取的原始数据按宿主机规范布局原子写入 ``{data_root}/raw/amazingdata/{table}/{YYYY-MM-DD}.parquet``，
SDK 本地缓存默认写入 ``{data_root}/raw/amazingdata/_cache``，
并同步更新 ``{data_root}/raw/amazingdata/.watermarks.json``，供宿主机通过
``fetch_daily.py --from-raw --source amazingdata`` 或交叉校验直接消费。
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Sequence

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

#: AmazingData 限流批量约束：get_option_basic_info 单次上限约 100 条，固定每批 10 个代码
OPTION_BATCH_SIZE = 10
#: 股票扩展信息 / 分钟线批量约束（避免单次请求过大超时）
STOCK_INFO_BATCH_SIZE = 200
#: 分钟线长区间拉取切片交易日数（与 SDK 内部 10 个交易日切片及 hist_code_list 窗口对齐）
MINUTE_CHUNK_DAYS = 10

#: 常用表名别名映射为规范表名
TABLE_ALIASES: dict[str, str] = {
    "calendar": "cn_calendar",
    "cn_calendar": "cn_calendar",
    "stock_basic": "cn_stock_basic",
    "cn_stock_basic": "cn_stock_basic",
    "stock": "cn_stock_bars_daily",
    "bars_daily": "cn_stock_bars_daily",
    "cn_stock_bars_daily": "cn_stock_bars_daily",
    "stock_minute": "cn_stock_bars_minute",
    "bars_minute": "cn_stock_bars_minute",
    "cn_stock_bars_minute": "cn_stock_bars_minute",
    "daily_status": "cn_stock_daily_status",
    "cn_stock_daily_status": "cn_stock_daily_status",
    "corporate_actions": "cn_stock_corporate_actions",
    "cn_stock_corporate_actions": "cn_stock_corporate_actions",
    "etf": "cn_etf_bars_daily",
    "etf_bars_daily": "cn_etf_bars_daily",
    "cn_etf_bars_daily": "cn_etf_bars_daily",
    "index": "cn_stock_index_bars_daily",
    "stock_index_bars_daily": "cn_stock_index_bars_daily",
    "cn_stock_index_bars_daily": "cn_stock_index_bars_daily",
    "option": "option_basic",
    "option_basic": "option_basic",
}

SUPPORTED_TABLES = (
    "cn_calendar",
    "cn_stock_basic",
    "cn_stock_bars_daily",
    "cn_stock_bars_minute",
    "cn_stock_daily_status",
    "cn_stock_corporate_actions",
    "cn_etf_bars_daily",
    "cn_stock_index_bars_daily",
    "option_basic",
)


def parse_date(value: str) -> date:
    """将 ``YYYY-MM-DD`` 或 ``YYYYMMDD`` 解析为 ``datetime.date``。"""
    raw = str(value).strip()
    if len(raw) == 8 and raw.isdigit():
        return datetime.strptime(raw, "%Y%m%d").date()
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError(
            f"日期格式非法: {value!r}（仅支持 YYYY-MM-DD 或 YYYYMMDD）"
        ) from exc


def to_ymd_int(day: date) -> int:
    """将 ``datetime.date`` 转为 SDK 要求的 8 位整型 ``YYYYMMDD``。"""
    return int(day.strftime("%Y%m%d"))


def resolve_tables(raw_tables: str) -> list[str]:
    """解析逗号分隔的表名并校验是否受支持。"""
    resolved: list[str] = []
    seen: set[str] = set()
    for part in raw_tables.split(","):
        token = part.strip()
        if not token:
            continue
        canonical = TABLE_ALIASES.get(token)
        if canonical is None:
            raise ValueError(
                f"不支持的表名 {token!r}，可选: {', '.join(SUPPORTED_TABLES)}"
            )
        if canonical not in seen:
            seen.add(canonical)
            resolved.append(canonical)
    if not resolved:
        raise ValueError("必须至少指定一张有效表")
    return resolved


def raw_target_path(data_root: Path, table: str, day: date) -> Path:
    """返回与宿主机 ``data.raw.raw_path`` 一致的原始落盘路径。"""
    return (
        data_root
        / "raw"
        / "amazingdata"
        / table
        / f"{day.strftime('%Y-%m-%d')}.parquet"
    )


def default_cache_path(data_root: Path) -> Path:
    """返回 AmazingData SDK 本地缓存默认目录（统一归口至 ``raw/amazingdata/_cache``）。"""
    return data_root / "raw" / "amazingdata" / "_cache"


def atomic_write_parquet(df: pd.DataFrame, path: Path) -> Path:
    """原子写入原始 Parquet（``tmp -> fsync -> os.replace``，无外部依赖）。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    table = pa.Table.from_pandas(df, preserve_index=False)
    try:
        pq.write_table(table, tmp, compression="zstd")
        fd = os.open(tmp, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.remove(tmp)
        except FileNotFoundError:
            pass
        raise
    return path


def update_watermark(data_root: Path, table: str, watermark: str) -> None:
    """原子更新 ``{data_root}/raw/amazingdata/.watermarks.json``。"""
    wm_path = data_root / "raw" / "amazingdata" / ".watermarks.json"
    wm_path.parent.mkdir(parents=True, exist_ok=True)
    data: dict[str, Any] = {}
    if wm_path.exists():
        try:
            loaded = json.loads(wm_path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                data = loaded
        except (json.JSONDecodeError, OSError):
            data = {}
    data[table] = watermark
    tmp = wm_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, wm_path)


def _read_credentials() -> tuple[str, str, str, int]:
    """从容器环境变量读取登录凭证（不打印、不落盘）。"""
    username = os.getenv("USER") or os.getenv("AMAZINGDATA_USER")
    password = os.getenv("PASSWORD") or os.getenv("AMAZINGDATA_PASSWORD")
    host = os.getenv("HOST") or os.getenv("AMAZINGDATA_HOST")
    port = os.getenv("PORT") or os.getenv("AMAZINGDATA_PORT")
    if not all([username, password, host, port]):
        raise RuntimeError(
            "未检测到 AmazingData 登录环境变量（USER/PASSWORD/HOST/PORT 或 AMAZINGDATA_*）"
        )
    assert username and password and host and port
    return username, password, host, int(port)


def _normalize_kline_result(result: Any) -> pd.DataFrame:
    """将 ``query_kline`` 返回的 dict 或 DataFrame 统一展开为扁平 DataFrame。"""
    frames: list[pd.DataFrame] = []
    if isinstance(result, dict):
        for code, frame in result.items():
            if not isinstance(frame, pd.DataFrame) or frame.empty:
                continue
            item = frame.copy()
            if "kline_time" not in item.columns and isinstance(
                item.index, (pd.DatetimeIndex, pd.Index)
            ):
                item = item.reset_index()
                if "index" in item.columns and "kline_time" not in item.columns:
                    item = item.rename(columns={"index": "kline_time"})
            if "code" not in item.columns:
                item["code"] = str(code)
            frames.append(item)
    elif isinstance(result, pd.DataFrame) and not result.empty:
        item = result.copy()
        if "kline_time" not in item.columns and isinstance(
            item.index, (pd.DatetimeIndex, pd.Index)
        ):
            item = item.reset_index()
            if "index" in item.columns and "kline_time" not in item.columns:
                item = item.rename(columns={"index": "kline_time"})
        frames.append(item)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _flatten_code_dict_result(
    result: Any, *, code_col: str = "MARKET_CODE"
) -> pd.DataFrame:
    """将 ``dict[code, DataFrame]``（如 ``get_history_stock_status``）展开为扁平 DataFrame。"""
    if isinstance(result, pd.DataFrame):
        return result.copy()
    if not isinstance(result, dict):
        return pd.DataFrame()
    frames: list[pd.DataFrame] = []
    for code, frame in result.items():
        if not isinstance(frame, pd.DataFrame) or frame.empty:
            continue
        item = frame.copy()
        if code_col not in item.columns:
            item[code_col] = str(code)
        frames.append(item)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _pull_calendar(base_data: Any, start: date, end: date) -> pd.DataFrame:
    all_days = base_data.get_calendar()
    frame = pd.DataFrame({"cal_date": list(all_days)})
    # SDK 实测返回 8 位整型 YYYYMMDD（如 20261113），先转字符串再解析避免按纳秒时间戳处理
    cleaned = (
        frame["cal_date"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    )
    frame["trade_date"] = pd.to_datetime(
        cleaned, format="mixed", errors="coerce"
    ).dt.date
    frame = frame.dropna(subset=["trade_date"])
    window = frame[(frame["trade_date"] >= start) & (frame["trade_date"] <= end)]
    return window.sort_values("trade_date").reset_index(drop=True)


def _resolve_code_list(
    base_data: Any,
    security_type: str,
    start: date,
    end: date,
    local_path: str,
    tickers: list[str] | None,
) -> list[str]:
    if tickers:
        return list(tickers)
    if security_type in ("EXTRA_STOCK_A_SH_SZ", "EXTRA_STOCK_A") and hasattr(
        base_data, "get_hist_code_list"
    ):
        try:
            codes = base_data.get_hist_code_list(
                security_type=security_type,
                start_date=to_ymd_int(start),
                end_date=to_ymd_int(end),
                local_path=local_path,
            )
            if codes:
                return list(codes)
        except TypeError:
            codes = base_data.get_hist_code_list(local_path=local_path)
            if codes:
                return list(codes)
    return list(base_data.get_code_list(security_type=security_type))


def _query_kline_with_retry(
    market_data: Any,
    code_list: list[str],
    *,
    begin_date: int,
    end_date: int,
    period: Any,
    max_retries: int = 2,
) -> Any:
    """对单批 ``query_kline`` 调用进行轻量重试，抵御瞬态网络抖动。"""
    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            return market_data.query_kline(
                code_list,
                begin_date=begin_date,
                end_date=end_date,
                period=period,
            )
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt >= max_retries:
                raise
            wait_s = 2.0 * (attempt + 1)
            print(
                f"query_kline 重试 {attempt + 1}/{max_retries} ({begin_date}..{end_date}, "
                f"codes={len(code_list)}): {exc}，等待 {wait_s:.0f}s",
                file=sys.stderr,
                flush=True,
            )
            time.sleep(wait_s)
    assert last_exc is not None
    raise last_exc


def _pull_kline_range(
    ad_mod: Any,
    base_data: Any,
    market_data: Any,
    *,
    security_type: str,
    start: date,
    end: date,
    local_path: str,
    tickers: list[str] | None,
    period: Any | None = None,
    batch_size: int | None = None,
) -> pd.DataFrame:
    codes = _resolve_code_list(
        base_data, security_type, start, end, local_path, tickers
    )
    if not codes:
        return pd.DataFrame()
    use_period = period if period is not None else ad_mod.constant.Period.day.value
    begin_int, end_int = to_ymd_int(start), to_ymd_int(end)
    if batch_size and len(codes) > batch_size:
        frames: list[pd.DataFrame] = []
        for i in range(0, len(codes), batch_size):
            batch = codes[i : i + batch_size]
            raw = _query_kline_with_retry(
                market_data,
                batch,
                begin_date=begin_int,
                end_date=end_int,
                period=use_period,
            )
            part = _normalize_kline_result(raw)
            if not part.empty:
                frames.append(part)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    raw = _query_kline_with_retry(
        market_data,
        codes,
        begin_date=begin_int,
        end_date=end_int,
        period=use_period,
    )
    return _normalize_kline_result(raw)


def _pull_minute_chunked(
    ad_mod: Any,
    base_data: Any,
    market_data: Any,
    *,
    start: date,
    end: date,
    data_root: Path,
    local_path: str,
    tickers: list[str] | None,
    force: bool,
    chunk_days: int = MINUTE_CHUNK_DAYS,
) -> int:
    """按交易日切片分块拉取分钟线并逐块原子落盘（控制内存与支持断点续传）。"""
    cal_df = _pull_calendar(base_data, start, end)
    if not cal_df.empty:
        target_dates = [d for d in cal_df["trade_date"].tolist() if isinstance(d, date)]
    else:
        target_dates = _iter_dates(start, end)

    if not target_dates:
        raise RuntimeError(f"cn_stock_bars_minute 在 {start}..{end} 无交易日")

    step = max(1, int(chunk_days))
    chunks = [target_dates[i : i + step] for i in range(0, len(target_dates), step)]
    total_chunks = len(chunks)
    skipped_chunks = 0
    total_pulled_rows = 0
    total_written_rows = 0
    pull_elapsed = 0.0

    for idx, chunk_dates in enumerate(chunks, start=1):
        c_start, c_end = chunk_dates[0], chunk_dates[-1]
        if not force and all(
            raw_target_path(data_root, "cn_stock_bars_minute", d).exists()
            for d in chunk_dates
        ):
            skipped_chunks += 1
            print(
                f"[min1 {idx}/{total_chunks}] 跳过已存在区间 {c_start}..{c_end} "
                f"({len(chunk_dates)} 日)",
                flush=True,
            )
            continue

        chunk_t0 = time.monotonic()
        chunk_df = _pull_kline_range(
            ad_mod,
            base_data,
            market_data,
            security_type="EXTRA_STOCK_A",
            start=c_start,
            end=c_end,
            local_path=local_path,
            tickers=tickers,
            period=ad_mod.constant.Period.min1.value,
            batch_size=STOCK_INFO_BATCH_SIZE,
        )
        pulled_n = len(chunk_df)
        total_pulled_rows += pulled_n
        written = _save_daily_slices(
            chunk_df,
            table="cn_stock_bars_minute",
            start=c_start,
            end=c_end,
            data_root=data_root,
            force=force,
            dates=chunk_dates,
        )
        total_written_rows += written
        del chunk_df

        dt_chunk = time.monotonic() - chunk_t0
        pull_elapsed += dt_chunk
        active_done = idx - skipped_chunks
        avg_chunk_s = pull_elapsed / active_done if active_done > 0 else 0.0
        eta_h = avg_chunk_s * (total_chunks - idx) / 3600.0
        print(
            f"[min1 {idx}/{total_chunks}] {c_start}..{c_end} "
            f"pulled={pulled_n:,} written={written:,} "
            f"chunk={dt_chunk:.1f}s elapsed={pull_elapsed / 3600.0:.2f}h ETA={eta_h:.2f}h",
            flush=True,
        )

    if total_pulled_rows == 0 and skipped_chunks == 0:
        raise RuntimeError(f"cn_stock_bars_minute 在 {start}..{end} 无返回行")
    return total_written_rows


def _pull_stock_basic(
    base_data: Any,
    info_data: Any,
    local_path: str,
    tickers: list[str] | None,
) -> pd.DataFrame:
    codes = (
        list(tickers)
        if tickers
        else list(base_data.get_code_list(security_type="EXTRA_STOCK_A"))
    )
    if not codes:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    for i in range(0, len(codes), STOCK_INFO_BATCH_SIZE):
        batch = codes[i : i + STOCK_INFO_BATCH_SIZE]
        part = info_data.get_stock_basic(batch)
        if isinstance(part, pd.DataFrame) and not part.empty:
            frames.append(part)
    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["MARKET_CODE"])

    # 叠加北交所新老代码平移表（OLD_CODE -> {OLD_CODE}.BJ, NEW_CODE -> {NEW_CODE}.BJ）
    try:
        bj_df = info_data.get_bj_code_mapping(local_path=local_path, is_local=False)
    except TypeError:
        bj_df = info_data.get_bj_code_mapping(is_local=False)
    if isinstance(bj_df, pd.DataFrame) and not bj_df.empty:
        bj_work = bj_df.copy()
        new_raw = (
            bj_work["NEW_CODE"]
            .astype(str)
            .str.strip()
            .str.replace(r"\.0$", "", regex=True)
        )
        old_raw = (
            bj_work["OLD_CODE"]
            .astype(str)
            .str.strip()
            .str.replace(r"\.0$", "", regex=True)
        )
        bj_work["MARKET_CODE"] = new_raw.map(lambda c: c if "." in c else f"{c}.BJ")
        bj_work["OLD_CODE"] = old_raw.map(
            lambda c: (
                None if not c or c.lower() == "nan" else (c if "." in c else f"{c}.BJ")
            )
        )
        bj_map = (
            bj_work[["MARKET_CODE", "OLD_CODE"]]
            .dropna(subset=["OLD_CODE"])
            .drop_duplicates(subset=["MARKET_CODE"])
        )
        df = df.merge(bj_map, on="MARKET_CODE", how="left")
    else:
        df["OLD_CODE"] = None

    return df.reset_index(drop=True)


def _pull_corporate_actions(
    base_data: Any,
    info_data: Any,
    local_path: str,
    end: date,
    tickers: list[str] | None,
) -> pd.DataFrame:
    codes = (
        list(tickers)
        if tickers
        else list(base_data.get_code_list(security_type="EXTRA_STOCK_A"))
    )
    if not codes:
        return pd.DataFrame()

    end_ymd = end.strftime("%Y%m%d")
    div_frames: list[pd.DataFrame] = []
    ri_frames: list[pd.DataFrame] = []
    for i in range(0, len(codes), STOCK_INFO_BATCH_SIZE):
        batch = codes[i : i + STOCK_INFO_BATCH_SIZE]
        try:
            dv = info_data.get_dividend(batch, local_path=local_path, is_local=False)
        except TypeError:
            dv = info_data.get_dividend(batch, is_local=False)
        if isinstance(dv, pd.DataFrame) and not dv.empty:
            div_frames.append(dv)

        try:
            ri = info_data.get_right_issue(batch, local_path=local_path, is_local=False)
        except TypeError:
            ri = info_data.get_right_issue(batch, is_local=False)
        if isinstance(ri, pd.DataFrame) and not ri.empty:
            ri_frames.append(ri)

    unified_parts: list[pd.DataFrame] = []
    if div_frames:
        div_all = pd.concat(div_frames, ignore_index=True)
        if "DIV_PROGRESS" in div_all.columns:
            div_all = div_all[
                div_all["DIV_PROGRESS"].astype(str).str.strip() == "3"
            ].copy()
        if not div_all.empty and "DATE_EX" in div_all.columns:
            ex_str = (
                div_all["DATE_EX"]
                .astype(str)
                .str.strip()
                .str.replace(r"\.0$", "", regex=True)
            )
            valid = ex_str.str.match(r"^\d{8}$") & (ex_str <= end_ymd)
            div_all = div_all.loc[valid].copy()
        if not div_all.empty:
            ann_series = div_all.get("ANN_DATE")
            if ann_series is None and "DATE_DVD_ANN" in div_all.columns:
                ann_series = div_all["DATE_DVD_ANN"]
            elif ann_series is not None and "DATE_DVD_ANN" in div_all.columns:
                ann_series = ann_series.fillna(div_all["DATE_DVD_ANN"])
            unified_parts.append(
                pd.DataFrame(
                    {
                        "MARKET_CODE": div_all["MARKET_CODE"].astype(str).values,
                        "_action_kind": "dividend",
                        "EX_DATE": div_all["DATE_EX"].astype(str).values,
                        "ANN_DATE": (
                            ann_series.astype(object).values
                            if ann_series is not None
                            else None
                        ),
                        "RECORD_DATE": (
                            div_all["DATE_EQY_RECORD"].astype(object).values
                            if "DATE_EQY_RECORD" in div_all.columns
                            else None
                        ),
                        "CASH_DIVIDEND": pd.to_numeric(
                            div_all.get("DVD_PER_SHARE_PRE_TAX_CASH"), errors="coerce"
                        ).values,
                        "STOCK_DIVIDEND": pd.to_numeric(
                            div_all.get("DVD_PER_SHARE_STK"), errors="coerce"
                        ).values,
                        "BONUS_RATE": pd.to_numeric(
                            div_all.get("DIV_BONUSRATE"), errors="coerce"
                        ).values,
                        "CONVERSED_RATE": pd.to_numeric(
                            div_all.get("DIV_CONVERSEDRATE"), errors="coerce"
                        ).values,
                        "RIGHTS_PRICE": float("nan"),
                        "RIGHTS_RATIO": float("nan"),
                        "REMARK": (
                            div_all["REMARK"].astype(object).values
                            if "REMARK" in div_all.columns
                            else None
                        ),
                    }
                )
            )

    if ri_frames:
        ri_all = pd.concat(ri_frames, ignore_index=True)
        if "PROGRESS" in ri_all.columns:
            ri_all = ri_all[ri_all["PROGRESS"].astype(str).str.strip() == "3"].copy()
        if not ri_all.empty and "EX_DIVIDEND_DATE" in ri_all.columns:
            ex_str = (
                ri_all["EX_DIVIDEND_DATE"]
                .astype(str)
                .str.strip()
                .str.replace(r"\.0$", "", regex=True)
            )
            valid = ex_str.str.match(r"^\d{8}$") & (ex_str <= end_ymd)
            ri_all = ri_all.loc[valid].copy()
        if not ri_all.empty:
            unified_parts.append(
                pd.DataFrame(
                    {
                        "MARKET_CODE": ri_all["MARKET_CODE"].astype(str).values,
                        "_action_kind": "rights",
                        "EX_DATE": ri_all["EX_DIVIDEND_DATE"].astype(str).values,
                        "ANN_DATE": (
                            ri_all["ANN_DATE"].astype(object).values
                            if "ANN_DATE" in ri_all.columns
                            else None
                        ),
                        "RECORD_DATE": (
                            ri_all["SHAREB_REG_DATE"].astype(object).values
                            if "SHAREB_REG_DATE" in ri_all.columns
                            else None
                        ),
                        "CASH_DIVIDEND": float("nan"),
                        "STOCK_DIVIDEND": float("nan"),
                        "BONUS_RATE": float("nan"),
                        "CONVERSED_RATE": float("nan"),
                        "RIGHTS_PRICE": pd.to_numeric(
                            ri_all.get("PRICE"), errors="coerce"
                        ).values,
                        "RIGHTS_RATIO": pd.to_numeric(
                            ri_all.get("RATIO"), errors="coerce"
                        ).values,
                        "REMARK": (
                            ri_all["RIGHTSISSUE_DESC"].astype(object).values
                            if "RIGHTSISSUE_DESC" in ri_all.columns
                            else None
                        ),
                    }
                )
            )

    if not unified_parts:
        return pd.DataFrame()
    return pd.concat(unified_parts, ignore_index=True)


def _pull_daily_status(
    ad_mod: Any,
    base_data: Any,
    info_data: Any,
    market_data: Any,
    *,
    start: date,
    end: date,
    local_path: str,
    tickers: list[str] | None,
) -> pd.DataFrame:
    codes = (
        list(tickers)
        if tickers
        else list(base_data.get_code_list(security_type="EXTRA_STOCK_A"))
    )
    if not codes:
        return pd.DataFrame()

    status_frames: list[pd.DataFrame] = []
    kline_frames: list[pd.DataFrame] = []
    begin_int, end_int = to_ymd_int(start), to_ymd_int(end)

    for i in range(0, len(codes), STOCK_INFO_BATCH_SIZE):
        batch = codes[i : i + STOCK_INFO_BATCH_SIZE]
        try:
            st_raw = info_data.get_history_stock_status(
                batch,
                local_path=local_path,
                is_local=False,
                begin_date=begin_int,
                end_date=end_int,
            )
        except TypeError:
            st_raw = info_data.get_history_stock_status(
                batch,
                is_local=False,
                begin_date=begin_int,
                end_date=end_int,
            )
        st_df = _flatten_code_dict_result(st_raw, code_col="MARKET_CODE")
        if not st_df.empty:
            status_frames.append(st_df)

        kl_raw = market_data.query_kline(
            batch,
            begin_date=begin_int,
            end_date=end_int,
            period=ad_mod.constant.Period.day.value,
        )
        kl_df = _normalize_kline_result(kl_raw)
        if not kl_df.empty:
            kline_frames.append(kl_df)

    if not status_frames:
        return pd.DataFrame()

    status_df = pd.concat(status_frames, ignore_index=True)
    status_df["_ymd"] = (
        status_df["TRADE_DATE"]
        .astype(str)
        .str.strip()
        .str.replace("-", "", regex=False)
    )
    if kline_frames:
        kl_all = pd.concat(kline_frames, ignore_index=True)
        kl_all["MARKET_CODE"] = kl_all["code"].astype(str)
        kl_all["_ymd"] = pd.to_datetime(
            kl_all["kline_time"], errors="coerce"
        ).dt.strftime("%Y%m%d")
        kl_sub = kl_all[
            ["MARKET_CODE", "_ymd", "high", "low", "close"]
        ].drop_duplicates(subset=["MARKET_CODE", "_ymd"])
        status_df = status_df.merge(kl_sub, on=["MARKET_CODE", "_ymd"], how="left")

    return status_df.drop(columns=["_ymd"]).reset_index(drop=True)


def _pull_option_basic(base_data: Any, info_data: Any, local_path: str) -> pd.DataFrame:
    try:
        all_codes = list(base_data.get_option_code_list(local_path=local_path))
    except TypeError:
        all_codes = list(base_data.get_option_code_list())
    frames: list[pd.DataFrame] = []
    for i in range(0, len(all_codes), OPTION_BATCH_SIZE):
        batch = all_codes[i : i + OPTION_BATCH_SIZE]
        try:
            part = info_data.get_option_basic_info(
                batch, local_path=local_path, is_local=False
            )
        except TypeError:
            part = info_data.get_option_basic_info(batch, is_local=False)
        if isinstance(part, pd.DataFrame) and not part.empty:
            frames.append(part)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _iter_dates(start: date, end: date) -> list[date]:
    days = (end - start).days + 1
    return [start + timedelta(days=i) for i in range(days)]


def _save_daily_slices(
    df: pd.DataFrame,
    *,
    table: str,
    start: date,
    end: date,
    data_root: Path,
    force: bool,
    dates: Sequence[date] | None = None,
) -> int:
    if df.empty:
        return 0
    date_col = next(
        (c for c in ("kline_time", "TRADE_DATE", "trade_date") if c in df.columns),
        None,
    )
    if date_col is None:
        out_path = raw_target_path(data_root, table, end)
        if force or not out_path.exists():
            atomic_write_parquet(df, out_path)
            update_watermark(data_root, table, end.strftime("%Y-%m-%d"))
        return len(df)

    work = df.copy()
    work["_slice_date"] = pd.to_datetime(
        work[date_col], format="mixed", errors="coerce"
    ).dt.date
    written_rows = 0
    latest_written: date | None = None
    target_days = list(dates) if dates is not None else _iter_dates(start, end)
    for day in target_days:
        sub = work[work["_slice_date"] == day].drop(columns=["_slice_date"])
        if sub.empty:
            continue
        out_path = raw_target_path(data_root, table, day)
        if out_path.exists() and not force:
            print(f"跳过已存在 raw: {out_path}", flush=True)
            continue
        atomic_write_parquet(sub.reset_index(drop=True), out_path)
        written_rows += len(sub)
        latest_written = day
        print(
            f"写入 {table} {day.isoformat()}: {len(sub)} 行 -> {out_path}", flush=True
        )

    if latest_written is not None:
        update_watermark(data_root, table, latest_written.strftime("%Y-%m-%d"))
    return written_rows


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="在 amazingdata Docker 容器内拉取原始数据并按 raw/ 规范布局落盘"
    )
    parser.add_argument(
        "--tables",
        default="cn_stock_bars_daily",
        help=f"逗号分隔的表名（默认 cn_stock_bars_daily，可选: {', '.join(SUPPORTED_TABLES)}）",
    )
    parser.add_argument(
        "--date",
        default=None,
        help="单日拉取日期（YYYY-MM-DD 或 YYYYMMDD）",
    )
    parser.add_argument(
        "--start",
        default=None,
        help="区间起始日期（YYYY-MM-DD 或 YYYYMMDD）",
    )
    parser.add_argument(
        "--end",
        default=None,
        help="区间结束日期（YYYY-MM-DD 或 YYYYMMDD）",
    )
    parser.add_argument(
        "--tickers",
        default=None,
        help="可选逗号分隔标的代码；不传则拉取全市场代码列表",
    )
    parser.add_argument(
        "--data-root",
        default="/data",
        help="容器内数据根目录（默认 /data）",
    )
    parser.add_argument(
        "--local-path",
        default=None,
        help="AmazingData SDK 本地缓存目录字符串（默认 <data-root>/raw/amazingdata/_cache）",
    )
    parser.add_argument(
        "--chunk-days",
        type=int,
        default=MINUTE_CHUNK_DAYS,
        help=f"分钟线分块拉取的交易日跨度（默认 {MINUTE_CHUNK_DAYS}）",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="覆盖已存在的 raw/ 分区文件",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """容器内拉取入口：0 表示完成，1 表示拉取失败，2 表示参数错误。"""
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        tables = resolve_tables(args.tables)
        if args.date and (args.start or args.end):
            raise ValueError("--date 与 --start/--end 不能同时指定")
        if args.date:
            start = end = parse_date(args.date)
        elif args.start or args.end:
            if not (args.start and args.end):
                raise ValueError("--start 与 --end 必须成对提供")
            start, end = parse_date(args.start), parse_date(args.end)
        else:
            start = end = date.today()
        if start > end:
            raise ValueError(f"起始日期 {start} 不能晚于结束日期 {end}")
    except ValueError as exc:
        print(f"参数错误: {exc}", file=sys.stderr)
        return 2

    tickers = (
        [t.strip() for t in args.tickers.split(",") if t.strip()]
        if args.tickers
        else None
    )
    data_root = Path(args.data_root)
    local_path_str = (
        str(args.local_path)
        if args.local_path is not None
        else str(default_cache_path(data_root))
    )
    Path(local_path_str).mkdir(parents=True, exist_ok=True)
    if not local_path_str.endswith(("/", os.sep)):
        local_path_str += os.sep

    try:
        username, password, host, port = _read_credentials()
    except Exception as exc:  # noqa: BLE001
        print(f"凭证错误: {exc}", file=sys.stderr)
        return 1

    import AmazingData as ad  # type: ignore[import-not-found]
    from AmazingData import BaseData, InfoData, MarketData  # type: ignore[import-not-found]

    ad.login(username=username, password=password, host=host, port=port)
    print("AmazingData login ok", flush=True)
    try:
        base_data = BaseData()
        calendar = base_data.get_calendar()
        market_data = MarketData(calendar)
        info_data = InfoData()

        for table in tables:
            if table == "cn_calendar":
                out_path = raw_target_path(data_root, table, end)
                if out_path.exists() and not args.force:
                    print(f"跳过已存在 raw: {out_path}", flush=True)
                    continue
                df = _pull_calendar(base_data, start, end)
                if df.empty:
                    raise RuntimeError(f"cn_calendar 在 {start}..{end} 无数据")
                atomic_write_parquet(df, out_path)
                update_watermark(data_root, table, end.strftime("%Y-%m-%d"))
                print(
                    f"写入 {table} {end.isoformat()}: {len(df)} 行 -> {out_path}",
                    flush=True,
                )
            elif table == "cn_stock_basic":
                out_path = raw_target_path(data_root, table, end)
                if out_path.exists() and not args.force:
                    print(f"跳过已存在 raw: {out_path}", flush=True)
                    continue
                df = _pull_stock_basic(base_data, info_data, local_path_str, tickers)
                if df.empty:
                    raise RuntimeError("cn_stock_basic 返回空数据")
                atomic_write_parquet(df, out_path)
                update_watermark(data_root, table, end.strftime("%Y-%m-%d"))
                print(
                    f"写入 {table} {end.isoformat()}: {len(df)} 行 -> {out_path}",
                    flush=True,
                )
            elif table == "cn_stock_corporate_actions":
                out_path = raw_target_path(data_root, table, end)
                if out_path.exists() and not args.force:
                    print(f"跳过已存在 raw: {out_path}", flush=True)
                    continue
                df = _pull_corporate_actions(
                    base_data, info_data, local_path_str, end, tickers
                )
                if df.empty:
                    raise RuntimeError("cn_stock_corporate_actions 返回空数据")
                atomic_write_parquet(df, out_path)
                update_watermark(data_root, table, end.strftime("%Y-%m-%d"))
                print(
                    f"写入 {table} {end.isoformat()}: {len(df)} 行 -> {out_path}",
                    flush=True,
                )
            elif table == "cn_stock_daily_status":
                df = _pull_daily_status(
                    ad,
                    base_data,
                    info_data,
                    market_data,
                    start=start,
                    end=end,
                    local_path=local_path_str,
                    tickers=tickers,
                )
                if df.empty:
                    raise RuntimeError(
                        f"cn_stock_daily_status 在 {start}..{end} 无返回行"
                    )
                _save_daily_slices(
                    df,
                    table=table,
                    start=start,
                    end=end,
                    data_root=data_root,
                    force=args.force,
                )
            elif table == "cn_stock_bars_daily":
                df = _pull_kline_range(
                    ad,
                    base_data,
                    market_data,
                    security_type="EXTRA_STOCK_A_SH_SZ",
                    start=start,
                    end=end,
                    local_path=local_path_str,
                    tickers=tickers,
                )
                if df.empty:
                    raise RuntimeError(
                        f"cn_stock_bars_daily 在 {start}..{end} 无返回行"
                    )
                _save_daily_slices(
                    df,
                    table=table,
                    start=start,
                    end=end,
                    data_root=data_root,
                    force=args.force,
                )
            elif table == "cn_stock_bars_minute":
                _pull_minute_chunked(
                    ad,
                    base_data,
                    market_data,
                    start=start,
                    end=end,
                    data_root=data_root,
                    local_path=local_path_str,
                    tickers=tickers,
                    force=args.force,
                    chunk_days=args.chunk_days,
                )
            elif table == "cn_etf_bars_daily":
                df = _pull_kline_range(
                    ad,
                    base_data,
                    market_data,
                    security_type="EXTRA_ETF",
                    start=start,
                    end=end,
                    local_path=local_path_str,
                    tickers=tickers,
                )
                if df.empty:
                    raise RuntimeError(f"cn_etf_bars_daily 在 {start}..{end} 无返回行")
                _save_daily_slices(
                    df,
                    table=table,
                    start=start,
                    end=end,
                    data_root=data_root,
                    force=args.force,
                )
            elif table == "cn_stock_index_bars_daily":
                df = _pull_kline_range(
                    ad,
                    base_data,
                    market_data,
                    security_type="EXTRA_INDEX_A",
                    start=start,
                    end=end,
                    local_path=local_path_str,
                    tickers=tickers,
                )
                if df.empty:
                    raise RuntimeError(
                        f"cn_stock_index_bars_daily 在 {start}..{end} 无返回行"
                    )
                _save_daily_slices(
                    df,
                    table=table,
                    start=start,
                    end=end,
                    data_root=data_root,
                    force=args.force,
                )
            elif table == "option_basic":
                out_path = raw_target_path(data_root, table, end)
                if out_path.exists() and not args.force:
                    print(f"跳过已存在 raw: {out_path}", flush=True)
                    continue
                df = _pull_option_basic(base_data, info_data, local_path_str)
                if df.empty:
                    raise RuntimeError("option_basic 返回空数据")
                atomic_write_parquet(df, out_path)
                update_watermark(data_root, table, end.strftime("%Y-%m-%d"))
                print(
                    f"写入 {table} {end.isoformat()}: {len(df)} 行 -> {out_path}",
                    flush=True,
                )
    except Exception as exc:  # noqa: BLE001
        print(f"拉取失败: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    finally:
        try:
            ad.logout(username=username)
            print("AmazingData logout ok", flush=True)
        except Exception as exc:  # noqa: BLE001 — 登出异常不掩盖主流程状态
            print(f"AmazingData logout failed: {exc}", flush=True)

    return 0


if __name__ == "__main__":
    sys.exit(main())
