"""AmazingData 原始数据拉取脚本（运行于 ``amazingdata`` Docker 容器内）。

容器镜像仅预装标准库、``pandas``、``pyarrow`` 与 ``AmazingData`` SDK，无宿主机 ``.venv``、
``pandera`` 或 ``duckdb``，因此本脚本严格保持零项目内部依赖；
拉取的原始数据按宿主机规范布局原子写入 ``{data_root}/raw/amazingdata/{table}/{YYYY-MM-DD}.parquet``，
并同步更新 ``{data_root}/raw/amazingdata/.watermarks.json``，供宿主机通过
``fetch_daily.py --from-raw --source amazingdata`` 或交叉校验直接消费。
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Sequence

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

#: AmazingData 限流批量约束：get_option_basic_info 单次上限约 100 条，固定每批 10 个代码
OPTION_BATCH_SIZE = 10

#: 常用表名别名映射为规范表名
TABLE_ALIASES: dict[str, str] = {
    "calendar": "cn_calendar",
    "cn_calendar": "cn_calendar",
    "stock": "cn_stock_bars_daily",
    "bars_daily": "cn_stock_bars_daily",
    "cn_stock_bars_daily": "cn_stock_bars_daily",
    "etf": "cn_etf_bars_daily",
    "etf_bars_daily": "cn_etf_bars_daily",
    "cn_etf_bars_daily": "cn_etf_bars_daily",
    "option": "option_basic",
    "option_basic": "option_basic",
}

SUPPORTED_TABLES = (
    "cn_calendar",
    "cn_stock_bars_daily",
    "cn_etf_bars_daily",
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


def _pull_calendar(base_data: Any, start: date, end: date) -> pd.DataFrame:
    all_days = base_data.get_calendar()
    frame = pd.DataFrame({"cal_date": list(all_days)})
    frame["trade_date"] = pd.to_datetime(frame["cal_date"], errors="coerce").dt.date
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
    if security_type == "EXTRA_STOCK_A_SH_SZ" and hasattr(
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
) -> pd.DataFrame:
    codes = _resolve_code_list(
        base_data, security_type, start, end, local_path, tickers
    )
    if not codes:
        return pd.DataFrame()
    raw = market_data.query_kline(
        codes,
        begin_date=to_ymd_int(start),
        end_date=to_ymd_int(end),
        period=ad_mod.constant.Period.day.value,
    )
    return _normalize_kline_result(raw)


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
) -> int:
    if df.empty:
        return 0
    if "kline_time" not in df.columns:
        out_path = raw_target_path(data_root, table, end)
        if force or not out_path.exists():
            atomic_write_parquet(df, out_path)
            update_watermark(data_root, table, end.strftime("%Y-%m-%d"))
        return len(df)

    work = df.copy()
    work["_slice_date"] = pd.to_datetime(work["kline_time"], errors="coerce").dt.date
    written_rows = 0
    latest_written: date | None = None
    for day in _iter_dates(start, end):
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
        default="/data/amazingdata_cache",
        help="AmazingData SDK 本地缓存目录字符串（默认 /data/amazingdata_cache）",
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
    local_path_str = str(args.local_path)
    Path(local_path_str).mkdir(parents=True, exist_ok=True)

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
