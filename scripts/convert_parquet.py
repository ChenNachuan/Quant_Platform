#!/usr/bin/env python3
"""L2 日 CSV(GBK) -> parquet：按日期合并全部股票，每表一个 parquet。

输入: ~/Downloads/2025/2025MM/YYYYMMDD/<股票>/<表>.csv
输出: ~/Documents/QDCData/cn_tick_l2_2025/2025MM/YYYYMMDD/<表>.parquet
布局: 月/日 即分区；一天三个 parquet（行情/逐笔成交/逐笔委托），各含全部股票，
      股票由"万得代码"列区分；每只股票一个 row group
幂等: 输出已存在则跳过，可随时重跑补漏；单表失败不影响其它
"""

import os
import time
from concurrent.futures import ProcessPoolExecutor

import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq

ROOT_IN = os.path.expanduser("~/Downloads/2025")
ROOT_OUT = os.path.expanduser("~/Documents/QDCData/cn_tick_l2_2025")
TABLES = ["行情", "逐笔成交", "逐笔委托"]
STOCKS = [
    "510300.SH",
    "510500.SH",
    "518880.SH",
    "511010.SH",
    "159915.SZ",
    "600519.SH",
    "000858.SZ",
    "300750.SZ",
    "601318.SH",
    "000001.SZ",
]
WORKERS = 12

# 代码/标志类列读取时强制 string：同列有的文件是纯数字（会被推断成 int64）、
# 有的含字母（string），且数字代码可能带前导零——强制 string 保真且跨天 schema 一致
FORCE_STRING = {
    "行情": ["万得代码", "交易所代码", "成交标志", "BS标志"],
    "逐笔成交": ["万得代码", "交易所代码", "成交代码", "委托代码", "BS标志"],
    "逐笔委托": ["万得代码", "交易所代码", "委托类型", "委托代码"],
}


def read_csv(path, table):
    return pv.read_csv(
        path,
        read_options=pv.ReadOptions(encoding="GBK"),
        convert_options=pv.ConvertOptions(
            column_types={c: pa.string() for c in FORCE_STRING[table]}
        ),
    )


def merge_type(a, b):
    if str(a) == "null":
        return b
    if str(b) == "null" or a == b:
        return a
    if pa.types.is_integer(a) and pa.types.is_floating(b):
        return b
    if pa.types.is_floating(a) and pa.types.is_integer(b):
        return a
    return pa.string()  # 其它冲突（如 int64 vs string）一律按字符串处理


def merged_schema(tables):
    types = {}
    order = [f.name for f in tables[0].schema]
    for t in tables:
        for f in t.schema:
            types[f.name] = merge_type(types.get(f.name, pa.null()), f.type)
    fields = []
    for name in order:
        ty = types[name]
        if str(ty) == "null":
            ty = pa.string()  # 整列为空 → 字符串占位，保证跨天 schema 一致
        fields.append(pa.field(name, ty))
    return pa.schema(fields)


def convert_day(ymd):
    base_in = f"{ROOT_IN}/{ymd[:6]}/{ymd}"
    out_dir = f"{ROOT_OUT}/{ymd[:6]}/{ymd}"
    rows = inb = outb = 0
    fails = []
    for table in TABLES:
        op = f"{out_dir}/{table}.parquet"
        if os.path.exists(op):
            continue  # 幂等：已转过
        paths = [f"{base_in}/{s}/{table}.csv" for s in STOCKS]
        paths = [p for p in paths if os.path.exists(p)]
        if not paths:
            continue
        try:
            tables = [read_csv(p, table) for p in paths]
            # 跨股票 schema 归并：类型不一致的列统一成 string（见 merge_type）
            schema = merged_schema(tables)
            os.makedirs(out_dir, exist_ok=True)
            writer = pq.ParquetWriter(op, schema, compression="snappy")
            try:
                for t in tables:
                    t = t.cast(schema)
                    writer.write_table(t)  # 每股票一个 row group
                    rows += t.num_rows
            finally:
                writer.close()
            for p in paths:
                inb += os.path.getsize(p)
            outb += os.path.getsize(op)
        except Exception as e:  # 单表失败不影响其它表/天
            if os.path.exists(op):
                os.remove(op)  # 清残缺输出，下次重跑
            fails.append(f"{ymd}/{table}: {e}")
    return ymd, rows, inb, outb, fails


def scan_days():
    days = []
    for m in range(1, 13):
        for d in range(1, 32):
            ymd = f"2025{m:02d}{d:02d}"
            base = f"{ROOT_IN}/{ymd[:6]}/{ymd}"
            if any(os.path.exists(f"{base}/{s}/行情.csv") for s in STOCKS):
                days.append(ymd)
    return days


def normalize_schemas(days):
    """跨天 schema 归一：逐天类型推断会漂移（如成交编号 int64/string、
    CSV 尾逗号多出的空列名），把全部输出统一到每表的规范 schema。
    只重写不符的文件，且保留原有 row group（每股票一个）。幂等。"""
    canon, order, paths_by_table = {}, {}, {t: [] for t in TABLES}
    for ymd in days:
        for table in TABLES:
            fp = f"{ROOT_OUT}/{ymd[:6]}/{ymd}/{table}.parquet"
            if not os.path.exists(fp):
                continue
            paths_by_table[table].append(fp)
            canon.setdefault(table, {})
            order.setdefault(table, [])
            for f in pq.read_schema(fp):
                if f.name == "":  # 空列名是 CSV 尾逗号产物，丢弃
                    continue
                if f.name not in canon[table]:
                    order[table].append(f.name)
                canon[table][f.name] = merge_type(
                    canon[table].get(f.name, pa.null()), f.type
                )

    rewritten = 0
    for table in TABLES:
        if table not in canon:
            continue
        schema = pa.schema(
            [
                pa.field(
                    n,
                    canon[table][n] if str(canon[table][n]) != "null" else pa.string(),
                )
                for n in order[table]
            ]
        )
        for fp in paths_by_table[table]:
            pf = pq.ParquetFile(fp)
            same = [f.name for f in pf.schema_arrow] == order[table] and all(
                pf.schema_arrow.field(i).type == schema.field(i).type
                for i in range(len(schema))
            )
            if same:
                continue
            try:
                tmp = fp + ".tmp"
                writer = pq.ParquetWriter(tmp, schema, compression="snappy")
                try:
                    for i in range(pf.metadata.num_row_groups):
                        tg = pf.read_row_group(i).select(order[table]).cast(schema)
                        writer.write_table(tg)
                finally:
                    writer.close()
                os.replace(tmp, fp)
                rewritten += 1
            except Exception as e:
                print(f"  归一失败 {fp}: {e}", flush=True)
    return rewritten


def main():
    days = scan_days()

    print(f"{len(days)} 个交易日，开始转换（{WORKERS} 进程）...", flush=True)
    t0 = time.time()
    tot_rows = tot_in = tot_out = 0
    all_fails = []
    with ProcessPoolExecutor(max_workers=WORKERS) as ex:
        for ymd, rows, inb, outb, fails in ex.map(convert_day, days):
            tot_rows += rows
            tot_in += inb
            tot_out += outb
            all_fails.extend(fails)
            print(f"  {ymd} rows={rows:,}", flush=True)

    wall = time.time() - t0
    print(f"=== 完成: wall={wall:.1f}s ({wall / 60:.2f}min)")
    if tot_in:
        print(
            f"=== 输入 {tot_in / 1e9:.2f} GB -> 输出 {tot_out / 1e9:.2f} GB (比 {tot_out / tot_in:.2f})"
        )
    print(f"=== 行数 {tot_rows:,}")
    if all_fails:
        print(f"=== 失败 {len(all_fails)} 项（重跑自动重试）:")
        for f in all_fails:
            print("   ", f)

    # 阶段2：跨天 schema 归一（消除逐天推断漂移）
    rewritten = normalize_schemas(days)
    print(f"=== schema 归一: 重写 {rewritten} 个文件")


if __name__ == "__main__":
    main()
