"""
Qlib 数据转储器
将数据湖 (Parquet/DuckDB) 中的数据转换为 Qlib 专用的 Binary 格式。
使用 Qlib Python API 替代 subprocess 调用。
"""
import logging
import shutil
from pathlib import Path
from typing import Optional

from infra.storage import StorageManager

logger = logging.getLogger(__name__)


class QlibDumper:
    """
    将数据湖行情数据增量导出为 Qlib Binary 格式。

    增量策略：通过 .last_dump_date 文件记录上次导出的最新日期，
    下次调用时自动从该日期起增量导出。
    """

    def __init__(self):
        self.storage = StorageManager()

        provider_uri = self.storage.config.get('qlib', {}).get('provider_uri')
        if not provider_uri:
            raise ValueError("配置文件中缺少 'qlib.provider_uri'")

        self.qlib_dir = Path(provider_uri)
        self.qlib_dir.mkdir(parents=True, exist_ok=True)
        self._last_dump_file = self.qlib_dir / ".last_dump_date"

        self._init_qlib()

    def _init_qlib(self) -> None:
        try:
            import qlib
            region = self.storage.config.get('qlib', {}).get('region', 'cn')
            qlib.init(provider_uri=str(self.qlib_dir), region=region)
            logger.info(f"Qlib 已初始化: provider_uri={self.qlib_dir}, region={region}")
        except ImportError:
            logger.warning("pyqlib 未安装，Qlib 功能不可用。请运行: uv sync")
        except Exception as e:
            logger.warning(f"Qlib 初始化失败（数据目录可能为空）: {e}")

    def _get_last_dump_date(self) -> Optional[str]:
        if self._last_dump_file.exists():
            return self._last_dump_file.read_text().strip() or None
        return None

    def _save_last_dump_date(self, date: str) -> None:
        self._last_dump_file.write_text(date)

    def dump(self, start_date: Optional[str] = None) -> None:
        """
        执行数据转储。

        Args:
            start_date: 开始日期 'YYYY-MM-DD'。若为 None，自动从上次导出日期起增量导出；
                        若从未导出过，则全量导出。
        """
        try:
            from qlib.data.dataset_dump import DumpDataAll, DumpDataUpdate
        except ImportError:
            logger.error("pyqlib 未安装，无法执行转储。请运行: uv sync")
            return

        if start_date is None:
            start_date = self._get_last_dump_date()

        is_incremental = start_date is not None
        logger.info(f"正在查询存储数据 (start_date={start_date}, incremental={is_incremental})...")

        df = self.storage.get_data(market='cn_stock', frequency='1d', start_date=start_date)
        if df.empty:
            logger.info("无新数据需要转储")
            return

        # get_data() 返回 MultiIndex(timestamp, entity_id)，重置为普通列
        df = df.reset_index()

        df = df.rename(columns={'entity_id': 'symbol', 'timestamp': 'date'})

        required = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']
        for col in required:
            if col not in df.columns:
                logger.warning(f"缺失字段 '{col}'，自动填充为 0")
                df[col] = 0.0

        csv_dir = self.qlib_dir / "temp_csv"
        csv_dir.mkdir(exist_ok=True)
        try:
            # Qlib 要求每只股票一个 CSV 文件，文件名为 {symbol}.csv
            for symbol, group in df.groupby('symbol'):
                group[['date', 'open', 'high', 'low', 'close', 'volume']].to_csv(
                    csv_dir / f"{symbol}.csv", index=False
                )

            dumper_cls = DumpDataUpdate if is_incremental else DumpDataAll
            dumper_cls(
                csv_path=str(csv_dir),
                qlib_dir=str(self.qlib_dir),
                symbol_field_name="symbol",
                date_field_name="date",
                include_fields="open,high,low,close,volume",
            ).dump()

            end_date = df['date'].max()
            if hasattr(end_date, 'strftime'):
                end_date = end_date.strftime('%Y-%m-%d')
            self._save_last_dump_date(str(end_date))
            logger.info(f"Qlib 转储完成，最新日期: {end_date}")

        finally:
            shutil.rmtree(csv_dir, ignore_errors=True)
            logger.debug("临时文件已清理")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    dumper = QlibDumper()
    # dumper.dump(start_date='2023-01-01')
