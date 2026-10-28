# -*- coding: utf-8 -*-
from ...context import init_test_context

init_test_context()

from zvt.recorders.eastmoney.meta.eastmoney_stock_meta_recorder import EastmoneyStockDetailRecorder  # noqa: E402

from zvt.consts import SAMPLE_STOCK_CODES  # noqa: E402


def test_meta_recorder():
    recorder = EastmoneyStockDetailRecorder(codes=SAMPLE_STOCK_CODES)
    try:
        recorder.run()
    except Exception:
        assert False
