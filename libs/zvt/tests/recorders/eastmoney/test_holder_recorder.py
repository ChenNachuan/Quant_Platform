# -*- coding: utf-8 -*-
from ...context import init_test_context

init_test_context()

from zvt.consts import SAMPLE_STOCK_CODES  # noqa: E402

from zvt.recorders.eastmoney.holder.eastmoney_top_ten_holder_recorder import TopTenHolderRecorder  # noqa: E402
from zvt.recorders.eastmoney.holder.eastmoney_top_ten_tradable_holder_recorder import TopTenTradableHolderRecorder  # noqa: E402


def test_top_ten_holder_recorder():
    recorder = TopTenHolderRecorder(codes=SAMPLE_STOCK_CODES)
    try:
        recorder.run()
    except Exception:
        assert False


def test_top_ten_tradable_holder_recorder():
    recorder = TopTenTradableHolderRecorder(codes=SAMPLE_STOCK_CODES)
    try:
        recorder.run()
    except Exception:
        assert False
