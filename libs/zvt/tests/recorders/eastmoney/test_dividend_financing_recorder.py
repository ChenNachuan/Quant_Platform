# -*- coding: utf-8 -*-
from zvt.domain import DividendDetail, RightsIssueDetail, SpoDetail, DividendFinancing
from ...context import init_test_context

init_test_context()

from zvt.consts import SAMPLE_STOCK_CODES  # noqa: E402


def test_dividend_detail():
    try:
        DividendDetail.record_data(provider="eastmoney", codes=SAMPLE_STOCK_CODES)
    except Exception:
        assert False


def test_rights_issue_detail():
    try:
        RightsIssueDetail.record_data(provider="eastmoney", codes=SAMPLE_STOCK_CODES)
    except Exception:
        assert False


def test_spo_detail():
    try:
        SpoDetail.record_data(provider="eastmoney", codes=SAMPLE_STOCK_CODES)
    except Exception:
        assert False


def test_dividend_financing():
    try:
        DividendFinancing.record_data(provider="eastmoney", codes=SAMPLE_STOCK_CODES)
    except Exception:
        assert False
