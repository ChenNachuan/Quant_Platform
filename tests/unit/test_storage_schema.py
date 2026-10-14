"""Tests for data.storage_schema — 11-table pandera contract.

Validates that every schema accepts well-formed data and rejects wrong column
names, wrong types, and wrong nullability.  Also pins the quality_flag bit
definitions to core.types.QualityFlag and the calendar exchange set to
core.asset_rules.
"""

from datetime import date, datetime

import pandas as pd
import pandera.pandas as pa
import pytest

from core.asset_rules import CALENDAR_EXCHANGES, EXCHANGE_MAP
from core.types import QualityFlag
from data.storage_schema import (
    CLEAN,
    PARTITION_PATHS,
    QUALITY_FLAG_BITS,
    QUALITY_FLAG_MAX,
    SCHEMA,
    TABLE_FAMILY,
    FactorViewRegistry,
    factor_schema,
    factor_view_name,
)

EXPECTED_TABLES = {
    "bars_daily",
    "bars_minute",
    "bars_daily_futures",
    "tick_futures_l2",
    "fundamental",
    "factor",
    "calendar",
    "contract_specs",
    "industry",
    "corporate_actions",
    "continuous",
}

D = date(2024, 1, 15)
DT = datetime(2024, 1, 15, 9, 31, 0)


# ---------------------------------------------------------------------------
# Helpers — one valid sample per table
# ---------------------------------------------------------------------------
def _valid_row(table: str) -> dict:
    """Return a single valid row as a dict for *table*."""
    base = {
        "bars_daily": {
            "ticker": "600519.SH", "trade_date": D,
            "open": 100.0, "high": 105.0, "low": 99.0, "close": 103.0,
            "volume": 1_000_000, "amount": 1.03e8,
            "adjfactor": 1.0, "turnover": 0.5,
            "suspend": False, "is_limit_up": False, "is_limit_down": False,
            "quality_flag": 0,
        },
        "bars_minute": {
            "ticker": "600519.SH", "trade_date": D, "datetime": DT,
            "open": 100.0, "high": 101.0, "low": 99.5, "close": 100.5,
            "volume": 50_000, "amount": 5.025e6,
            "quality_flag": 0,
        },
        "bars_daily_futures": {
            "ticker": "IF2601.CFFEX", "trade_date": D,
            "open": 3500.0, "high": 3550.0, "low": 3480.0, "close": 3520.0,
            "settle": 3515.0,
            "volume": 120_000, "amount": 1.26e11,
            "open_interest": 80_000,
            "quality_flag": 0,
        },
        "tick_futures_l2": {
            "ticker": "IF2601.CFFEX", "trade_date": D, "datetime": DT,
            "last_price": 3520.0,
            "volume": 120_000, "amount": 1.26e11,
            "open_interest": 80_000,
            "quality_flag": 0,
        },
        "fundamental": {
            "ticker": "600519.SH", "trade_date": D,
            "total_market_cap": 2.0e12, "float_market_cap": 1.5e12,
            "pe_ttm": 25.0, "pe_lyr": 24.0, "pb": 8.0, "ps_ttm": 12.0,
            "total_shares": 1_256_000_000, "float_shares": 1_256_000_000,
            "quality_flag": 0,
        },
        "factor": {
            "ticker": "600519.SH", "date": D, "value": 0.05,
        },
        "calendar": {
            "trade_date": D,
        },
        "contract_specs": {
            "ticker": "IF2601.CFFEX", "symbol": "IF", "exchange": "CFFEX",
            "contract_multiplier": 300.0, "price_tick": 0.2,
            "list_date": D, "expire_date": date(2026, 1, 16),
            "delivery_month": "202601",
            "price_limit_pct": 0.10, "margin_rate": 0.12,
            "trading_hours": "09:30-11:30,13:00-15:00",
            "is_active": True,
        },
        "industry": {
            "ticker": "600519.SH", "industry_l1": "食品饮料",
            "industry_l2": "白酒", "industry_l3": "高端白酒",
            "classification": "citics", "effective_date": D,
        },
        "corporate_actions": {
            "ticker": "600519.SH", "ex_date": D, "event_type": "dividend",
            "announce_date": date(2024, 1, 10),
            "record_date": date(2024, 1, 14),
            "detail": "10派2.00",
            "cash_dividend": 0.2, "stock_dividend": float("nan"),
            "rights_price": float("nan"), "rights_ratio": float("nan"),
        },
        "continuous": {
            "trade_date": D, "dominant_contract": "IF2601.CFFEX",
            "open": 3500.0, "high": 3550.0, "low": 3480.0, "close": 3520.0,
            "volume": 120_000, "amount": 1.26e11,
            "open_interest": 80_000,
            "settle": 3515.0, "adjfactor": 1.0,
        },
    }
    row = base[table].copy()
    if table == "tick_futures_l2":
        for i in range(1, 6):
            row[f"bid_price_{i}"] = 3520.0 - i * 0.2
            row[f"bid_volume_{i}"] = 10 * i
            row[f"ask_price_{i}"] = 3520.0 + i * 0.2
            row[f"ask_volume_{i}"] = 10 * i
    return row


def _valid_df(table: str) -> pd.DataFrame:
    return pd.DataFrame([_valid_row(table)])


# ---------------------------------------------------------------------------
# Table set completeness
# ---------------------------------------------------------------------------
class TestSchemaRegistry:
    def test_exactly_eleven_tables(self):
        assert set(SCHEMA) == EXPECTED_TABLES
        assert len(SCHEMA) == 11

    def test_all_values_are_dataframe_schemas(self):
        for name, schema in SCHEMA.items():
            assert isinstance(schema, pa.DataFrameSchema), name

    def test_partition_paths_cover_all_tables(self):
        assert set(PARTITION_PATHS) == EXPECTED_TABLES

    def test_table_family_covers_all_tables(self):
        assert set(TABLE_FAMILY) == EXPECTED_TABLES

    def test_factor_has_no_quality_flag(self):
        assert "quality_flag" not in SCHEMA["factor"].columns

    def test_calendar_is_single_column(self):
        assert set(SCHEMA["calendar"].columns) == {"trade_date"}


# ---------------------------------------------------------------------------
# All 11 schemas accept well-formed data
# ---------------------------------------------------------------------------
class TestValidDataAccepted:
    @pytest.mark.parametrize("table", sorted(EXPECTED_TABLES))
    def test_valid_row_accepted(self, table):
        df = _valid_df(table)
        result = SCHEMA[table].validate(df)
        assert len(result) == 1


# ---------------------------------------------------------------------------
# Rejection: wrong column name
# ---------------------------------------------------------------------------
class TestWrongColumnNameRejected:
    @pytest.mark.parametrize("table", sorted(EXPECTED_TABLES))
    def test_wrong_column_name_rejected(self, table):
        df = _valid_df(table)
        first_col = df.columns[0]
        df = df.rename(columns={first_col: "bogus_column"})
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA[table].validate(df)

    def test_missing_column_rejected(self):
        df = _valid_df("bars_daily").drop(columns=["adjfactor"])
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["bars_daily"].validate(df)

    def test_extra_column_rejected_strict(self):
        df = _valid_df("bars_daily")
        df["extra"] = 1
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["bars_daily"].validate(df)


# ---------------------------------------------------------------------------
# Rejection: wrong type
# ---------------------------------------------------------------------------
class TestWrongTypeRejected:
    def test_string_for_float_rejected(self):
        df = _valid_df("bars_daily")
        df["open"] = "not_a_number"
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["bars_daily"].validate(df)

    def test_string_for_int_rejected(self):
        df = _valid_df("bars_daily")
        df["volume"] = "not_an_int"
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["bars_daily"].validate(df)

    def test_unparseable_string_for_date_rejected(self):
        df = _valid_df("calendar")
        df["trade_date"] = "garbage_not_a_date"
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError, ValueError)):
            SCHEMA["calendar"].validate(df)

    def test_list_for_scalar_rejected(self):
        df = _valid_df("bars_daily")
        df["amount"] = [[1.0, 2.0]]
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["bars_daily"].validate(df)

    def test_non_integral_float_for_int_rejected(self):
        """Regression: coerce=True used to silently truncate 10.5 -> 10."""
        df = _valid_df("bars_daily")
        df["volume"] = 10.5
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["bars_daily"].validate(df)

    def test_non_integral_float_quality_flag_rejected(self):
        """Regression: coerce=True used to silently truncate 31.9 -> 31."""
        df = _valid_df("bars_daily")
        df["quality_flag"] = 31.9
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["bars_daily"].validate(df)

    def test_integral_float_for_int_rejected(self):
        """Float dtype in int column is rejected even when value is integral."""
        df = _valid_df("bars_daily")
        df["volume"] = 10.0
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["bars_daily"].validate(df)


# ---------------------------------------------------------------------------
# Rejection: wrong nullability
# ---------------------------------------------------------------------------
class TestWrongNullabilityRejected:
    def test_null_in_non_nullable_rejected(self):
        df = _valid_df("bars_daily")
        df["close"] = None
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["bars_daily"].validate(df)

    def test_null_in_non_nullable_date_rejected(self):
        df = _valid_df("calendar")
        df["trade_date"] = None
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["calendar"].validate(df)

    def test_null_in_nullable_accepted(self):
        df = _valid_df("bars_daily")
        df["turnover"] = float("nan")
        result = SCHEMA["bars_daily"].validate(df)
        assert pd.isna(result["turnover"].iloc[0])

    def test_null_in_nullable_pe_accepted(self):
        df = _valid_df("fundamental")
        df["pe_ttm"] = float("nan")
        result = SCHEMA["fundamental"].validate(df)
        assert pd.isna(result["pe_ttm"].iloc[0])


# ---------------------------------------------------------------------------
# quality_flag contract
# ---------------------------------------------------------------------------
class TestQualityFlag:
    def test_bit_definitions_match_quality_flag_enum(self):
        expected = {
            "C02_COVERAGE": QualityFlag.C02_COVERAGE,
            "C05_NULL_RATE": QualityFlag.C05_NULL_RATE,
            "L04_BEYOND_LIMIT": QualityFlag.L04_BEYOND_LIMIT,
            "L07_ADJFACTOR_RANGE": QualityFlag.L07_ADJFACTOR_RANGE,
            "K01_OVERNIGHT_GAP": QualityFlag.K01_OVERNIGHT_GAP,
        }
        assert set(QUALITY_FLAG_BITS) == set(expected)
        for name, bit in expected.items():
            assert QUALITY_FLAG_BITS[name] == int(bit), name

    def test_clean_is_zero(self):
        assert CLEAN == 0
        assert CLEAN == int(QualityFlag.CLEAN)

    def test_five_bits_max_value(self):
        assert QUALITY_FLAG_MAX == 31

    def test_quality_flag_is_int64(self):
        df = _valid_df("bars_daily")
        result = SCHEMA["bars_daily"].validate(df)
        assert result["quality_flag"].dtype.kind in "iu"

    def test_quality_flag_non_negative_enforced(self):
        df = _valid_df("bars_daily")
        df["quality_flag"] = -1
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["bars_daily"].validate(df)

    def test_quality_flag_within_five_bits_enforced(self):
        df = _valid_df("bars_daily")
        df["quality_flag"] = 32
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["bars_daily"].validate(df)

    def test_quality_flag_boundary_values_accepted(self):
        for value in (0, 1, 15, 31):
            df = _valid_df("bars_daily")
            df["quality_flag"] = value
            result = SCHEMA["bars_daily"].validate(df)
            assert result["quality_flag"].iloc[0] == value

    def test_quality_flag_on_market_tables(self):
        tables_with = {
            "bars_daily", "bars_minute", "bars_daily_futures",
            "tick_futures_l2", "fundamental",
        }
        for table in tables_with:
            assert "quality_flag" in SCHEMA[table].columns, table

    def test_quality_flag_absent_on_non_market_tables(self):
        tables_without = {
            "factor", "calendar", "contract_specs", "industry",
            "corporate_actions", "continuous",
        }
        for table in tables_without:
            assert "quality_flag" not in SCHEMA[table].columns, table


# ---------------------------------------------------------------------------
# Calendar exchanges
# ---------------------------------------------------------------------------
class TestCalendarExchanges:
    def test_nine_exchanges(self):
        assert len(CALENDAR_EXCHANGES) == 9

    def test_equals_exchange_map_values(self):
        assert CALENDAR_EXCHANGES == set(EXCHANGE_MAP.values())

    def test_includes_ine_and_gfex(self):
        assert "ine" in CALENDAR_EXCHANGES
        assert "gfex" in CALENDAR_EXCHANGES


# ---------------------------------------------------------------------------
# Factor schema factory
# ---------------------------------------------------------------------------
class TestFactorSchema:
    def test_default_uses_value_column(self):
        df = _valid_df("factor")
        result = SCHEMA["factor"].validate(df)
        assert "value" in result.columns

    def test_named_factor_schema(self):
        schema = factor_schema("momentum_20d")
        df = pd.DataFrame({
            "ticker": ["600519.SH"],
            "date": [D],
            "momentum_20d": [0.05],
        })
        result = schema.validate(df)
        assert "momentum_20d" in result.columns

    def test_named_factor_rejects_wrong_column(self):
        schema = factor_schema("momentum_20d")
        df = pd.DataFrame({
            "ticker": ["600519.SH"],
            "date": [D],
            "value": [0.05],  # wrong column name for this factor
        })
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            schema.validate(df)

    def test_factor_value_nullable(self):
        schema = factor_schema("momentum_20d")
        df = pd.DataFrame({
            "ticker": ["600519.SH"],
            "date": [D],
            "momentum_20d": [float("nan")],
        })
        result = schema.validate(df)
        assert pd.isna(result["momentum_20d"].iloc[0])


# ---------------------------------------------------------------------------
# Factor view naming
# ---------------------------------------------------------------------------
class TestFactorViewName:
    def test_simple_name(self):
        assert factor_view_name("momentum_20d") == "factor_momentum_20d"

    def test_dot_replaced(self):
        assert factor_view_name("alpha.1") == "factor_alpha_1"

    def test_dash_replaced(self):
        assert factor_view_name("mom-20d") == "factor_mom_20d"

    def test_leading_digit_gets_extra_prefix(self):
        assert factor_view_name("20d_mom") == "factor__20d_mom"

    def test_underscore_preserved(self):
        assert factor_view_name("a_b_c") == "factor_a_b_c"


# ---------------------------------------------------------------------------
# Column-level spot checks (types and checks per §47)
# ---------------------------------------------------------------------------
class TestColumnContracts:
    def test_bars_daily_column_count(self):
        assert len(SCHEMA["bars_daily"].columns) == 14

    def test_bars_minute_no_adjfactor(self):
        assert "adjfactor" not in SCHEMA["bars_minute"].columns
        assert len(SCHEMA["bars_minute"].columns) == 10

    def test_bars_daily_no_prev_close_pct_change_close_adj(self):
        cols = SCHEMA["bars_daily"].columns
        for forbidden in ("prev_close", "pct_change", "close_adj"):
            assert forbidden not in cols

    def test_bars_daily_futures_no_adjfactor(self):
        assert "adjfactor" not in SCHEMA["bars_daily_futures"].columns
        assert "settle" in SCHEMA["bars_daily_futures"].columns
        assert "open_interest" in SCHEMA["bars_daily_futures"].columns

    def test_continuous_no_close_adj(self):
        assert "close_adj" not in SCHEMA["continuous"].columns
        assert "adjfactor" in SCHEMA["continuous"].columns

    def test_contract_specs_column_count(self):
        assert len(SCHEMA["contract_specs"].columns) == 12

    def test_corporate_actions_column_count(self):
        assert len(SCHEMA["corporate_actions"].columns) == 10

    def test_corporate_actions_event_type_rejects_fifth_type(self):
        df = _valid_df("corporate_actions")
        df["event_type"] = "merger"
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["corporate_actions"].validate(df)

    def test_industry_classification_rejects_unknown(self):
        df = _valid_df("industry")
        df["classification"] = "unknown_system"
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["industry"].validate(df)

    def test_equity_ticker_rejected_on_futures_table(self):
        df = _valid_df("bars_daily_futures")
        df["ticker"] = "600519.SH"  # equity ticker on futures table
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["bars_daily_futures"].validate(df)

    def test_futures_ticker_rejected_on_equity_table(self):
        df = _valid_df("bars_daily")
        df["ticker"] = "IF2601.CFFEX"  # futures ticker on equity table
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["bars_daily"].validate(df)

    def test_ohlcv_positive_enforced(self):
        df = _valid_df("bars_daily")
        df["open"] = 0.0
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["bars_daily"].validate(df)

    def test_volume_non_negative_enforced(self):
        df = _valid_df("bars_daily")
        df["volume"] = -1
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["bars_daily"].validate(df)

    def test_datetime_coerced_to_date(self):
        """trade_date datetime64 input is coerced to date before validation."""
        df = _valid_df("bars_daily")
        df["trade_date"] = pd.to_datetime(["2024-01-15"])
        result = SCHEMA["bars_daily"].validate(df)
        assert result["trade_date"].iloc[0] == D


# ---------------------------------------------------------------------------
# Regression: code-review findings 2026-10-14
# ---------------------------------------------------------------------------
class TestDateColumnRejectsInt:
    """Finding 1: YYYYMMDD ints must not silently coerce to epoch dates."""

    def test_int_yyyymmdd_rejected_on_date_column(self):
        df = _valid_df("calendar")
        df["trade_date"] = 20240115
        with pytest.raises(ValueError, match="numeric"):
            SCHEMA["calendar"].validate(df)

    def test_int_yyyymmdd_rejected_on_bars_daily(self):
        df = _valid_df("bars_daily")
        df["trade_date"] = 20240115
        with pytest.raises(ValueError, match="numeric"):
            SCHEMA["bars_daily"].validate(df)

    def test_string_date_still_accepted(self):
        df = _valid_df("calendar")
        df["trade_date"] = "2024-01-15"
        result = SCHEMA["calendar"].validate(df)
        assert result["trade_date"].iloc[0] == D

    def test_date_object_still_accepted(self):
        df = _valid_df("calendar")
        df["trade_date"] = D
        result = SCHEMA["calendar"].validate(df)
        assert result["trade_date"].iloc[0] == D


class TestDatetimeColumnRejectsTzAware:
    """Finding 2: tz-aware timestamps must not silently shift."""

    def test_tz_aware_datetime_rejected(self):
        df = _valid_df("bars_minute")
        df["datetime"] = pd.to_datetime(["2024-01-15 09:31:00+08:00"])
        with pytest.raises(ValueError, match="tz-aware"):
            SCHEMA["bars_minute"].validate(df)

    def test_naive_datetime_accepted(self):
        df = _valid_df("bars_minute")
        result = SCHEMA["bars_minute"].validate(df)
        assert result["datetime"].iloc[0] == DT


class TestNullableDateColumns:
    """Finding A regression: nullable date columns must accept missing values."""

    def test_announce_date_none_accepted(self):
        row = _valid_row("corporate_actions")
        row["announce_date"] = None
        df = pd.DataFrame([row])
        result = SCHEMA["corporate_actions"].validate(df)
        assert pd.isna(result["announce_date"].iloc[0])

    def test_record_date_nan_accepted(self):
        row = _valid_row("corporate_actions")
        row["record_date"] = float("nan")
        df = pd.DataFrame([row])
        result = SCHEMA["corporate_actions"].validate(df)
        assert pd.isna(result["record_date"].iloc[0])

    def test_mixed_present_missing_batch_accepted(self):
        row1 = _valid_row("corporate_actions")
        row1["announce_date"] = date(2024, 1, 10)
        row1["record_date"] = None
        row2 = {**row1, "ticker": "000001.SZ", "announce_date": None, "record_date": date(2024, 1, 14)}
        df = pd.DataFrame([row1, row2])
        result = SCHEMA["corporate_actions"].validate(df)
        assert result["announce_date"].iloc[0] == date(2024, 1, 10)
        assert pd.isna(result["record_date"].iloc[0])
        assert pd.isna(result["announce_date"].iloc[1])
        assert result["record_date"].iloc[1] == date(2024, 1, 14)

    def test_both_dates_none_accepted(self):
        row = _valid_row("corporate_actions")
        row["announce_date"] = None
        row["record_date"] = None
        df = pd.DataFrame([row])
        result = SCHEMA["corporate_actions"].validate(df)
        assert pd.isna(result["announce_date"].iloc[0])
        assert pd.isna(result["record_date"].iloc[0])


class TestDateColumnRejectsFloatAndObject:
    """Finding B regression: float/object dtype integers must not silently coerce."""

    def test_float64_yyyymmdd_rejected(self):
        df = _valid_df("calendar")
        df["trade_date"] = pd.Series([20240115.0])
        with pytest.raises(ValueError, match="numeric"):
            SCHEMA["calendar"].validate(df)

    def test_object_dtype_int_rejected(self):
        df = _valid_df("calendar")
        df["trade_date"] = pd.Series([20240115], dtype="object")
        with pytest.raises(ValueError, match="numeric"):
            SCHEMA["calendar"].validate(df)

    def test_mixed_int_string_rejected(self):
        df = _valid_df("calendar")
        df["trade_date"] = pd.Series([20240115, "2024-01-16"], dtype="object")
        with pytest.raises(ValueError, match="numeric"):
            SCHEMA["calendar"].validate(df)

    def test_float64_epoch_datetime_rejected(self):
        row = _valid_row("bars_minute")
        row["datetime"] = 1705282260.0
        df = pd.DataFrame([row])
        with pytest.raises(ValueError, match="numeric"):
            SCHEMA["bars_minute"].validate(df)

    def test_object_dtype_int_datetime_rejected(self):
        row = _valid_row("bars_minute")
        row["datetime"] = pd.Series([1705282260], dtype="object").iloc[0]
        df = pd.DataFrame([row])
        with pytest.raises(ValueError, match="numeric"):
            SCHEMA["bars_minute"].validate(df)

    def test_object_tz_aware_timestamp_rejected(self):
        ts = pd.Timestamp("2024-01-15 09:31:00+08:00")
        row = _valid_row("bars_minute")
        row["datetime"] = ts
        df = pd.DataFrame([row])
        with pytest.raises((ValueError, pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["bars_minute"].validate(df)


class TestPrimaryKeyUniqueness:
    """Finding 3: duplicate PK rows must be rejected (§47.14)."""

    def test_duplicate_bars_daily_pk_rejected(self):
        row = _valid_row("bars_daily")
        df = pd.DataFrame([row, row])
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["bars_daily"].validate(df)

    def test_distinct_bars_daily_pk_accepted(self):
        row1 = _valid_row("bars_daily")
        row2 = {**row1, "ticker": "000001.SZ"}
        df = pd.DataFrame([row1, row2])
        result = SCHEMA["bars_daily"].validate(df)
        assert len(result) == 2

    def test_duplicate_corporate_actions_pk_rejected(self):
        row = _valid_row("corporate_actions")
        df = pd.DataFrame([row, row])
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["corporate_actions"].validate(df)

    def test_duplicate_calendar_pk_rejected(self):
        row = _valid_row("calendar")
        df = pd.DataFrame([row, row])
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["calendar"].validate(df)


class TestFactorViewRegistry:
    """Finding 4: factor_view_name collisions must be detected."""

    def test_collision_detected(self):
        registry = FactorViewRegistry()
        registry.register("alpha.1")
        with pytest.raises(ValueError, match="collision"):
            registry.register("alpha_1")

    def test_same_name_reregister_ok(self):
        registry = FactorViewRegistry()
        v1 = registry.register("momentum_20d")
        v2 = registry.register("momentum_20d")
        assert v1 == v2

    def test_distinct_names_ok(self):
        registry = FactorViewRegistry()
        assert registry.register("momentum_20d") == "factor_momentum_20d"
        assert registry.register("reversal_5d") == "factor_reversal_5d"

    def test_views_property(self):
        registry = FactorViewRegistry()
        registry.register("alpha.1")
        assert registry.views == {"factor_alpha_1": "alpha.1"}


class TestFactorSchemaReservedNames:
    """Finding 5: factor_schema must reject reserved column names."""

    def test_ticker_rejected(self):
        with pytest.raises(ValueError, match="reserved"):
            factor_schema("ticker")

    def test_date_rejected(self):
        with pytest.raises(ValueError, match="reserved"):
            factor_schema("date")

    def test_normal_name_accepted(self):
        schema = factor_schema("momentum_20d")
        assert "momentum_20d" in schema.columns


class TestContractSpecsExchange:
    """Finding 7: exchange column must be validated against EXCHANGE_MAP."""

    def test_valid_exchange_accepted(self):
        df = _valid_df("contract_specs")
        df["exchange"] = "CFFEX"
        result = SCHEMA["contract_specs"].validate(df)
        assert result["exchange"].iloc[0] == "CFFEX"

    def test_invalid_exchange_rejected(self):
        df = _valid_df("contract_specs")
        df["exchange"] = "FAKE_EXCHANGE"
        with pytest.raises((pa.errors.SchemaErrors, pa.errors.SchemaError)):
            SCHEMA["contract_specs"].validate(df)
