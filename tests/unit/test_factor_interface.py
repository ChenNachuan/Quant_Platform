"""Tests for research.factor_interface — Factor ABC, dependency parsing,
output contract, metadata extraction, and category / name validation.

Covers every DoD row: depends_on prefix resolution and rejection, output
schema enforcement, extract_factor_meta field extraction, FACTOR_CATEGORIES
boundary, and the three Dev-default decisions (version convention shape,
event/alternative category acceptance, name validation point).
"""

from __future__ import annotations

import pandas as pd
import pytest

from research.factor_interface import (
    DEFAULT_CATEGORY,
    FACTOR_CATEGORIES,
    KIND_FACTOR,
    KIND_TABLE,
    VALID_CATEGORIES,
    Factor,
    factor_meta,
    extract_factor_meta,
    parse_dependency,
    validate_output,
)


# ---------------------------------------------------------------------------
# Concrete test fixtures (not real factors — test doubles only)
# ---------------------------------------------------------------------------


class Momentum20D(Factor):
    name = "momentum_20d"
    description = "20-day momentum"
    category = "time_series"
    frequency = "daily"
    asset_types = ["equity"]
    version = "1.0"

    def depends_on(self):
        return ["bars_daily"]

    def compute(self, data):
        df = data["bars_daily"][["date", "ticker", "close"]].copy()
        df = df.sort_values(["ticker", "date"])
        df["momentum_20d"] = df.groupby("ticker")["close"].pct_change(20)
        return df[["date", "ticker", "momentum_20d"]].dropna()


class VolAdjMomentum(Factor):
    name = "vol_adj_momentum"
    description = "vol-adjusted momentum"
    category = "time_series"
    version = "2.1"

    def depends_on(self):
        return ["factor:momentum_20d", "factor:volatility_20d"]

    def compute(self, data):
        mom = data["factor:momentum_20d"]
        vol = data["factor:volatility_20d"]
        merged = mom.merge(vol, on=["date", "ticker"])
        merged["vol_adj_momentum"] = merged["momentum_20d"] / merged["volatility_20d"]
        return merged[["date", "ticker", "vol_adj_momentum"]].dropna()


class EventFactor(Factor):
    """category="event" — shape only, V1 has no event runtime."""

    name = "earnings_surprise_1d"
    category = "event"

    def depends_on(self):
        return ["earnings_calendar"]

    def compute(self, data):
        df = data["earnings_calendar"]
        return df[["date", "ticker", "surprise"]].rename(
            columns={"surprise": "earnings_surprise_1d"}
        )


class AltSentiment(Factor):
    """category="alternative" — shape only."""

    name = "sentiment_flow_5d"
    category = "alternative"

    def depends_on(self):
        return ["money_flow"]

    def compute(self, data):
        df = data["money_flow"]
        return df[["date", "ticker", "net_flow"]].rename(
            columns={"net_flow": "sentiment_flow_5d"}
        )


class MinimalFactor(Factor):
    name = "min_fac"

    def depends_on(self):
        return []

    def compute(self, data):
        return pd.DataFrame(columns=["date", "ticker", "min_fac"])


# ---------------------------------------------------------------------------
# parse_dependency
# ---------------------------------------------------------------------------


class TestParseDependency:
    def test_bare_name_is_table(self):
        kind, name = parse_dependency("bars_daily")
        assert kind == KIND_TABLE
        assert name == "bars_daily"

    def test_factor_prefix(self):
        kind, name = parse_dependency("factor:momentum_20d")
        assert kind == KIND_FACTOR
        assert name == "momentum_20d"

    def test_multiple_colons_in_factor_name_keeps_rest(self):
        kind, name = parse_dependency("factor:aaa:bbb")
        assert kind == KIND_FACTOR
        assert name == "aaa:bbb"

    @pytest.mark.parametrize(
        "dep",
        [
            "event:earnings",
            "alt:sentiment",
            "foo:bar",
            ":name",
            "data:bars_daily",
        ],
    )
    def test_unrecognised_prefix_rejected(self, dep):
        with pytest.raises(ValueError, match="unrecognised dependency prefix"):
            parse_dependency(dep)

    @pytest.mark.parametrize("dep", ["", "   ", "\t"])
    def test_empty_or_whitespace_rejected(self, dep):
        with pytest.raises(ValueError, match="non-empty"):
            parse_dependency(dep)

    def test_factor_prefix_empty_name_rejected(self):
        with pytest.raises(ValueError, match="empty factor name"):
            parse_dependency("factor:")

    def test_factor_prefix_whitespace_name_rejected(self):
        with pytest.raises(ValueError, match="empty factor name"):
            parse_dependency("factor:   ")

    def test_non_string_rejected(self):
        with pytest.raises(ValueError, match="non-empty"):
            parse_dependency(123)  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="non-empty"):
            parse_dependency(None)  # type: ignore[arg-type]

    def test_strips_outer_whitespace(self):
        kind, name = parse_dependency("  bars_daily  ")
        assert kind == KIND_TABLE
        assert name == "bars_daily"

    def test_strips_prefix_whitespace(self):
        kind, name = parse_dependency("factor : momentum_20d")
        assert kind == KIND_FACTOR
        assert name == "momentum_20d"

    def test_return_types(self):
        for result in (parse_dependency("x"), parse_dependency("factor:y")):
            assert isinstance(result, tuple)
            assert len(result) == 2
            assert all(isinstance(r, str) for r in result)


# ---------------------------------------------------------------------------
# FACTOR_CATEGORIES
# ---------------------------------------------------------------------------


class TestFactorCategories:
    def test_exactly_four(self):
        assert len(FACTOR_CATEGORIES) == 4

    def test_members(self):
        assert set(FACTOR_CATEGORIES) == {
            "time_series",
            "cross_section",
            "event",
            "alternative",
        }

    def test_valid_categories_include_custom(self):
        assert DEFAULT_CATEGORY == "custom"
        assert VALID_CATEGORIES == set(FACTOR_CATEGORIES) | {"custom"}

    @pytest.mark.parametrize("cat", list(FACTOR_CATEGORIES) + [DEFAULT_CATEGORY])
    def test_valid_category_accepted(self, cat):
        klass = type("CatTest", (Factor,), {"name": "cat_test", "category": cat})
        assert klass.category == cat

    @pytest.mark.parametrize(
        "bad", ["momentum", "mom", "TIME_SERIES", "crosssection", "", "Custom"]
    )
    def test_invalid_category_rejected(self, bad):
        with pytest.raises(ValueError, match="invalid category"):
            type("BadCat", (Factor,), {"name": "bad_cat", "category": bad})


# ---------------------------------------------------------------------------
# Factor name validation
# ---------------------------------------------------------------------------


class TestNameValidation:
    def test_abstract_cannot_instantiate(self):
        with pytest.raises(TypeError):
            Factor()  # type: ignore[abstract]

    def test_missing_name_rejected(self):
        with pytest.raises(ValueError, match="name"):
            type("NoName", (Factor,), {"depends_on": lambda s: [], "compute": lambda s, d: None})

    def test_empty_name_rejected(self):
        with pytest.raises(ValueError, match="name"):
            type(
                "EmptyName",
                (Factor,),
                {"name": "", "depends_on": lambda s: [], "compute": lambda s, d: None},
            )

    @pytest.mark.parametrize(
        "bad",
        ["mom20", "Momentum20", "factor_001", "20d", "_mom", "mom_", "mom__20d", "mom-20d", "mom 20d"],
    )
    def test_bad_name_rejected(self, bad):
        with pytest.raises(ValueError, match="snake_case"):
            type(
                "BadName",
                (Factor,),
                {"name": bad, "depends_on": lambda s: [], "compute": lambda s, d: None},
            )

    @pytest.mark.parametrize(
        "good",
        ["momentum_20d", "vol_adj_momentum", "cs_rank_close", "sentiment_flow_5d", "a_b", "x1_y2_z3"],
    )
    def test_good_name_accepted(self, good):
        klass = type(
            "GoodName",
            (Factor,),
            {"name": good, "depends_on": lambda s: [], "compute": lambda s, d: None},
        )
        assert klass.name == good

    def test_intermediate_abstract_class_without_name_fails(self):
        """The contract requires every Factor subclass to carry a name."""
        with pytest.raises(ValueError, match="name"):
            type("AbstractMid", (Factor,), {})


# ---------------------------------------------------------------------------
# Factor ABC defaults and IO contract
# ---------------------------------------------------------------------------


class TestFactorDefaults:
    def test_default_metadata(self):
        assert MinimalFactor.description == ""
        assert MinimalFactor.category == "custom"
        assert MinimalFactor.frequency == "daily"
        assert MinimalFactor.asset_types == ["equity"]
        assert MinimalFactor.version == "1.0"

    def test_subclass_overrides(self):
        assert Momentum20D.name == "momentum_20d"
        assert Momentum20D.category == "time_series"
        assert Momentum20D.version == "1.0"
        assert VolAdjMomentum.version == "2.1"

    def test_depends_on_returns_list(self):
        result = Momentum20D().depends_on()
        assert isinstance(result, list)
        assert all(isinstance(x, str) for x in result)

    def test_depends_on_is_static_declaration(self):
        """No emitter, no listener, no callback registry — just a list."""
        inst = Momentum20D()
        assert inst.depends_on() == ["bars_daily"]
        assert inst.depends_on() == ["bars_daily"]  # deterministic

    def test_compute_returns_dataframe(self):
        data = {
            "bars_daily": pd.DataFrame(
                {
                    "date": pd.to_datetime(
                        ["2024-01-01"] * 3 + ["2024-01-02"] * 3 + ["2024-01-25"] * 3
                    ),
                    "ticker": ["A", "B", "C"] * 3,
                    "close": [10.0, 20.0, 30.0, 10.5, 21.0, 30.3, 12.0, 22.0, 33.0],
                }
            )
        }
        out = Momentum20D().compute(data)
        assert isinstance(out, pd.DataFrame)
        assert list(out.columns) == ["date", "ticker", "momentum_20d"]


# ---------------------------------------------------------------------------
# Output schema validation
# ---------------------------------------------------------------------------


class TestValidateOutput:
    def test_correct_schema_passes(self):
        df = pd.DataFrame(
            {"date": ["2024-01-01"], "ticker": ["A"], "momentum_20d": [0.1]}
        )
        result = validate_output("momentum_20d", df)
        assert result is df

    def test_missing_column_rejected(self):
        df = pd.DataFrame({"date": ["2024-01-01"], "ticker": ["A"]})
        with pytest.raises(ValueError, match="columns"):
            validate_output("momentum_20d", df)

    def test_extra_column_rejected(self):
        df = pd.DataFrame(
            {
                "date": ["2024-01-01"],
                "ticker": ["A"],
                "momentum_20d": [0.1],
                "extra": [1],
            }
        )
        with pytest.raises(ValueError, match="columns"):
            validate_output("momentum_20d", df)

    def test_wrong_column_name_rejected(self):
        df = pd.DataFrame({"date": ["2024-01-01"], "ticker": ["A"], "wrong": [0.1]})
        with pytest.raises(ValueError, match="columns"):
            validate_output("momentum_20d", df)

    def test_wrong_order_rejected(self):
        df = pd.DataFrame(
            {"ticker": ["A"], "date": ["2024-01-01"], "momentum_20d": [0.1]}
        )
        with pytest.raises(ValueError, match="columns"):
            validate_output("momentum_20d", df)

    def test_exactly_three_columns(self):
        df = pd.DataFrame(
            {"date": ["2024-01-01"], "ticker": ["A"], "momentum_20d": [0.1]}
        )
        assert len(df.columns) == 3
        validate_output("momentum_20d", df)

    def test_factor_compute_output_validates(self):
        """End-to-end: fixture compute() output passes the schema check."""
        data = {
            "bars_daily": pd.DataFrame(
                {
                    "date": pd.to_datetime(
                        ["2024-01-01"] * 2 + ["2024-01-02"] * 2 + ["2024-01-25"] * 2
                    ),
                    "ticker": ["A", "B"] * 3,
                    "close": [10.0, 20.0, 10.5, 21.0, 12.0, 22.0],
                }
            )
        }
        out = Momentum20D().compute(data)
        validate_output("momentum_20d", out)


# ---------------------------------------------------------------------------
# Metadata extraction
# ---------------------------------------------------------------------------


class TestFactorMeta:
    def test_fields(self):
        meta = factor_meta(Momentum20D)
        assert meta["name"] == "momentum_20d"
        assert meta["description"] == "20-day momentum"
        assert meta["category"] == "time_series"
        assert meta["frequency"] == "daily"
        assert meta["asset_types"] == ["equity"]
        assert meta["version"] == "1.0"

    def test_defaults_extracted(self):
        meta = factor_meta(MinimalFactor)
        assert meta["name"] == "min_fac"
        assert meta["description"] == ""
        assert meta["category"] == "custom"
        assert meta["frequency"] == "daily"
        assert meta["asset_types"] == ["equity"]
        assert meta["version"] == "1.0"

    def test_exactly_six_fields(self):
        meta = factor_meta(Momentum20D)
        assert set(meta.keys()) == {
            "name",
            "description",
            "category",
            "frequency",
            "asset_types",
            "version",
        }

    def test_missing_attribute_is_none(self):
        class NoDesc(Factor):
            name = "no_desc_fac"
            description = None  # type: ignore[assignment]

            def depends_on(self):
                return []

            def compute(self, data):
                return pd.DataFrame(columns=["date", "ticker", "no_desc_fac"])

        meta = factor_meta(NoDesc)
        assert meta["description"] is None

    def test_extract_factor_meta_from_classes(self):
        rows = extract_factor_meta([Momentum20D, VolAdjMomentum])
        assert len(rows) == 2
        assert rows[0]["name"] == "momentum_20d"
        assert rows[1]["name"] == "vol_adj_momentum"
        assert rows[1]["version"] == "2.1"

    def test_extract_factor_meta_from_registry(self):
        from core.registry import FactorRegistry

        FactorRegistry._store.clear()
        try:
            FactorRegistry.register(Momentum20D)
            FactorRegistry.register(VolAdjMomentum)
            rows = extract_factor_meta()
            assert len(rows) == 2
            names = {r["name"] for r in rows}
            assert names == {"momentum_20d", "vol_adj_momentum"}
        finally:
            FactorRegistry._store.clear()

    def test_extract_factor_meta_empty_registry(self):
        from core.registry import FactorRegistry

        FactorRegistry._store.clear()
        try:
            assert extract_factor_meta() == []
        finally:
            FactorRegistry._store.clear()


# ---------------------------------------------------------------------------
# event / alternative category shape (Dev default #2)
# ---------------------------------------------------------------------------


class TestEventAlternativeShape:
    def test_event_factor_declares_bare_table_dep(self):
        """Event inputs use bare table names, same prefix contract."""
        deps = EventFactor().depends_on()
        assert deps == ["earnings_calendar"]
        kind, name = parse_dependency(deps[0])
        assert kind == KIND_TABLE
        assert name == "earnings_calendar"

    def test_alternative_factor_declares_bare_table_dep(self):
        deps = AltSentiment().depends_on()
        assert deps == ["money_flow"]
        kind, name = parse_dependency(deps[0])
        assert kind == KIND_TABLE

    def test_event_factor_output_schema(self):
        data = {
            "earnings_calendar": pd.DataFrame(
                {
                    "date": ["2024-01-01"],
                    "ticker": ["A"],
                    "surprise": [0.05],
                    "eps_actual": [1.2],
                }
            )
        }
        out = EventFactor().compute(data)
        validate_output("earnings_surprise_1d", out)

    def test_alternative_factor_output_schema(self):
        data = {
            "money_flow": pd.DataFrame(
                {
                    "date": ["2024-01-01"],
                    "ticker": ["A"],
                    "net_flow": [1e6],
                    "total_flow": [5e6],
                }
            )
        }
        out = AltSentiment().compute(data)
        validate_output("sentiment_flow_5d", out)

    def test_event_and_alternative_are_valid_categories(self):
        assert EventFactor.category in VALID_CATEGORIES
        assert AltSentiment.category in VALID_CATEGORIES


# ---------------------------------------------------------------------------
# version convention (Dev default #1)
# ---------------------------------------------------------------------------


class TestVersionConvention:
    def test_default_version(self):
        assert Factor.version == "1.0"

    def test_version_is_string(self):
        assert isinstance(Momentum20D.version, str)
        assert isinstance(VolAdjMomentum.version, str)

    def test_version_bump_changes_identity(self):
        """Two versions of the same factor name are distinct entries."""
        assert Momentum20D.version != VolAdjMomentum.version

    def test_version_visible_in_meta(self):
        meta = factor_meta(VolAdjMomentum)
        assert meta["version"] == "2.1"


# ---------------------------------------------------------------------------
# depends_on prefix round-trip with Factor
# ---------------------------------------------------------------------------


class TestDependsOnIntegration:
    def test_table_deps_parse_as_table(self):
        for dep in Momentum20D().depends_on():
            kind, _ = parse_dependency(dep)
            assert kind == KIND_TABLE

    def test_factor_deps_parse_as_factor(self):
        for dep in VolAdjMomentum().depends_on():
            kind, _ = parse_dependency(dep)
            assert kind == KIND_FACTOR

    def test_mixed_deps(self):
        class Mixed(Factor):
            name = "mixed_dep_fac"

            def depends_on(self):
                return ["bars_daily", "factor:momentum_20d"]

            def compute(self, data):
                return pd.DataFrame(columns=["date", "ticker", "mixed_dep_fac"])

        kinds = [parse_dependency(d)[0] for d in Mixed().depends_on()]
        assert kinds == [KIND_TABLE, KIND_FACTOR]

    def test_bad_dep_would_be_rejected(self):
        """A typo'd prefix in depends_on() is caught by parse_dependency."""

        class BadDep(Factor):
            name = "bad_dep_fac"

            def depends_on(self):
                return ["event:typo"]

            def compute(self, data):
                return pd.DataFrame(columns=["date", "ticker", "bad_dep_fac"])

        with pytest.raises(ValueError, match="unrecognised"):
            parse_dependency(BadDep().depends_on()[0])
