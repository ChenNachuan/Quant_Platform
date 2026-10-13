"""Tests for core.registry — decorator registration, metadata extraction, factory.

Covers every DoD row: register / list / get_class / create_instance, duplicate
name raises, unknown name raises ``KeyError``, ``list_factor_meta`` key set,
missing metadata -> ``None``, invalid ``status`` fails loudly, and a plain
(non-Factor) class registers — proof the mechanism is domain-agnostic.
"""

import pytest

from core.registry import (
    META_FIELDS,
    VALID_STATUSES,
    FactorRegistry,
    RuleRegistry,
    list_factor_meta,
    register_factor,
)


@pytest.fixture(autouse=True)
def _isolated_registries():
    """Each test starts and ends with both registries empty."""
    FactorRegistry._store.clear()
    RuleRegistry._store.clear()
    yield
    FactorRegistry._store.clear()
    RuleRegistry._store.clear()


class TestRegisterAndList:
    def test_decorator_registers_and_returns_class(self):
        @register_factor
        class Momentum:
            name = "momentum_20d"

        assert FactorRegistry.get_class("momentum_20d") is Momentum
        assert "momentum_20d" in FactorRegistry.list_all()

    def test_register_returns_class_for_decorator_use(self):
        class Momentum:
            name = "momentum_20d"

        assert FactorRegistry.register(Momentum) is Momentum
        assert FactorRegistry.get_class("momentum_20d") is Momentum

    def test_key_prefers_name_attribute(self):
        @register_factor
        class Momentum20d:
            name = "momentum_20d"

        assert FactorRegistry.list_all() == ["momentum_20d"]
        assert "Momentum20d" not in FactorRegistry.list_all()

    def test_key_falls_back_to_class_name(self):
        @register_factor
        class Plain:
            pass

        assert FactorRegistry.list_all() == ["Plain"]
        assert FactorRegistry.get_class("Plain") is Plain

    def test_list_all_is_registration_order(self):
        for key in ("a", "b", "c"):
            klass = type(f"K_{key}", (), {"name": key})
            FactorRegistry.register(klass)
        assert FactorRegistry.list_all() == ["a", "b", "c"]

    def test_duplicate_name_raises_no_silent_overwrite(self):
        @register_factor
        class First:
            name = "dup"

        class Second:
            name = "dup"

        with pytest.raises(ValueError):
            FactorRegistry.register(Second)
        assert FactorRegistry.get_class("dup") is First
        assert FactorRegistry.list_all() == ["dup"]

    def test_duplicate_via_class_name_raises(self):
        first = type("Twin", (), {})
        second = type("Twin", (), {})
        FactorRegistry.register(first)
        with pytest.raises(ValueError):
            FactorRegistry.register(second)
        assert FactorRegistry.get_class("Twin") is first


class TestGetClassAndFactory:
    def test_get_class_returns_registered_type(self):
        @register_factor
        class Momentum:
            name = "momentum_20d"

        assert FactorRegistry.get_class("momentum_20d") is Momentum
        assert isinstance(FactorRegistry.get_class("momentum_20d"), type)

    def test_get_class_unknown_raises_keyerror(self):
        with pytest.raises(KeyError):
            FactorRegistry.get_class("nope")

    def test_create_instance_builds_object(self):
        @register_factor
        class WithInit:
            name = "with_init"

            def __init__(self, alpha):
                self.alpha = alpha

        obj = FactorRegistry.create_instance("with_init", alpha=2)
        assert isinstance(obj, WithInit)
        assert obj.alpha == 2

    def test_create_instance_passes_all_kwargs(self):
        @register_factor
        class WithInit:
            name = "with_init"

            def __init__(self, alpha, beta=1, gamma="g"):
                self.alpha = alpha
                self.beta = beta
                self.gamma = gamma

        obj = FactorRegistry.create_instance("with_init", alpha=2, beta=3, gamma="z")
        assert (obj.alpha, obj.beta, obj.gamma) == (2, 3, "z")

    def test_create_instance_default_kwargs(self):
        @register_factor
        class WithDefault:
            name = "with_default"

            def __init__(self, beta=1):
                self.beta = beta

        assert FactorRegistry.create_instance("with_default").beta == 1

    def test_create_instance_unknown_raises_keyerror(self):
        with pytest.raises(KeyError):
            FactorRegistry.create_instance("nope")


class TestListFactorMeta:
    def test_keys_are_exactly_meta_fields(self):
        @register_factor
        class Full:
            name = "full"
            version = "1.0"
            description = "d"
            category = "momentum"
            frequency = "daily"
            asset_types = ["stock"]
            depends_on = []
            status = "draft"
            created_at = "2026-10-13"

        meta = list_factor_meta()
        assert len(meta) == 1
        assert list(meta[0].keys()) == META_FIELDS
        assert set(meta[0].keys()) == set(META_FIELDS)

    def test_meta_field_values_are_extracted(self):
        @register_factor
        class Full:
            name = "full"
            version = "1.0"
            description = "d"
            category = "momentum"
            frequency = "daily"
            asset_types = ["stock"]
            depends_on = ["bars_daily"]
            status = "production"
            created_at = "2026-10-13"

        row = list_factor_meta()[0]
        assert row["name"] == "full"
        assert row["version"] == "1.0"
        assert row["description"] == "d"
        assert row["category"] == "momentum"
        assert row["frequency"] == "daily"
        assert row["asset_types"] == ["stock"]
        assert row["depends_on"] == ["bars_daily"]
        assert row["status"] == "production"
        assert row["created_at"] == "2026-10-13"

    def test_missing_attributes_are_none(self):
        @register_factor
        class Bare:
            name = "bare"

        row = list_factor_meta()[0]
        assert row["name"] == "bare"
        for field in META_FIELDS:
            if field != "name":
                assert row[field] is None, f"{field} should be None"

    def test_no_attributes_at_all_is_all_none(self):
        @register_factor
        class Empty:
            pass

        row = list_factor_meta()[0]
        assert list(row.keys()) == META_FIELDS
        assert all(v is None for v in row.values())

    def test_meta_covers_every_registered_factor(self):
        for key in ("a", "b"):
            klass = type(f"K_{key}", (), {"name": key})
            FactorRegistry.register(klass)
        rows = list_factor_meta()
        assert [r["name"] for r in rows] == ["a", "b"]


class TestStatus:
    def test_three_states_only(self):
        assert VALID_STATUSES == ("draft", "production", "deprecated")
        assert len(VALID_STATUSES) == 3

    @pytest.mark.parametrize("status", ["draft", "production", "deprecated"])
    def test_valid_status_registers(self, status):
        ok = type("Ok", (), {"name": "ok", "status": status})
        assert FactorRegistry.register(ok) is ok
        assert FactorRegistry.get_class("ok") is ok
        assert list_factor_meta()[0]["status"] == status

    @pytest.mark.parametrize("status", ["wip", "DRAFT", "archived", "", "live"])
    def test_invalid_status_raises(self, status):
        bad = type("Bad", (), {"name": "bad", "status": status})
        with pytest.raises(ValueError):
            FactorRegistry.register(bad)
        assert FactorRegistry.list_all() == []

    def test_missing_status_is_not_validated(self):
        @register_factor
        class NoStatus:
            name = "no_status"

        assert list_factor_meta()[0]["status"] is None

    def test_none_status_is_not_validated(self):
        @register_factor
        class NoneStatus:
            name = "none_status"
            status = None

        assert list_factor_meta()[0]["status"] is None


class TestDomainAgnostic:
    def test_plain_class_registers(self):
        """Proof of red line 3: no Factor base class is required."""

        class Plain:
            """Not a Factor subclass — Factor does not exist yet."""

            answer = 42

        assert FactorRegistry.register(Plain) is Plain
        assert FactorRegistry.get_class("Plain") is Plain
        assert FactorRegistry.create_instance("Plain").answer == 42

    def test_plain_class_via_decorator(self):
        @register_factor
        class Whatever:
            x = 1

        assert "Whatever" in FactorRegistry.list_all()

    def test_non_class_attributes_do_not_break_extraction(self):
        class Weird:
            name = "weird"
            depends_on = "not-a-list"

        FactorRegistry.register(Weird)
        assert list_factor_meta()[0]["depends_on"] == "not-a-list"


class TestRuleRegistry:
    def test_same_shape_as_factor_registry(self):
        for method in ("register", "list_all", "get_class", "create_instance"):
            assert callable(getattr(FactorRegistry, method, None))
            assert callable(getattr(RuleRegistry, method, None))

    def test_register_list_get_create(self):
        @RuleRegistry.register
        class LotSize:
            name = "lot_size"

            def __init__(self, n):
                self.n = n

        assert RuleRegistry.list_all() == ["lot_size"]
        assert RuleRegistry.get_class("lot_size") is LotSize
        assert RuleRegistry.create_instance("lot_size", n=100).n == 100

    def test_store_is_independent_of_factor_registry(self):
        class R:
            name = "shared_name"

        class F:
            name = "shared_name"

        RuleRegistry.register(R)
        FactorRegistry.register(F)
        assert RuleRegistry.get_class("shared_name") is R
        assert FactorRegistry.get_class("shared_name") is F
        assert FactorRegistry.get_class("shared_name") is not R

    def test_duplicate_name_raises(self):
        @RuleRegistry.register
        class First:
            name = "dup"

        class Second:
            name = "dup"

        with pytest.raises(ValueError):
            RuleRegistry.register(Second)
        assert RuleRegistry.get_class("dup") is First

    def test_get_class_unknown_raises_keyerror(self):
        with pytest.raises(KeyError):
            RuleRegistry.get_class("nope")

    def test_invalid_status_raises(self):
        class Bad:
            name = "bad_rule"
            status = "nope"

        with pytest.raises(ValueError):
            RuleRegistry.register(Bad)


class TestContract:
    def test_meta_fields_frozen(self):
        assert META_FIELDS == [
            "name",
            "version",
            "description",
            "category",
            "frequency",
            "asset_types",
            "depends_on",
            "status",
            "created_at",
        ]

    def test_register_factor_lands_in_factor_registry(self):
        class ViaSugar:
            name = "via_sugar"

        class ViaMethod:
            name = "via_method"

        register_factor(ViaSugar)
        FactorRegistry.register(ViaMethod)
        assert set(FactorRegistry.list_all()) == {"via_sugar", "via_method"}

    def test_no_third_registry(self):
        import core.registry as registry_module

        registries = [
            obj
            for obj in vars(registry_module).values()
            if isinstance(obj, type)
            and obj.__name__.endswith("Registry")
            and not obj.__name__.startswith("_")
        ]
        assert {r.__name__ for r in registries} == {"FactorRegistry", "RuleRegistry"}
