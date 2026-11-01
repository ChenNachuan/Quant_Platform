"""Namespace shim so ``python -m dqc.report`` resolves (S9-T3).

The package actually lives at ``data/dqc/``; this shim only extends
``__path__`` so ``dqc.report`` / ``dqc.engine`` resolve to those files.
Canonical imports (``data.dqc.*``) keep their own module identity — the
engine's monkeypatchable seams and the check resolver rely on those names.
"""

from pathlib import Path

__path__ = [str(Path(__file__).resolve().parent.parent / "data" / "dqc")]
