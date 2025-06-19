"""Top-level utils package.

This repo now has two *utils* modules:
1. ``streamlit/utils.py`` – helper functions/constants used by the Streamlit
   dashboard (DATA_REPO, get_filtered_date_range, …).
2. ``utils`` package (this folder) – common infra helpers (logger, error
   handling, feature flags, etc.).

To avoid breaking old import paths (``from utils import …``) in the dashboard
code, we import *everything* from ``streamlit.utils`` and re-export it here.
That way both the dashboard and backend modules can simply do ``import utils``
and access the combined namespace.
"""

from importlib import import_module as _imp

# ---------------------------------------------------------------------------
# Attempt to load the *dashboard* helper module that lives at
# ``<repo_root>/streamlit/utils.py``.  We **cannot** use the normal module
# name ``streamlit.utils`` because that would resolve to the *PyPI* "streamlit"
# package that is already imported above. Therefore we load the file explicitly
# and register it under a private module name.
# ---------------------------------------------------------------------------

import importlib.util as _ilu
from pathlib import Path as _Path

_REPO_ROOT = _Path(__file__).resolve().parents[1]
_DASH_UTILS_PATH = _REPO_ROOT / "streamlit" / "utils.py"

if _DASH_UTILS_PATH.exists():
    _spec = _ilu.spec_from_file_location("_dashboard_utils", _DASH_UTILS_PATH)
    _dash_utils = _ilu.module_from_spec(_spec)  # type: ignore[var-annotated]
    _spec.loader.exec_module(_dash_utils)  # type: ignore[union-attr]

    for _name in dir(_dash_utils):
        if not _name.startswith("_"):
            globals()[_name] = getattr(_dash_utils, _name)

# ---------------------------------------------------------------------------
# Re-export local helpers so ``from utils import logger`` etc. still works.
# ---------------------------------------------------------------------------

from .logging import *  # noqa: F401,F403
from .error_handler import *  # noqa: F401,F403
from .feature_flags import *  # noqa: F401,F403

# Clean-up helper names
for _name in list(globals()):
    if _name.startswith("_") and _name not in {"__all__", "__doc__", "__name__", "__package__", "__loader__", "__spec__"}:
        globals().pop(_name, None)
