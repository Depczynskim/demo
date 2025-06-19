"""
Simple feature flags implementation for POPS Analytics System.
"""
import os
from typing import Dict

# Global feature flags dictionary
FEATURE_FLAGS: Dict[str, bool] = {
    "use_google_ads_api": True,
    "use_search_console_api": True,
    "use_bigquery_api": True,
    "use_ga4_api": True,
}

def init_feature_flags() -> None:
    """Initialize feature flags from environment variables."""
    for flag_name in FEATURE_FLAGS:
        env_var_name = f"ENABLE_{flag_name.upper()}"
        FEATURE_FLAGS[flag_name] = os.getenv(env_var_name, "true").lower() == "true"

def is_feature_enabled(feature_name: str) -> bool:
    """Check if a feature is enabled."""
    return FEATURE_FLAGS.get(feature_name, False) 