"""
Configuration module for POPS Analytics System.

This module centralizes all configuration settings for the POPS Analytics system,
loading values from environment variables with sensible defaults.
"""
import os
import sys
import logging
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv

# Initialize feature flags
from utils.feature_flags import init_feature_flags

# Load environment variables from .env file
load_dotenv()

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Check for required environment variables
def _check_required_env_vars() -> None:
    """Check that required environment variables are set."""
    required_vars = [
        "OPENAI_API_KEY",
        "GOOGLE_APPLICATION_CREDENTIALS"
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        logger.error("Please set these variables in your .env file or environment")
        logger.error("See .env.example for a template")

# API Keys and Authentication
# Make the app Streamlit-aware: try to get the key from st.secrets first.
try:
    import streamlit as st
    if 'OPENAI_API_KEY' in st.secrets:
        OPENAI_API_KEY = st.secrets['OPENAI_API_KEY']
    else:
        OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
except (ImportError, AttributeError):
    # Fallback for non-Streamlit environments (e.g., backend, scripts)
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if not OPENAI_API_KEY:
    logger.warning("OPENAI_API_KEY not set - LLM functionality will not work")

# OpenAI Configuration
OPENAI_EMBEDDING_MODEL = os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-ada-002")
OPENAI_COMPLETION_MODEL = os.getenv("OPENAI_COMPLETION_MODEL", "gpt-4-turbo-preview")

# Suggestion generation model (cheap & fast)
COPILOT_SUGGESTION_MODEL = os.getenv("COPILOT_SUGGESTION_MODEL", "gpt-3.5-turbo-0125")

# Google Cloud Settings
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if not GOOGLE_APPLICATION_CREDENTIALS:
    logger.warning("GOOGLE_APPLICATION_CREDENTIALS not set - Google API functionality will not work")

GOOGLE_CLOUD_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT", "ga4-pops")
BIGQUERY_DATASET_ID = os.getenv("BIGQUERY_DATASET_ID", "analytics_386545135")

# Google Ads API Settings
GOOGLE_ADS_DEVELOPER_TOKEN = os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN", "PAMTTq3nXJXqye9xWgUsVw")
GOOGLE_ADS_CLIENT_ID = os.getenv("GOOGLE_ADS_CLIENT_ID", "81313229989-ums92qsu9v4sr0gtkvuepie9pmijk9lt.apps.googleusercontent.com")
GOOGLE_ADS_CLIENT_SECRET = os.getenv("GOOGLE_ADS_CLIENT_SECRET", "GOCSPX-bdMQ4oSWvSjT9D5Rh00IH5qvLEI")
GOOGLE_ADS_REFRESH_TOKEN = os.getenv("GOOGLE_ADS_REFRESH_TOKEN", "1//04N_g-ZQlNWjlCgYIARAaGAQSNwF-L9IrTLEGUIe8vB5HT1eCAuwa85GYQYHsFkf1wZFXHZ8gW10CW9zKX-o18e00-HkZPRT2PE")
GOOGLE_ADS_YAML_PATH = os.getenv("GOOGLE_ADS_YAML_PATH", os.path.join(os.path.dirname(__file__), "google-ads.yaml"))
GOOGLE_ADS_LOGIN_CUSTOMER_ID = os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "9017808460")  # 901-780-8460 without hyphens
GOOGLE_ADS_CUSTOMER_ID = os.getenv("GOOGLE_ADS_CUSTOMER_ID", "1940970197")  # 194-097-0197 without hyphens
GOOGLE_ADS_USE_PROTO_PLUS = os.getenv("GOOGLE_ADS_USE_PROTO_PLUS", "True").lower() == "true"

# GA4 Settings
GA4_PROPERTY_ID = os.getenv("GA4_PROPERTY_ID", "")
GA4_IMPLEMENTATION_DATE = os.getenv("GA4_IMPLEMENTATION_DATE", "2024-09-19")

# Search Console Settings
SEARCH_CONSOLE_SITE_URL = os.getenv("SEARCH_CONSOLE_SITE_URL", "sc-domain:pops.studio")

# Email Settings
SMTP_SERVER = os.getenv("SMTP_SERVER", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USERNAME = os.getenv("SMTP_USERNAME", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
EMAIL_RECIPIENTS = os.getenv("EMAIL_RECIPIENTS", "").split(",") if os.getenv("EMAIL_RECIPIENTS") else []

# System Settings
REPORT_FREQUENCY = os.getenv("REPORT_FREQUENCY", "weekly")  # daily, weekly, monthly
LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4")
LLM_TEMPERATURE = float(os.getenv("LLM_TEMPERATURE", "0.0"))

# Memory System Settings
VECTOR_DB_HOST = os.getenv("QDRANT_HOST", "localhost")
VECTOR_DB_PORT = int(os.getenv("QDRANT_PORT", "6333"))
VECTOR_DB_COLLECTION = os.getenv("QDRANT_COLLECTION", "pops_memories")
VECTOR_DB_DIMENSIONS = int(os.getenv("VECTOR_DB_DIMENSIONS", "1536"))  # Dimensions for text-embedding-ada-002

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
LOGS_DIR = os.path.join(BASE_DIR, "logs")
HISTORY_DIR = os.path.join(DATA_DIR, "history")
MEMORY_DIR = os.path.join(DATA_DIR, "memories")
REPORT_DIR = os.path.join(HISTORY_DIR, "reports")

# Business-specific configuration
HIGH_VALUE_THRESHOLD = float(os.getenv("HIGH_VALUE_THRESHOLD", "300.0"))  # Orders above this value are considered high-value
PRODUCT_CATEGORIES = [
    "Bathroom Vanity Unit", 
    "Floating Vanity Unit",
    "Indoor Swing",
    "Outdoor Swing",
    "Accesories"
]

# Ensure directories exist
#os.makedirs(DATA_DIR, exist_ok=True)
#os.makedirs(LOGS_DIR, exist_ok=True)
#os.makedirs(HISTORY_DIR, exist_ok=True)
#os.makedirs(MEMORY_DIR, exist_ok=True)
#os.makedirs(REPORT_DIR, exist_ok=True)

# Initialize feature flags
init_feature_flags()

# Check required environment variables on import
_check_required_env_vars()

# SQL Query Templates - Moved to separate module for better organization
#from data.query_templates import QUERY_TEMPLATES
