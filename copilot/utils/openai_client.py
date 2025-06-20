"""OpenAI client factory for both Streamlit Cloud and local environments."""

import os
from typing import Optional
import openai
from dotenv import load_dotenv

# Load environment variables from .env file (for local development)
load_dotenv()

def get_openai_client() -> openai.OpenAI:
    """Initialize and return an OpenAI client with proper API key configuration.
    
    This function handles both Streamlit Cloud and local development environments:
    1. In Streamlit Cloud: Uses st.secrets['OPENAI_API_KEY']
    2. In local dev: Uses OPENAI_API_KEY environment variable
    
    Returns:
        openai.OpenAI: Configured OpenAI client
        
    Raises:
        ValueError: If no API key is found in either environment
    """
    api_key: Optional[str] = None
    
    # Try Streamlit secrets first (for cloud deployment)
    try:
        import streamlit as st
        if hasattr(st, 'secrets') and 'OPENAI_API_KEY' in st.secrets:
            api_key = st.secrets['OPENAI_API_KEY']
    except ImportError:
        pass  # Not in a Streamlit environment
        
    # Fallback to environment variable (for local development)
    if not api_key:
        api_key = os.getenv("OPENAI_API_KEY")
        
    if not api_key:
        raise ValueError(
            "OpenAI API key not found. Please set either:\n"
            "1. OPENAI_API_KEY in Streamlit secrets (for cloud deployment)\n"
            "2. OPENAI_API_KEY environment variable (for local development)"
        )
    
    return openai.OpenAI(api_key=api_key) 