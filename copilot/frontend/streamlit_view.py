from __future__ import annotations

"""Streamlit views for Copilot chat and report."""

import glob
import os
from pathlib import Path
from typing import List

import openai
import streamlit as st
from dotenv import load_dotenv
import requests
import io
import tempfile

# Use unified prompt builder
from copilot.llm import prompt_builder

# --- Secret / API Key Management ---

def get_openai_client() -> openai.OpenAI:
    """
    Initializes and returns an OpenAI client.

    It tries to get the API key from Streamlit secrets first, which is the standard
    for deployed apps. Falls back to environment variables (and .env file) for
    local development.
    """
    api_key = None
    st.info("Attempting to configure OpenAI client...")

    # First, try Streamlit secrets (for deployed app)
    if hasattr(st, 'secrets') and 'OPENAI_API_KEY' in st.secrets:
        st.info("Found 'st.secrets', attempting to read 'OPENAI_API_KEY'.")
        api_key = st.secrets.get('OPENAI_API_KEY')
        if api_key:
            st.info(f"OpenAI API key loaded from st.secrets. Length: {len(api_key)}. Key starts with: {api_key[:4]}")
        else:
            st.warning("Found 'OPENAI_API_KEY' in st.secrets, but it is empty.")
    else:
        st.info("No 'OPENAI_API_KEY' in st.secrets. Falling back to environment variables.")
        # Fallback to environment variables (for local development)
        load_dotenv()
        api_key = os.getenv("OPENAI_API_KEY")
        if api_key:
            st.info(f"OpenAI API key loaded from .env file. Length: {len(api_key)}. Key starts with: {api_key[:4]}")
        else:
            st.warning("No 'OPENAI_API_KEY' found in environment variables.")

    if not api_key:
        st.error("Fatal: OpenAI API key is not set. The application cannot proceed. Please add the key to your Streamlit secrets or a local .env file.")
        st.stop()
    
    return openai.OpenAI(api_key=api_key)

# Get the client once and use it throughout the module
client = get_openai_client()

# Summary files live under ``copilot/summaries`` (not repo-root /summaries).
# Resolve the path robustly even if the folder is moved later.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
_default_dir = PROJECT_ROOT / "copilot" / "summaries"
SUMMARIES_DIR = _default_dir if _default_dir.exists() else PROJECT_ROOT / "summaries"
BACKEND_URL = os.getenv("COPILOT_BACKEND_URL", "http://localhost:8000")

BUSINESS_QUESTIONS = [
    "1. How should we modify Google Search campaigns, times, and keyword bids?",
    "2. What blog posts should we publish next, and can the LLM create deep research prompts?",
    "3. How has the overall position of the shop changed, and what were the drivers?",
    "4. What are the top performing products, and what is the typical user behaviour for them?",
    "5. Where are users coming from, did that change with time, and do the data suggest any improvements?",
    "6. What are Pinterest trends telling us about our relevance, and should we add new products?",
]

# ---------------------------------------------------------------------------
# Helper to load the most recent *30-day* summary for a given source.
# The summarisation scripts now save files as ``<prefix>_summary_30d.md``
# instead of the previous 7-day suffix.
# ---------------------------------------------------------------------------

SUMMARY_WINDOW = "30d"  # keep a single place to change the window length


def _load_latest_summary(prefix: str) -> str | None:
    pattern = f"{prefix}_summary_{SUMMARY_WINDOW}.md"
    files = sorted(glob.glob(str(SUMMARIES_DIR / pattern)))
    if files:
        with open(files[-1]) as f:
            return f.read()
    return None


def generate_report(client: openai.OpenAI, model: str = "gpt-3.5-turbo-0125") -> str:
    """Generate the full performance report via prompt_builder & OpenAI."""

    # Gather latest summaries as extra context chunks
    ga4 = _load_latest_summary("ga4")
    gsc = _load_latest_summary("search_console")
    ads = _load_latest_summary("google_ads")
    combined = _load_latest_summary("combined")

    if not (ga4 and gsc and ads and combined):
        st.warning("One or more summaries are missing. Run summarisation scripts first.")
        return ""

    # Load JSON summaries for data-rich context
    def _read_json(prefix: str):
        p = SUMMARIES_DIR / f"{prefix}_summary_{SUMMARY_WINDOW}.json"
        return p.read_text() if p.exists() else None

    ga4_json = _read_json("ga4")
    ads_json = _read_json("google_ads")
    sc_json = _read_json("search_console")

    if not (ga4_json and ads_json and sc_json):
        st.warning("One or more JSON summaries missing – run summarisation.")
        return ""

    context_chunks = [
        "GA4 Summary (Markdown):\n" + ga4,
        "GA4 Summary (JSON):\n```json\n" + ga4_json + "\n```",
        "Search Console Summary (Markdown):\n" + gsc,
        "Search Console Summary (JSON):\n```json\n" + sc_json + "\n```",
        "Google Ads Summary (Markdown):\n" + ads,
        "Google Ads Summary (JSON):\n```json\n" + ads_json + "\n```",
        "Combined Summary (Markdown):\n" + combined,
    ]

    # Use a fixed question prompt to trigger full report generation
    question_txt = "Generate the detailed performance report following the required structure."

    messages = prompt_builder.build_messages(
        question=question_txt,
        window=30,
        context_chunks=context_chunks,
    )

    # Increase the token allowance so that richer GPT-4o reports don't get
    # truncated. 2 000 tokens gives ample room for the longer multi-section
    # markdown output plus JSON context reflections.
    response = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=0.3,
        max_tokens=2000,
    )

    return response.choices[0].message.content


# ---------------------------------------------------------------------------
# TTS helpers (OpenAI Speech API)
# ---------------------------------------------------------------------------

def _text_to_speech(client: openai.OpenAI, text: str, *, voice: str = "alloy", tts_model: str = "tts-1") -> bytes:
    """Return MP3 bytes for *text* using the OpenAI Speech API.

    Requires the Python `openai` ≥1.3 package and a valid `OPENAI_API_KEY` env var.
    """
    try:
        response = client.audio.speech.create(
            model=tts_model,
            voice=voice,
            input=text,
            response_format="mp3",
        )

        # Newer SDK versions expose the binary directly
        if hasattr(response, "audio") and hasattr(response.audio, "data"):
            return response.audio.data  # type: ignore[attr-defined]

        # Fallback for SDKs that require a filename. We stream to a temporary
        # file and then read its contents back into memory so we can hand the
        # bytes to Streamlit.
        with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            response.stream_to_file(tmp_path)  # type: ignore[arg-type]
            with open(tmp_path, "rb") as f:
                return f.read()
        finally:
            try:
                os.remove(tmp_path)
            except OSError:
                pass

    except Exception as exc:
        st.error(f"TTS generation failed: {exc}")
        return b""


# ---------------------------------------------------------------------------
# Streamlit render functions
# ---------------------------------------------------------------------------

def render_report():
    """Render the Copilot *Report* tab in Streamlit including a model selector."""

    st.header("🧠 AI-Generated Insights")
    st.markdown(
        """
        **What this does:** Uses AI to analyze your data and create summary reports with trends and recommendations. 
        Takes 30 days of data from Google Analytics, Google Ads, and Search Console to tell you what's working and what isn't.

        **Current version:** Uses GPT to write narrative summaries based on your performance data. 
        You can choose different AI models using the dropdown in the sidebar.
        
        **What I'm building next:**
        
        • **Natural language chat** — Ask follow-up questions in plain English with the system remembering your conversation history
        
        • **Smarter quality detection** — Better ways to identify valuable visitors vs. random traffic
        
        • **Self-checking analysis** — AI that reviews its own conclusions for better accuracy
        
        • **Focused reports** — Separate analysis for different products, locations, and campaigns
        
        • **Better recommendations** — More specific suggestions about what to do next
        
        The goal is to move from "here's what happened" to "here's what you should do about it."
        """
    )

    # ▸ Sidebar ‑-- choose model
    with st.sidebar:
        st.markdown("### LLM Settings")
        available_models = [
            "gpt-3.5-turbo-0125",
            "gpt-4o-2024-05-13",
            "gpt-4o-mini-2024-05-13",
        ]

        # Default to env var or fall back to first option
        default_model = os.getenv("OPENAI_COMPLETION_MODEL", available_models[0])
        if default_model not in available_models:
            available_models.insert(0, default_model)

        selected_model = st.selectbox("Model to use", available_models, index=available_models.index(default_model))

    # Keep the generated report in session_state so that we can reuse it for
    # TTS without incurring extra token cost unless requested.

    if st.button("Generate Report", key="copilot_report_btn"):
        client = get_openai_client() # Initialize client just-in-time
        with st.spinner(f"Generating report via {selected_model} …"):
            report = generate_report(client=client, model=selected_model)

        st.session_state["latest_report"] = report
        st.markdown(report)

    # ───────────────────────────────────────────────────────────────────────
    # Text-to-Speech playback
    # ───────────────────────────────────────────────────────────────────────

    if "latest_report" in st.session_state and st.session_state["latest_report"]:
        with st.expander("🔊 Listen to this report", expanded=False):
            voice_choice = st.selectbox(
                "Voice",
                ["alloy", "nova", "shimmer", "fable", "echo", "onyx"],
                index=0,
                key="tts_voice_select",
            )

            if st.button("Generate Audio", key="tts_generate_btn"):
                client = get_openai_client() # Initialize client just-in-time
                with st.spinner("Generating audio…"):
                    audio_bytes = _text_to_speech(client=client, text=st.session_state["latest_report"], voice=voice_choice)

                if audio_bytes:
                    st.audio(audio_bytes, format="audio/mp3")


def render_chat():
    st.header("💬 Chat with Your Data")
    st.markdown(
        """
        **What this does:** Ask questions about your data in plain English and get answers back. 
        The system looks through your analytics data and uses AI to give you relevant responses.
        
        **Try asking things like:**
        
        • "Are mobile users behaving differently than desktop users?"
        
        • "What's causing the recent spike in traffic?"
        
        • "Which products get the most engagement but lowest sales?"
        
        • "What should I focus on based on this month's data?"
        
        Great for quick questions and exploring hunches without building complex reports.
        """
    )
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []  # list of (question, answer, suggestions)

    # Helper to POST a question and update chat_history
    def _send_question(question: str):
        try:
            resp = requests.post(f"{BACKEND_URL}/chat", json={"question": question})
            resp.raise_for_status()
            data = resp.json()
            answer = data["answer"]
            suggestions = data.get("suggestions", [])
            st.session_state.chat_history.append((question, answer, suggestions))
        except Exception as e:
            st.error(f"Backend error: {e}")

    user_input = st.text_input("Ask a question about Ads & SEO…", key="chat_input")
    if st.button("Send", key="chat_send_btn") and user_input:
        _send_question(user_input)

    # Display history
    for q, a, suggs in st.session_state.chat_history[::-1]:
        st.markdown(f"**You:** {q}")
        st.markdown(f"**Copilot:** {a}")

        if suggs:
            st.markdown("_Follow-up suggestions:_")
            cols = st.columns(len(suggs))
            for idx, s in enumerate(suggs):
                if cols[idx].button(s, key=f"sugg_{len(st.session_state.chat_history)}_{idx}"):
                    _send_question(s) 