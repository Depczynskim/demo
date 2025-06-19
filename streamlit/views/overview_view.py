import streamlit as st

def render():
    """Render the introductory overview page."""
    st.title("POPS Analytics Dashboard")

    st.markdown(
        """
        ### What this dashboard does
        This is an analytics platform I built for my friend's e-commerce shop. It takes data from Google Analytics, 
        Google Ads, and Search Console and turns it into clear, actionable insights. 

        ### How to navigate
        **🔍 Search Console** — See which Google searches bring traffic and how your pages rank  
        **💰 Google Ads** — Understand ad performance, costs, and which campaigns get the most clicks  
        **🛍️ Products** — Deep dive into how individual products perform across different channels  
        **🧠 AI-Generated Insights** — Get automated reports with trend analysis and suggestions  
        **🔬 GA4 Browser** — Inspect individual user events for debugging and detailed analysis

        ### Technical setup
        Built with Python and Streamlit, connected to Google Analytics 4 and other data sources. 
        Uses OpenAI for intelligent summaries. All data is anonymized and shared with permission.

        ### What's coming next
        I'm working on improving the AI analysis with:
        
        • **Better quality detection** — Smarter ways to identify valuable vs. low-quality traffic
        • **Multi-step analysis** — AI that reviews its own work for better insights  
        • **Focused reporting** — Separate analysis by product, location, and campaign type
        
        The goal is to move from just showing what happened to suggesting what to do next.
        """,
        unsafe_allow_html=False,
    ) 