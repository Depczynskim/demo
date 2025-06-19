import streamlit as st

def render():
    """Render the introductory overview page."""
    st.title("Welcome to the POPS Analytics Dashboard!")

    st.markdown(
        """
        This dashboard provides a live look into the marketing and sales performance for an e-commerce store. 
        It automatically gathers data from Google Analytics, Google Ads, and Search Console to present a 
        unified view of what's working and where there are opportunities to grow.

        ---

        ### How to Use This Dashboard

        *   **🔍 Search Console:** Discover which search terms are driving traffic and see how different pages rank on Google.
        *   **💰 Google Ads:** Track the performance and cost of ad campaigns to see which ones are most effective.
        *   **🛍️ Products:** Analyze sales and traffic for individual products to understand customer behavior.
        *   **🧠 AI-Generated Insights:** Get automated summaries and suggestions powered by OpenAI to quickly spot trends.
        *   **🔬 GA4 Browser:** Explore raw event data for in-depth, granular analysis.

        ---

        ### About the Project
        This is a personal project built with Python and Streamlit, designed to provide valuable, easy-to-understand 
        insights for a small business. The AI features use the OpenAI API to generate helpful reports. All data is anonymized.

        ---

        ### Future Development
        I'm currently focused on enhancing the AI capabilities to provide more predictive insights, such as identifying 
        high-value user segments and offering specific recommendations to improve marketing ROI. The goal is to 
        evolve this from a reporting tool into a proactive decision-making assistant.
        """,
        unsafe_allow_html=False,
    ) 