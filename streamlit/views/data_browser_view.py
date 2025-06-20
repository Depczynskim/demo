import streamlit as st
import pandas as pd
from utils import get_filtered_date_range

def render(context):
    """Render the Data Browser view."""
    st.title("🔬 GA4 Raw Data Browser")

    with st.expander("ℹ️ About this view", expanded=False):
        st.markdown(
            """
            This view provides direct access to the raw event data from Google Analytics 4. It's a powerful tool 
            for detailed analysis and for verifying that analytics tracking is working as expected.

            **What you can do here:**
            
            *   **Filter Specific Events:** Isolate particular user interactions, such as `page_view` or `add_to_cart`, for granular analysis.
            *   **Debug Analytics:** Confirm that custom events and parameters are being captured correctly.
            *   **Explore User Journeys:** Follow sequences of events to understand detailed user behavior patterns.
            """
        )
    df = context['df']
    _parse_ga4_event_params = context['_parse_ga4_event_params']
    categorize_page_type = context['categorize_page_type']
    
    # Use new date handling utility
    df, date_info = get_filtered_date_range(df, context)
    
    if df.empty:
        st.warning("No data available for the selected date range.")
        return
        
    # Show current date range if dates are available
    start_date = date_info.get('start_date')
    end_date = date_info.get('end_date')
    available_days = date_info.get('available_days')
    
    if start_date and end_date:
        st.info(
            f"Analyzing data from {start_date.strftime('%Y-%m-%d')} to "
            f"{end_date.strftime('%Y-%m-%d')}"
            + (f" ({available_days} days)" if available_days else "")
        )
    
    # Parse event parameters
    df_parsed = _parse_ga4_event_params(df)
    # Add page categorization
    df_parsed['page_type'] = df_parsed['page_location'].apply(categorize_page_type)

    # Sidebar filters for GA4
    st.sidebar.subheader("GA4 Filters")

    # Page type filter (searchable if >10 options)
    page_types = ["All"] + sorted(df_parsed['page_type'].dropna().unique().tolist())
    if len(page_types) > 10:
        selected_page_type = st.sidebar.selectbox("Page Type", page_types, index=0, key="page_type", help="Type to search")
    else:
        selected_page_type = st.sidebar.selectbox("Page Type", page_types, index=0)

    # Event name filter (searchable if >10 options)
    event_names = ["All"] + sorted(df_parsed['event_name'].dropna().unique().tolist())
    if len(event_names) > 10:
        selected_event = st.sidebar.selectbox("Event Type", event_names, index=0, key="event_type", help="Type to search")
    else:
        selected_event = st.sidebar.selectbox("Event Type", event_names, index=0)

    # Apply filters
    filtered_df = df_parsed.copy()
    if selected_page_type != "All":
        filtered_df = filtered_df[filtered_df['page_type'] == selected_page_type]
    if selected_event != "All":
        filtered_df = filtered_df[filtered_df['event_name'] == selected_event]

    # Second-level filter for Product Pages and FAQ/Help
    second_level_label = None
    second_level_col = None
    second_level_options = None
    selected_second_level = None
    if selected_page_type == "Product Pages":
        if 'ecomm_prodid' in filtered_df.columns and filtered_df['ecomm_prodid'].notna().any():
            second_level_label = "Product ID"
            second_level_col = "ecomm_prodid"
            second_level_options = ["All"] + sorted(filtered_df[second_level_col].dropna().unique().tolist())
        elif 'item_id' in filtered_df.columns and filtered_df['item_id'].notna().any():
            second_level_label = "Item ID"
            second_level_col = "item_id"
            second_level_options = ["All"] + sorted(filtered_df[second_level_col].dropna().unique().tolist())
        elif 'page_title' in filtered_df.columns and filtered_df['page_title'].notna().any():
            second_level_label = "Product Title"
            second_level_col = "page_title"
            second_level_options = ["All"] + sorted(filtered_df[second_level_col].dropna().unique().tolist())
        if second_level_options:
            selected_second_level = st.sidebar.selectbox(second_level_label, second_level_options, index=0)
            if selected_second_level != "All":
                filtered_df = filtered_df[filtered_df[second_level_col] == selected_second_level]
    elif selected_page_type == "FAQ/Help":
        if 'faq_question' in filtered_df.columns and filtered_df['faq_question'].notna().any():
            second_level_label = "FAQ Question"
            second_level_col = "faq_question"
            second_level_options = ["All"] + sorted(filtered_df[second_level_col].dropna().unique().tolist())
        elif 'faq_topic' in filtered_df.columns and filtered_df['faq_topic'].notna().any():
            second_level_label = "FAQ Topic"
            second_level_col = "faq_topic"
            second_level_options = ["All"] + sorted(filtered_df[second_level_col].dropna().unique().tolist())
        elif 'faq_section' in filtered_df.columns and filtered_df['faq_section'].notna().any():
            second_level_label = "FAQ Section"
            second_level_col = "faq_section"
            second_level_options = ["All"] + sorted(filtered_df[second_level_col].dropna().unique().tolist())
        if second_level_options:
            selected_second_level = st.sidebar.selectbox(second_level_label, second_level_options, index=0)
            if selected_second_level != "All":
                filtered_df = filtered_df[filtered_df[second_level_col] == selected_second_level]

    # Display raw data table
    st.write(f"**Filtered Data:** {len(filtered_df):,} rows")
    display_columns = [
        'event_date', 'event_name', 'user_pseudo_id', 'page_type', 
        'page_title', 'page_location', 'device_category', 'geo_country',
        'click_text', 'click_element', 'engagement_time_msec', 'section', 'page_referrer', 'click_url',
        'ecomm_prodid', 'item_id', 'faq_question', 'faq_topic', 'faq_section', 'faq_id'
    ]
    # Always include the second-level column if it's being used and not already present
    if second_level_col and second_level_col not in display_columns:
        display_columns.append(second_level_col)
    available_columns = [col for col in display_columns if col in filtered_df.columns]
    st.dataframe(filtered_df[available_columns].head(500), use_container_width=True)

    # --- Add summary table below raw data ---
    summary_col = None
    summary_label = None

    if selected_page_type == "FAQ/Help":
        if 'faq_question' in filtered_df.columns and filtered_df['faq_question'].notna().any():
            summary_col = 'faq_question'
            summary_label = 'FAQ Question'
        elif 'faq_topic' in filtered_df.columns and filtered_df['faq_topic'].notna().any():
            summary_col = 'faq_topic'
            summary_label = 'FAQ Topic'
        elif 'faq_section' in filtered_df.columns and filtered_df['faq_section'].notna().any():
            summary_col = 'faq_section'
            summary_label = 'FAQ Section'
    elif selected_page_type == "Product Pages":
        if 'page_title' in filtered_df.columns and filtered_df['page_title'].notna().any():
            summary_col = 'page_title'
            summary_label = 'Product Title'
        elif 'ecomm_prodid' in filtered_df.columns and filtered_df['ecomm_prodid'].notna().any():
            summary_col = 'ecomm_prodid'
            summary_label = 'Product ID'
        elif 'item_id' in filtered_df.columns and filtered_df['item_id'].notna().any():
            summary_col = 'item_id'
            summary_label = 'Item ID'
    else:
        if 'faq_question' in filtered_df.columns and filtered_df['faq_question'].notna().any():
            summary_col = 'faq_question'
            summary_label = 'FAQ Question'
        elif 'ecomm_prodid' in filtered_df.columns and filtered_df['ecomm_prodid'].notna().any():
            summary_col = 'ecomm_prodid'
            summary_label = 'Product ID'
        elif 'item_id' in filtered_df.columns and filtered_df['item_id'].notna().any():
            summary_col = 'item_id'
            summary_label = 'Item ID'
        elif 'event_name' in filtered_df.columns and filtered_df['event_name'].notna().any():
            summary_col = 'event_name'
            summary_label = 'Event Name'
        elif 'page_title' in filtered_df.columns and filtered_df['page_title'].notna().any():
            summary_col = 'page_title'
            summary_label = 'Page Title'

    # Find all event parameter columns (those parsed from event_params_json)
    event_param_candidates = [
        'faq_question', 'faq_topic', 'faq_section', 'faq_id', 'page_title', 'page_location',
        'interaction_type', 'page_path', 'click_element', 'click_text', 'click_url',
        'ecomm_prodid', 'item_id', 'section', 'engagement_time_msec'
    ]
    event_param_cols = [
        col for col in event_param_candidates
        if col in filtered_df.columns and filtered_df[col].notna().any()
    ]

    second_summary_col = None
    second_summary_label = None

    if event_param_cols:
        # Add "All" option to prevent defaulting to first item
        # This selectbox is for the frequency table
        _second_level_options_freq = ["All"] + event_param_cols
        _selected_second_level_freq = st.sidebar.selectbox(
            "Second-level split (event parameter)",
            _second_level_options_freq,
            format_func=lambda x: x.replace('_', ' ').title() if x != "All" else x,
            index=0  # Default to "All"
        )
        if _selected_second_level_freq != "All":
            second_summary_col = _selected_second_level_freq
            second_summary_label = second_summary_col.replace('_', ' ').title()

    # Show summary table if possible
    if summary_col and second_summary_col:
        st.markdown(f"### Frequency by {summary_label} and {second_summary_label}")
        try:
            # Use a simpler approach to avoid column naming conflicts
            counts = filtered_df.value_counts([summary_col, second_summary_col])
            freq_table = pd.DataFrame({
                summary_col: counts.index.get_level_values(0),
                second_summary_col: counts.index.get_level_values(1),
                'Count': counts.values
            }).sort_values('Count', ascending=False)
        except Exception:
            freq_table = (
                filtered_df
                .pivot_table(index=[summary_col, second_summary_col], aggfunc='size', fill_value=0)
                .to_frame('Count')
                .reset_index()
                .sort_values('Count', ascending=False)
            )
        st.dataframe(freq_table, use_container_width=True)
    elif summary_col:
        st.markdown(f"### Frequency by {summary_label}")
        try:
            # Fix the column naming conflict by using a different approach
            value_counts_series = filtered_df[summary_col].value_counts()
            freq_table = pd.DataFrame({
                'Item': value_counts_series.index,
                'Count': value_counts_series.values
            }).sort_values('Count', ascending=False)
            # Rename the first column to match the summary_col
            freq_table.columns = [summary_label, 'Count']
        except Exception:
            freq_table = (
                filtered_df[summary_col]
                .value_counts()
                .to_frame('Count')
                .reset_index()
                .sort_values('Count', ascending=False)
            )
        st.dataframe(freq_table, use_container_width=True)
