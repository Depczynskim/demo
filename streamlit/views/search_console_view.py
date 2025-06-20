import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from datetime import datetime, timedelta
from utils import get_filtered_date_range, get_comparison_periods

def get_filtered_drivers(current_data, previous_data, metric_focus, ascending=False):
    """Get filtered and sorted driver data"""
    # Calculate changes
    changes = pd.DataFrame({
        'clicks_change': current_data.groupby('page')['clicks'].sum() - previous_data.groupby('page')['clicks'].sum(),
        'impressions_change': current_data.groupby('page')['impressions'].sum() - previous_data.groupby('page')['impressions'].sum(),
        'position_change': previous_data.groupby('page')['position'].mean() - current_data.groupby('page')['position'].mean()
    }).reset_index()
    
    # Add impact score
    changes['impact'] = changes['position_change'].abs() * np.log1p(changes['impressions_change'].abs())
    
    # Sort by selected metric
    metric_map = {
        'Position': 'position_change',
        'Clicks': 'clicks_change',
        'Impressions': 'impressions_change',
        'Impact': 'impact'
    }
    sort_by = metric_map.get(metric_focus, 'impact')
    changes = changes.sort_values(sort_by, ascending=ascending)
    
    return changes

def display_enhanced_drivers(df, metric_focus):
    """Display enhanced driver analysis with progress bars"""
    if df.empty:
        st.info("No significant changes found.")
        return
        
    # Map display names to column names
    metric_map = {
        'Position': 'position_change',
        'Clicks': 'clicks_change',
        'Impressions': 'impressions_change',
        'CTR': 'impact',  # Use impact score for CTR
        'Impact': 'impact'
    }
    
    # Get the correct column name, defaulting to impact
    column = metric_map.get(metric_focus, 'impact')
    if column not in df.columns:
        st.warning(f"Metric '{metric_focus}' not found in data. Showing impact score instead.")
        column = 'impact'
    
    # Display each change with a progress bar
    for _, row in df.iterrows():
        value = row[column]
        if pd.isna(value) or value == 0:
            continue
            
        col1, col2 = st.columns([3, 7])
        with col1:
            st.write(f"`{row['page']}`")
        with col2:
            # Calculate progress percentage (normalized to max value)
            max_value = max(abs(df[column]))
            if max_value > 0:  # Avoid division by zero
                progress = abs(value) / max_value
            else:
                progress = 0
            st.progress(progress)
            st.write(f"{value:+.2f}")

def render(context):
    """
    Renders a clear, action-oriented view of Search Console data, focusing on
    understanding performance changes.
    """
    unfiltered_df = context['df']
    
    if 'date' not in unfiltered_df.columns:
        st.warning("Dataframe must contain a 'date' column.")
        return

    # Use new date handling utility
    df, date_info = get_filtered_date_range(unfiltered_df, context)

    if df.empty:
        st.warning("No data available for the selected date range.")
        return

    # --- Header and Global Filters ---
    st.title("🔍 Search Console Analysis")

    with st.expander("ℹ️ About this dashboard", expanded=False):
        st.markdown(
            """
            This view shows how your website performs in Google Search. Use it to see which search queries bring users 
            to your site, which pages are most popular, and how your search ranking changes over time.

            **What you can do here:**
            
            *   **Compare Time Periods:** See if your search traffic is growing or declining.
            *   **Filter by Location & Device:** Understand how performance differs across countries and on mobile vs. desktop.
            *   **Analyze Specific Pages & Queries:** Drill down to see the exact search terms that lead to a specific page.
            """
        )

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

        # --- Global Filters ---
        col1, col2 = st.columns(2)
        with col1:
            countries = ['All Countries'] + sorted(unfiltered_df['country'].unique())
            selected_country = st.selectbox("Select Country", options=countries)

        with col2:
            devices = ['All Devices'] + sorted(unfiltered_df['device'].unique())
            selected_device = st.selectbox("Select Device", options=devices)

        # --- Filter Data ---
        # Create a dataframe for comparison that includes historical data, up to the end of the selected period.
        comparison_df = unfiltered_df[unfiltered_df['date'] <= end_date].copy()
        if selected_country != 'All Countries':
            comparison_df = comparison_df[comparison_df['country'] == selected_country]
        if selected_device != 'All Devices':
            comparison_df = comparison_df[comparison_df['device'] == selected_device]

        # Create a dataframe for display that is strictly filtered to the selected date range.
        view_df = df.copy()
        if selected_country != 'All Countries':
            view_df = view_df[view_df['country'] == selected_country]
        if selected_device != 'All Devices':
            view_df = view_df[view_df['device'] == selected_device]

        # Use available_days as the initial comparison period, but auto-reduce if needed
        min_comparison_days = 1
        if available_days < min_comparison_days:
            st.warning(
                f"Comparisons require at least {min_comparison_days} days in your selected range. "
                f"Please select a longer date range."
            )
            return
        comparison_found = False
        period_info = {}  # Ensure this is always defined
        for days in range(available_days, min_comparison_days - 1, -1):
            current_data, previous_data, period_info = get_comparison_periods(comparison_df, days)
            if not previous_data.empty:
                if days < available_days:
                    st.info(f"Not enough data for a {available_days}-day comparison. Showing {days}-day comparison (max possible for both periods).")
                days_to_compare = days
                comparison_found = True
                break
        if not comparison_found:
            error_type = period_info.get('error', 'unknown')
            if error_type == 'insufficient_history':
                max_days = period_info.get('max_comparison_days', 0)
                total_days = period_info.get('total_available_days', 0)
                st.warning(
                    f"Not enough historical data for a {available_days}-day comparison. "
                    "This usually means there isn't enough data before your selected date range. "
                    f"Try expanding your date range.\n\n"
                    "### Available Data Range\n"
                    f"Earliest date: {unfiltered_df['date'].min().strftime('%Y-%m-%d')}\n\n"
                    f"Latest date: {unfiltered_df['date'].max().strftime('%Y-%m-%d')}\n\n"
                    f"Total days: {total_days}\n\n"
                    f"Maximum comparison period possible: {max_days} days"
                )
            elif error_type == 'incomplete_data':
                current_days = period_info.get('current_days', 0)
                previous_days = period_info.get('previous_days', 0)
                expected_days = period_info.get('expected_days', 0)
                min_required = period_info.get('min_required_days', min_comparison_days)
                st.warning(
                    f"Incomplete data for {available_days}-day comparison.\n\n"
                    f"Current period has {current_days} days of data (need at least {min_required}).\n"
                    f"Previous period has {previous_days} days of data (need at least {min_required}).\n\n"
                    "Try expanding your date range."
                )
            else:
                st.warning(
                    f"Unable to perform {available_days}-day comparison. "
                    "Please check your date range selection."
                )
            return
        # --- Tabbed Layout ---
        # Custom CSS to make tabs more prominent
        st.markdown("""
            <style>
                div[data-testid="stTabs"] div[role="tablist"] {
                    border: 2px solid #EAEBEF;
                    border-radius: 10px;
                    padding: 5px;
                    gap: 10px;
                    justify-content: center;
                }
                div[data-testid="stTabs"] button[role="tab"] {
                    flex-grow: 1;
                    text-align: center;
                    font-size: 1.1rem;
                    font-weight: 600;
                    border-radius: 8px !important;
                    border: none !important;
                    padding: 10px 0;
                    transition: background-color 0.2s ease-in-out;
                }
                div[data-testid="stTabs"] button[role="tab"]:hover {
                    background-color: #F0F2F6;
                }
                div[data-testid="stTabs"] button[aria-selected="true"] {
                    background-color: #FFFFFF;
                    color: #1E2022;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                }
            </style>
        """, unsafe_allow_html=True)
        tab1, tab2, tab3, tab4 = st.tabs(["📊 Performance Overview", "🚀 Top Movers", "🔍 Page Deep-Dive", "💬 Query Deep-Dive"])
        with tab1:
            render_overview(current_data, previous_data)
        with tab2:
            render_top_movers(current_data, previous_data)
        with tab3:
            render_deep_dive(view_df, current_data, previous_data)
        with tab4:
            render_query_deep_dive(view_df, current_data, previous_data)
    else:
        st.warning("No date range available. Please check your data contains valid dates.")

def get_comparison_metrics(current_data, previous_data, group_by_col):
    """Helper function to calculate and compare metrics between two periods."""
    current_agg = current_data.groupby(group_by_col).agg(
        clicks_current=('clicks', 'sum'),
        impressions_current=('impressions', 'sum'),
        position_current=('position', 'mean')
    ).reset_index()

    previous_agg = previous_data.groupby(group_by_col).agg(
        clicks_previous=('clicks', 'sum'),
        impressions_previous=('impressions', 'sum'),
        position_previous=('position', 'mean')
    ).reset_index()

    merged = pd.merge(current_agg, previous_agg, on=group_by_col, how='outer')

    # Fill NaNs for metrics that should be 0, but leave positions as NaN to avoid confusion
    fill_zeros = {
        'clicks_current': 0, 'impressions_current': 0,
        'clicks_previous': 0, 'impressions_previous': 0,
    }
    merged = merged.fillna(value=fill_zeros)

    # Positive change is better rank (lower number)
    merged['position_change'] = merged['position_previous'] - merged['position_current']
    merged['clicks_change'] = merged['clicks_current'] - merged['clicks_previous']
    merged['impressions_change'] = merged['impressions_current'] - merged['impressions_previous']
    
    # Calculate an "impact" score to find most meaningful changes
    # We use log of impressions to avoid massive numbers and normalize the scale
    merged['impact'] = merged['position_change'].abs() * (merged['impressions_current'] + merged['impressions_previous']).apply(lambda x: np.log1p(x))

    return merged

def render_changes_table(df, type='page'):
    """Helper function to render tables for top gaining/losing pages or queries."""
    if df.empty:
        st.info(f"No significant {type} changes to display.")
        return [] if type == 'page' else None

    display_data = []
    full_urls = [] if type == 'page' else None

    for _, row in df.iterrows():
        # Create arrow based on position change
        if row['position_change'] > 0:
            arrow = "⬆️"
        elif row['position_change'] < 0:
            arrow = "⬇️"
        else:
            arrow = "➡️"
        
        name_col = row[type]
        display_name = name_col

        if type == 'page':
            full_urls.append(name_col)
            full_url = name_col
            if '/' in full_url:
                # Get the last meaningful part of the URL
                url_parts = [part for part in full_url.split('/') if part]
                if url_parts:
                    page_name = url_parts[-1]
                    # If it's just a file extension or empty, try the second to last part
                    if not page_name or page_name in ['index.html', 'index.php', '']:
                        page_name = url_parts[-2] if len(url_parts) > 1 else full_url
                else:
                    page_name = full_url
            else:
                page_name = full_url
            
            # Clean up common URL patterns
            if page_name.endswith('.html'):
                page_name = page_name[:-5]
            if page_name.endswith('.php'):
                page_name = page_name[:-4]
            
            # Limit page name length for display
            display_name = page_name[:30] + '...' if len(page_name) > 30 else page_name
            if not display_name.strip():
                display_name = "Homepage"
        else: # query
            display_name = name_col[:40] + '...' if len(name_col) > 40 else name_col
        
        display_data.append({
            type.capitalize(): display_name,
            'Change': f"{arrow} {row['position_change']:+.1f}",
            'Current': f"{row['position_current']:.1f}" if not pd.isna(row['position_current']) else "N/A",
            'Previous': f"{row['position_previous']:.1f}" if not pd.isna(row['position_previous']) else "N/A"
        })
    
    # Display the main table
    if display_data:
        df_display = pd.DataFrame(display_data)
        st.dataframe(df_display, use_container_width=True, hide_index=True)
        
    if type == 'page':
        return [{'Page': d['Page'], 'url': url} for d, url in zip(display_data, full_urls)]
    
    return None

def render_overview(current_data, previous_data):
    """Renders the high-level performance overview."""
    st.header("Performance Snapshot")
    
    # --- Key Metrics ---
    clicks_current = current_data['clicks'].sum()
    impressions_current = current_data['impressions'].sum()
    position_current = current_data['position'].mean()
    ctr_current = (clicks_current / impressions_current * 100) if impressions_current > 0 else 0

    clicks_previous = previous_data['clicks'].sum()
    impressions_previous = previous_data['impressions'].sum()
    position_previous = previous_data['position'].mean()
    ctr_previous = (clicks_previous / impressions_previous * 100) if impressions_previous > 0 else 0
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(
            label="Total Clicks",
            value=f"{clicks_current:,.0f}",
            delta=f"{clicks_current - clicks_previous:,.0f}",
        )
    with col2:
        st.metric(
            label="Total Impressions",
            value=f"{impressions_current:,.0f}",
            delta=f"{impressions_current - impressions_previous:,.0f}",
        )
    with col3:
        st.metric(
            label="Average CTR",
            value=f"{ctr_current:.2f}%",
            delta=f"{ctr_current - ctr_previous:.2f}%",
        )
    with col4:
        # For position, a negative delta is an improvement
        st.metric(
            label="Average Position",
            value=f"{position_current:.1f}",
            delta=f"{position_current - position_previous:.1f}",
            delta_color="inverse"
        )
    
    st.markdown("---")
    
    # --- Interactive Time Series ---
    st.header("Performance Trends")
    
    daily_summary = current_data.groupby('date').agg(
        Position=('position', 'mean'),
        Clicks=('clicks', 'sum'),
        Impressions=('impressions', 'sum')
    ).reset_index()
    
    # Add CTR calculation
    daily_summary['CTR'] = (daily_summary['Clicks'] / daily_summary['Impressions'] * 100).fillna(0)
    
    # Metric selector above the chart
    selected_metric = st.selectbox(
        "Select Metric to Display",
        options=['Clicks', 'Impressions', 'Position', 'CTR'],
        index=0
    )
    
    # Create chart based on selected metric
    if selected_metric == 'Position':
        fig = px.line(daily_summary, x='date', y='Position', 
                     title=f'Daily {selected_metric} Trend', markers=True,
                     color_discrete_sequence=['#ff6b6b'])
        fig.update_yaxes(autorange="reversed")  # Lower position is better
    elif selected_metric == 'CTR':
        fig = px.line(daily_summary, x='date', y='CTR', 
                     title=f'Daily {selected_metric} Trend (%)', markers=True,
                     color_discrete_sequence=['#4ecdc4'])
    else:
        fig = px.line(daily_summary, x='date', y=selected_metric, 
                     title=f'Daily {selected_metric} Trend', markers=True,
                     color_discrete_sequence=['#45b7d1'])
    
    fig.update_layout(height=350, showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    
    # --- Key Changes Split into Pages and Queries ---
    st.header("What's Driving the Changes?")
    st.markdown("The pages and queries with the most significant impact on your performance changes.")
    st.info("💡 **Driver Analysis**: Rankings are based on a composite 'impact' score that considers both position changes and impression volume to identify the most meaningful shifts in your search performance.")
    
    page_changes = get_comparison_metrics(current_data, previous_data, 'page')
    query_changes = get_comparison_metrics(current_data, previous_data, 'query')
    
    # Split into gaining and declining
    gaining_pages = page_changes[page_changes['position_change'] > 0].sort_values('impact', ascending=False).head(7)
    declining_pages = page_changes[page_changes['position_change'] < 0].sort_values('impact', ascending=False).head(7)
    
    gaining_queries = query_changes[query_changes['position_change'] > 0].sort_values('impact', ascending=False).head(7)
    declining_queries = query_changes[query_changes['position_change'] < 0].sort_values('impact', ascending=False).head(7)
    
    # Create two columns for side-by-side tables
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🚀 Top Gaining Pages")
        gaining_pages_urls = render_changes_table(gaining_pages, 'page')
        st.markdown("---")
        st.subheader("📉 Top Declining Pages")
        declining_pages_urls = render_changes_table(declining_pages, 'page')

    with col2:
        st.subheader("🚀 Top Gaining Queries")
        render_changes_table(gaining_queries, 'query')
        st.markdown("---")
        st.subheader("📉 Top Declining Queries")
        render_changes_table(declining_queries, 'query')
        
    # Combine and display URLs below the tables
    all_page_urls = gaining_pages_urls + declining_pages_urls
    if all_page_urls:
        # Remove duplicates based on url, preserving order
        seen_urls = set()
        unique_urls = []
        for item in all_page_urls:
            if item['url'] not in seen_urls:
                seen_urls.add(item['url'])
                unique_urls.append(item)

        if unique_urls:
            with st.expander("📋 Copy Full Page URLs", expanded=False):
                for i, item in enumerate(unique_urls):
                    st.text(f"{i+1}. {item['Page']}")
                    st.code(item['url'], language=None)
                    if i < len(unique_urls) - 1:
                        st.divider()

def render_top_movers(current_data, previous_data):
    """Renders the tables for top gaining and losing pages/queries."""
    st.header("Top Performance Changes")
    st.info("Highlights the pages and queries with the most significant changes, based on a combination of rank change and impression volume.")

    # --- Page Movers ---
    st.subheader("🚀 Page Movers")
    page_changes = get_comparison_metrics(current_data, previous_data, 'page')
    page_changes = page_changes.sort_values('impact', ascending=False).head(20)

    st.dataframe(page_changes[[
        'page', 'position_change', 'position_current', 'position_previous', 
        'clicks_change', 'impressions_change'
    ]].style.format({
        'position_change': '{:+.1f}',
        'position_current': '{:.1f}',
        'position_previous': '{:.1f}',
        'clicks_change': '{:+,}',
        'impressions_change': '{:+,}',
    }, na_rep="N/A").bar(subset=['position_change'], align='mid', color=['#d65f5f', '#5fba7d']),
    use_container_width=True)
    
    # --- Query Movers ---
    st.subheader("💬 Query Movers")
    query_changes = get_comparison_metrics(current_data, previous_data, 'query')
    query_changes = query_changes.sort_values('impact', ascending=False).head(20)
    
    st.dataframe(query_changes[[
        'query', 'position_change', 'position_current', 'position_previous', 
        'clicks_change', 'impressions_change'
    ]].style.format({
        'position_change': '{:+.1f}',
        'position_current': '{:.1f}',
        'position_previous': '{:.1f}',
        'clicks_change': '{:+,}',
        'impressions_change': '{:+,}',
    }, na_rep="N/A").bar(subset=['position_change'], align='mid', color=['#d65f5f', '#5fba7d']),
    use_container_width=True)

def render_deep_dive(full_df, current_data, previous_data):
    """Renders the interactive deep-dive section."""
    st.header("Investigate a Page")
    
    top_pages = current_data.groupby('page')['impressions'].sum().nlargest(50).index.tolist()
    selected_page = st.selectbox(
        "Select a page to analyze",
        options=top_pages,
        help="Pages are pre-filtered to the top 50 by impressions in the current period."
    )

    if not selected_page:
        st.info("Select a page from the dropdown to see its detailed performance.")
        return

    page_df = full_df[full_df['page'] == selected_page]

    # --- Historical Performance for Selected Page ---
    st.subheader(f"Historical Performance for: `{selected_page}`")
    
    daily_page_summary = page_df.groupby('date').agg(
        Position=('position', 'mean'),
        Impressions=('impressions', 'sum'),
        Clicks=('clicks', 'sum')
    ).reset_index()
    
    fig = px.line(daily_page_summary, x='date', y=['Position', 'Clicks', 'Impressions'],
                  title="Daily Performance Trend", markers=True)
    fig.update_yaxes(rangemode='tozero')
    fig.update_yaxes(autorange="reversed", selector=dict(title_text="Position"))
    st.plotly_chart(fig, use_container_width=True)

    # --- Query Changes for Selected Page ---
    st.subheader("Top Query Changes on This Page")
    
    page_current_data = current_data[current_data['page'] == selected_page]
    page_previous_data = previous_data[previous_data['page'] == selected_page]

    if not page_previous_data.empty:
        query_changes_on_page = get_comparison_metrics(page_current_data, page_previous_data, 'query')
        query_changes_on_page = query_changes_on_page.sort_values('impact', ascending=False)

        st.dataframe(query_changes_on_page[[
            'query', 'position_change', 'position_current', 'position_previous',
            'clicks_change', 'impressions_change'
        ]].style.format({
            'position_change': '{:+.1f}',
            'position_current': '{:.1f}',
            'position_previous': '{:.1f}',
            'clicks_change': '{:+,}',
            'impressions_change': '{:+,}',
        }, na_rep="N/A"), use_container_width=True)
    else:
        st.warning("Not enough historical data for this specific page to perform a comparison.")

def render_query_deep_dive(full_df, current_data, previous_data):
    """Renders the interactive query deep-dive section."""
    st.header("Investigate a Query")
    
    top_queries = current_data.groupby('query')['impressions'].sum().nlargest(50).index.tolist()
    selected_query = st.selectbox(
        "Select a query to analyze",
        options=top_queries,
        help="Queries are pre-filtered to the top 50 by impressions in the current period.",
        key="query_deep_dive_selectbox"
    )

    if not selected_query:
        st.info("Select a query from the dropdown to see its detailed performance.")
        return

    query_df = full_df[full_df['query'] == selected_query]

    # --- Historical Performance for Selected Query ---
    st.subheader(f"Historical Performance for: `{selected_query}`")
    
    daily_query_summary = query_df.groupby('date').agg(
        Position=('position', 'mean'),
        Impressions=('impressions', 'sum'),
        Clicks=('clicks', 'sum')
    ).reset_index()

    if daily_query_summary.empty:
        st.warning("No data available for this query in the selected date range.")
    else:
        daily_query_summary['CTR'] = (daily_query_summary['Clicks'] / daily_query_summary['Impressions'] * 100).fillna(0)
    
        # Metric selector for the chart
        selected_metric = st.selectbox(
            "Select Metric to Display",
            options=['Clicks', 'Impressions', 'Position', 'CTR'],
            index=0,
            key="query_deep_dive_metric_selector"
        )
    
        # Create chart based on selected metric
        if selected_metric == 'Position':
            fig = px.line(daily_query_summary, x='date', y='Position', 
                         title=f'Daily {selected_metric} Trend', markers=True,
                         color_discrete_sequence=['#ff6b6b'])
            fig.update_yaxes(autorange="reversed")
        elif selected_metric == 'CTR':
            fig = px.line(daily_query_summary, x='date', y='CTR', 
                         title=f'Daily {selected_metric} Trend (%)', markers=True,
                         color_discrete_sequence=['#4ecdc4'])
        else:
            fig = px.line(daily_query_summary, x='date', y=selected_metric, 
                         title=f'Daily {selected_metric} Trend', markers=True,
                         color_discrete_sequence=['#45b7d1'])
    
        fig.update_layout(height=350, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    # --- Page Changes for Selected Query ---
    st.subheader("Top Page Changes for This Query")
    
    query_current_data = current_data[current_data['query'] == selected_query]
    query_previous_data = previous_data[previous_data['query'] == selected_query]

    if not query_previous_data.empty and not query_current_data.empty:
        page_changes_on_query = get_comparison_metrics(query_current_data, query_previous_data, 'page')
        page_changes_on_query = page_changes_on_query.sort_values('impact', ascending=False)

        st.dataframe(page_changes_on_query[[
            'page', 'position_change', 'position_current', 'position_previous',
            'clicks_change', 'impressions_change'
        ]].style.format({
            'position_change': '{:+.1f}',
            'position_current': '{:.1f}',
            'position_previous': '{:.1f}',
            'clicks_change': '{:+,}',
            'impressions_change': '{:+,}',
        }, na_rep="N/A"), use_container_width=True)
    else:
        st.warning("Not enough historical data for this specific query to perform a comparison.") 