import streamlit as st
import pandas as pd
import numpy as np
import pydeck as pdk
import matplotlib.pyplot as plt
import pytz
from datetime import datetime
import json
from utils import get_filtered_date_range

def _display_filters(df_parsed):
    """Displays product and country filters and returns the filtered dataframe and selections."""
    st.title("🛍️ Product Performance Analysis")

    # Force a redeploy on Streamlit Cloud
    with st.expander("ℹ️ About this dashboard", expanded=False):
        st.markdown(
            """
            This view provides a detailed look at how individual products are performing. Use it to understand 
            customer engagement and identify which products are most popular.

            **What you can explore here:**
            
            *   **Traffic Sources:** See which channels (e.g., Google Search, Ads, direct visits) are driving traffic to specific product pages.
            *   **Geographic Interest:** Discover which countries show the most interest in each product.
            *   **User Behavior:** Analyze how users interact with product pages, including clicks and other events.
            """
        )
    
    st.markdown("## Product Analysis Filters")
    
    # --- Product filter ---
    product_pages_df = df_parsed[df_parsed['page_type'] == 'Product Pages']
    id_title_map = {}
    filter_by_id = False

    if 'ecomm_prodid' in product_pages_df.columns and 'page_title' in product_pages_df.columns:
        id_title_pairs = product_pages_df[['ecomm_prodid', 'page_title']].dropna().drop_duplicates(subset=['ecomm_prodid'])
        if not id_title_pairs.empty:
            id_title_pairs['clean_title'] = id_title_pairs['page_title'].str.replace("POPS: Buy From Makers - ", "").str.strip()
            id_title_map = pd.Series(id_title_pairs.ecomm_prodid.values, index=id_title_pairs.clean_title).to_dict()

    if id_title_map:
        product_options = sorted(id_title_map.keys())
        product_col_for_filtering = 'ecomm_prodid'
        filter_by_id = True
    elif 'page_title' in product_pages_df.columns and product_pages_df['page_title'].notna().any():
        product_options = sorted(product_pages_df['page_title'].dropna().unique().tolist())
        product_col_for_filtering = 'page_title'
    elif 'page_title' in df_parsed.columns and df_parsed['page_title'].notna().any():
        st.warning("Could not identify specific 'Product Pages'. Displaying all available page titles.")
        product_options = sorted(df_parsed['page_title'].dropna().unique().tolist())
        product_col_for_filtering = 'page_title'
    else:
        product_options = []

    if product_options:
        products = ["All Products"] + product_options
        selected_product = st.selectbox(
            "Select Product:", products, index=0,
            help="Choose a specific product to filter all analysis below"
        )
        if selected_product != "All Products":
            if filter_by_id:
                product_id_to_filter = id_title_map[selected_product]
                filtered_df = df_parsed[df_parsed[product_col_for_filtering] == product_id_to_filter]
            else:
                filtered_df = df_parsed[df_parsed[product_col_for_filtering] == selected_product]
            st.write(f"**Filtered to product:** `{selected_product}`")
        else:
            filtered_df = df_parsed.copy()
            st.write(f"**Showing:** All products")
    else:
        st.warning("No product data found to populate the filter.")
        filtered_df = df_parsed.copy()
        selected_product = "All Products"

    st.write(f"**Rows:** {len(filtered_df):,}")

    if filtered_df.empty:
        return filtered_df, selected_product

    # --- Country filter ---
    country_options = ['All Countries'] + sorted(filtered_df['geo_country'].dropna().unique().tolist())
    selected_country = st.selectbox(
        "Select Country:", country_options, index=0,
        help="Choose a specific country to filter all analysis below"
    )
    if selected_country != 'All Countries':
        filtered_df = filtered_df[filtered_df['geo_country'] == selected_country]
        st.write(f"**Filtered to country:** `{selected_country}`")
    else:
        st.write(f"**Showing:** All countries")
    
    st.write(f"**Rows after country filter:** {len(filtered_df):,}")

    return filtered_df, selected_product


def _display_traffic_source_analysis(df):
    """Displays traffic source breakdown table and chart."""
    st.markdown("### 🌐 Traffic Source & Medium Breakdown")
    traffic_df = df.copy()

    def extract_from_json_col(col, source_key='source', medium_key='medium'):
        sources, mediums = [], []
        for val in col:
            try:
                if pd.isna(val):
                    sources.append(None); mediums.append(None); continue
                d = json.loads(val)
                for camp_key in ['cross_channel_campaign', 'manual_campaign']:
                    if camp_key in d and d[camp_key]:
                        sources.append(d[camp_key].get(source_key))
                        mediums.append(d[camp_key].get(medium_key))
                        break
                else:
                    sources.append(None); mediums.append(None)
            except Exception:
                sources.append(None); mediums.append(None)
        return pd.DataFrame({'source': sources, 'medium': mediums})

    traffic_pairs = []
    if 'traffic_source' in traffic_df.columns and 'traffic_medium' in traffic_df.columns:
        traffic_pairs.append(traffic_df[['traffic_source', 'traffic_medium']].rename(columns={'traffic_source': 'source', 'traffic_medium': 'medium'}))
    if 'collected_traffic_source_json' in traffic_df.columns:
        traffic_pairs.append(extract_from_json_col(traffic_df['collected_traffic_source_json']))
    if 'session_traffic_source_last_click_json' in traffic_df.columns:
        traffic_pairs.append(extract_from_json_col(traffic_df['session_traffic_source_last_click_json']))

    if traffic_pairs:
        all_traffic = pd.concat(traffic_pairs, ignore_index=True).dropna(subset=['source', 'medium'])
        if not all_traffic.empty:
            traffic_counts = all_traffic.groupby(['source', 'medium']).size().reset_index(name='Event Count').sort_values('Event Count', ascending=False)
            st.dataframe(traffic_counts, use_container_width=True)

            st.markdown("#### Top 15 Traffic Sources by Event Count")
            chart_data = traffic_counts.head(15).copy()
            chart_data['source_medium'] = chart_data['source'] + ' / ' + chart_data['medium']
            
            fig, ax = plt.subplots(figsize=(10, 8))
            ax.barh(chart_data['source_medium'], chart_data['Event Count'], color='skyblue')
            ax.set_xlabel('Event Count')
            ax.set_title('Top 15 Traffic Sources/Mediums')
            ax.invert_yaxis()
            for index, value in enumerate(chart_data['Event Count']):
                ax.text(value, index, f' {value}')
            plt.tight_layout()
            st.pyplot(fig)
            plt.close()
        else:
            st.info("No traffic source/medium data available for the selected filters.")
    else:
        st.info("No traffic source/medium data available in this dataset.")


def _display_map_visualization(df):
    """Displays map of unique visitors with dynamic dot sizes."""
    st.markdown("### 📍 Unique Visitors by Location (Hover for Details)")
    map_df = df.dropna(subset=['user_pseudo_id', 'geo_country']).groupby(['geo_country', 'geo_city'])['user_pseudo_id'].nunique().reset_index(name='unique_visitors')

    city_coords = {
        ('United States', 'New York'): (40.7128, -74.0060), ('United States', 'Los Angeles'): (34.0522, -118.2437),
        ('United States', 'Chicago'): (41.8781, -87.6298), ('United States', 'Houston'): (29.7604, -95.3698),
        ('United States', 'Phoenix'): (33.4484, -112.0740), ('United States', 'Philadelphia'): (39.9526, -75.1652),
        ('United States', 'San Antonio'): (29.4241, -98.4936), ('United States', 'San Diego'): (32.7157, -117.1611),
        ('United States', 'Dallas'): (32.7767, -96.7970), ('United States', 'San Jose'): (37.3382, -121.8863),
        ('United States', 'Austin'): (30.2672, -97.7431), ('United States', 'Jacksonville'): (30.3322, -81.6557),
        ('United States', 'Fort Worth'): (32.7555, -97.3308), ('United States', 'Columbus'): (39.9612, -82.9988),
        ('United States', 'Charlotte'): (35.2271, -80.8431), ('United States', 'San Francisco'): (37.7749, -122.4194),
        ('United States', 'Indianapolis'): (39.7684, -86.1581), ('United States', 'Seattle'): (47.6062, -122.3321),
        ('United States', 'Denver'): (39.7392, -104.9903), ('United States', 'Washington'): (38.9072, -77.0369),
        ('United States', 'Boston'): (42.3601, -71.0589), ('United States', 'Nashville'): (36.1627, -86.7816),
        ('United States', 'Baltimore'): (39.2904, -76.6122), ('United States', 'Oklahoma City'): (35.4676, -97.5164),
        ('United States', 'Portland'): (45.5152, -122.6784), ('United States', 'Las Vegas'): (36.1699, -115.1398),
        ('United States', 'Louisville'): (38.2527, -85.7585), ('United States', 'Milwaukee'): (43.0389, -87.9065),
        ('United States', 'Albuquerque'): (35.0844, -106.6504), ('United States', 'Tucson'): (32.2226, -110.9747),
        ('United States', 'Fresno'): (36.7378, -119.7871), ('United States', 'Sacramento'): (38.5816, -121.4944),
        ('United States', 'Mesa'): (33.4152, -111.8315), ('United States', 'Kansas City'): (39.0997, -94.5786),
        ('United States', 'Atlanta'): (33.7490, -84.3880), ('United States', 'Miami'): (25.7617, -80.1918),
        ('United States', 'Raleigh'): (35.7796, -78.6382), ('United States', 'Omaha'): (41.2565, -95.9345),
        ('United States', 'Minneapolis'): (44.9778, -93.2650), ('United States', 'Tulsa'): (36.1540, -95.9928),
        ('United States', 'Cleveland'): (41.4993, -81.6944), ('United States', 'Wichita'): (37.6872, -97.3301),
        ('United States', 'Arlington'): (32.7357, -97.1081), ('United States', 'Boardman'): (45.8398, -119.7006),
        ('United Kingdom', 'London'): (51.5074, -0.1278), ('United Kingdom', 'Birmingham'): (52.4862, -1.8904),
        ('United Kingdom', 'Manchester'): (53.4808, -2.2426), ('United Kingdom', 'Glasgow'): (55.8642, -4.2518),
        ('United Kingdom', 'Liverpool'): (53.4084, -2.9916), ('United Kingdom', 'Leeds'): (53.8008, -1.5491),
        ('United Kingdom', 'Sheffield'): (53.3811, -1.4701), ('United Kingdom', 'Edinburgh'): (55.9533, -3.1883),
        ('United Kingdom', 'Bristol'): (51.4545, -2.5879), ('United Kingdom', 'Cardiff'): (51.4816, -3.1791),
        ('United Kingdom', 'Milton Keynes'): (52.0406, -0.7594),
        ('Australia', 'Sydney'): (-33.8688, 151.2093), ('Australia', 'Melbourne'): (-37.8136, 144.9631),
        ('Australia', 'Brisbane'): (-27.4698, 153.0251), ('Australia', 'Perth'): (-31.9505, 115.8605),
        ('Australia', 'Adelaide'): (-34.9285, 138.6007), ('Australia', 'Gold Coast'): (-28.0167, 153.4000),
        ('Australia', 'Newcastle'): (-32.9283, 151.7817), ('Australia', 'Canberra'): (-35.2809, 149.1300),
        ('Canada', 'Toronto'): (43.6532, -79.3832), ('Canada', 'Vancouver'): (49.2827, -123.1207),
        ('Canada', 'Montreal'): (45.5017, -73.5673), ('France', 'Paris'): (48.8566, 2.3522),
        ('Germany', 'Berlin'): (52.5200, 13.4050), ('Spain', 'Madrid'): (40.4168, -3.7038),
        ('Italy', 'Rome'): (41.9028, 12.4964), ('Netherlands', 'Amsterdam'): (52.3676, 4.9041),
    }
    country_coords = {
        'United States': (39.8283, -98.5795), 'United Kingdom': (55.3781, -3.4360), 'Australia': (-25.2744, 133.7751),
        'Canada': (56.1304, -106.3468), 'France': (46.2276, 2.2137), 'Germany': (51.1657, 10.4515),
        'Spain': (40.4637, -3.7492), 'Italy': (41.8719, 12.5674), 'Netherlands': (52.1326, 5.2913),
        'Poland': (51.9194, 19.1451), 'New Zealand': (-40.9006, 174.8860), 'Sweden': (60.1282, 18.6435),
        'Switzerland': (46.8182, 8.2275), 'Ireland': (53.4129, -8.2439), 'Greece': (39.0742, 21.8243),
    }

    lats, lons = [], []
    for _, row in map_df.iterrows():
        key = (row['geo_country'], row['geo_city'])
        if key in city_coords:
            lat, lon = city_coords[key]
        elif row['geo_country'] in country_coords:
            lat, lon = country_coords[row['geo_country']]
        else:
            lat, lon = np.nan, np.nan
        lats.append(lat); lons.append(lon)
    
    map_df['lat'] = lats; map_df['lon'] = lons
    map_df = map_df.dropna(subset=['lat', 'lon'])

    radius_scale = st.slider(
        "Adjust map dot size:", 
        min_value=500, max_value=50000, value=5000, step=500,
        help="Increase or decrease the size of the dots on the map, which scale with visitor count."
    )
    
    st.write(f"**Showing {len(map_df)} locations** (hover over dots for visitor counts)")
    
    if not map_df.empty:
        st.pydeck_chart(pdk.Deck(
            map_provider='carto',               # free basemap provider
            map_style='light',                  # carto style
            initial_view_state=pdk.ViewState(
                latitude=map_df['lat'].mean(), longitude=map_df['lon'].mean(), zoom=2, pitch=0
            ),
            layers=[
                pdk.Layer(
                    'ScatterplotLayer', data=map_df, get_position='[lon, lat]',
                    get_radius='unique_visitors', radius_scale=radius_scale,
                    radius_min_pixels=3, radius_max_pixels=100,
                    get_fill_color='[0, 100, 200, 180]', get_line_color='[255, 255, 255, 255]',
                    line_width_min_pixels=2, pickable=True, auto_highlight=True,
                ),
            ],
            tooltip={
                "html": "<b>{geo_country}</b><br/>City: {geo_city}<br/>Unique Visitors: <b>{unique_visitors}</b>",
                "style": { "backgroundColor": "steelblue", "color": "white", "fontSize": "14px", "padding": "10px", "borderRadius": "5px" }
            }
        ))
        
        st.markdown("### Top 10 Locations by Unique Visitors")
        top_locations = map_df.nlargest(10, 'unique_visitors')[['geo_country', 'geo_city', 'unique_visitors']]
        st.dataframe(top_locations, use_container_width=True)
    else:
        st.warning("No geographic data available for mapping.")


def _display_timing_analysis(df, selected_product):
    """Displays day-of-week and hour-of-day analysis."""
    st.markdown("### Filter by Country for Timing Analysis")
    top_countries = df.dropna(subset=['user_pseudo_id', 'geo_country']).groupby('geo_country')['user_pseudo_id'].nunique().sort_values(ascending=False).head(5).index.tolist()
    available_countries = ['All Countries'] + top_countries
    
    selected_country_for_timing = st.selectbox(
        "Choose country to filter timing patterns:", available_countries, index=0,
        help="Select a specific country to see timing patterns for those visitors. All times are shown in London time."
    )
    
    if selected_country_for_timing == 'All Countries':
        analysis_data = df
        country_label = 'All Countries'
    else:
        analysis_data = df[df['geo_country'] == selected_country_for_timing]
        country_label = selected_country_for_timing
    
    timezone_str = 'Europe/London'
    
    if 'event_timestamp' in analysis_data.columns and not analysis_data.empty:
        times_utc = pd.to_datetime(analysis_data['event_timestamp'] // 1000_000, unit='s', utc=True)
        times_london = times_utc.dt.tz_convert(timezone_str)
        
        st.markdown("### 📅 Day of Week Analysis (London Time)")
        product_context = f" for {selected_product}" if selected_product != "All Products" else ""
        st.info(f"**All days of the week below are calculated from the event timestamp, converted to Europe/London time{product_context}.**\\n\\nThis ensures accurate day-of-week analysis for UK-based operations.")

        dow_user = pd.DataFrame({'day_of_week': times_london.dt.day_name(), 'user_pseudo_id': analysis_data['user_pseudo_id'].values})
        unique_per_dow = dow_user.groupby('day_of_week')['user_pseudo_id'].nunique()
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        unique_per_dow = unique_per_dow.reindex(day_order, fill_value=0)

        fig, ax = plt.subplots(figsize=(12, 6))
        bars = ax.bar(range(len(unique_per_dow)), unique_per_dow.values, color=['lightcoral' if day in ['Saturday', 'Sunday'] else 'steelblue' for day in unique_per_dow.index])
        title_suffix = f" - {country_label}" if country_label != "All Countries" else ""
        product_suffix = f" - {selected_product}" if selected_product != "All Products" else ""
        ax.set_title(f'Product Activity by Day of Week{title_suffix}{product_suffix} (London Time)', fontsize=14, fontweight='bold')
        ax.set_xlabel('Day of Week'); ax.set_ylabel('Unique Visitors')
        ax.set_xticks(range(len(unique_per_dow))); ax.set_xticklabels(unique_per_dow.index, rotation=45)
        ax.grid(True, alpha=0.3, axis='y')
        for i, v in enumerate(unique_per_dow.values):
            ax.text(i, v + max(unique_per_dow.values) * 0.01, str(v), ha='center', va='bottom', fontweight='bold')
        plt.tight_layout(); st.pyplot(fig); plt.close()

        available_days = [day for day in day_order if unique_per_dow[day] > 0]
        if available_days:
            default_day = unique_per_dow.idxmax()
            selected_day = st.selectbox(
                "Select day of week for peak time analysis:", available_days,
                index=available_days.index(default_day) if default_day in available_days else 0
            )

            st.markdown(f"### ⏰ Peak Activity Times in London Time ({selected_day})")
            st.info(f"**The chart below shows hour-of-day activity for the selected day of the week: {selected_day}. All times are in London time.")

            mask = times_london.dt.day_name() == selected_day
            day_data = analysis_data[mask]
            day_times = times_london[mask]
            if not day_data.empty:
                hour_user = pd.DataFrame({'hour': day_times.dt.hour, 'user_pseudo_id': day_data['user_pseudo_id'].values})
                unique_per_hour = hour_user.groupby('hour')['user_pseudo_id'].nunique()
                all_hours = pd.Series(0, index=range(24)); all_hours.update(unique_per_hour)
                
                fig, ax = plt.subplots(figsize=(14, 8))
                bars = ax.bar(all_hours.index, all_hours.values, color='darkgreen', alpha=0.7, edgecolor='white', linewidth=1)
                top_3_hours = all_hours.nlargest(3)
                colors = ['gold', 'silver', '#CD7F32']
                for i, (hour, value) in enumerate(top_3_hours.items()):
                    bars[hour].set_color(colors[i]); bars[hour].set_alpha(0.9)
                ax.axvspan(9, 18, alpha=0.1, color='blue', label='Business Hours (9 AM - 6 PM)')
                
                peak_hour = all_hours.idxmax(); peak_value = all_hours.max()
                chart_title = f'Product Activity by Hour - London Time ({selected_day})'
                if selected_product != "All Products": chart_title += f'\\n{selected_product}'
                chart_title += f'\\nPeak: {peak_hour:02d}:00 ({peak_value} unique visitors)'
                ax.set_title(chart_title, fontsize=16, fontweight='bold', pad=20)
                ax.set_xlabel(f'Hour of Day (London Time)', fontsize=12); ax.set_ylabel('Unique Visitors', fontsize=12)
                ax.set_xticks(range(24)); ax.set_xticklabels([f'{h:02d}:00' for h in range(24)], rotation=45)
                ax.grid(True, alpha=0.3, axis='y'); ax.legend()
                
                for i, (hour, value) in enumerate(top_3_hours.items()):
                    rank = ['1st', '2nd', '3rd'][i]
                    ax.annotate(f'{rank}\\n{value} visitors', xy=(hour, value), xytext=(hour, value + max(all_hours) * 0.1),
                               ha='center', va='bottom', fontweight='bold', arrowprops=dict(arrowstyle='->', color='black', alpha=0.7))
                plt.tight_layout(); st.pyplot(fig); plt.close()
            else:
                st.write(f"Not enough data for {selected_day} to show hourly activity.")
    else:
        st.warning(f"No timestamp data available for {country_label} timing analysis.")


def _display_click_analysis(df):
    """Displays analysis of user click behavior."""
    st.markdown("## 🖱️ Click Analysis")
    
    # First, ensure necessary columns from parsing are available
    required_cols = ['event_name', 'click_text', 'click_element', 'click_url', 'section', 'page_type']
    if not all(col in df.columns for col in required_cols):
        st.info("Click analysis requires event parameters that are not available in this dataset.")
        # Try to show what's missing
        missing_cols = [col for col in required_cols if col not in df.columns]
        st.warning(f"Missing expected columns: `{', '.join(missing_cols)}`")
        return

    click_events = df[df['event_name'] == 'click'].copy()

    if click_events.empty:
        st.info("No click events found for the selected filters.")
        return

    # For better readability, let's clean up click_text
    click_events['click_text'] = click_events['click_text'].str.strip().replace('', 'N/A').fillna('N/A')

    st.markdown("### Top Clicked Elements")
    
    # Let user choose what to group by
    group_by_col = st.radio(
        "Group clicks by:",
        ('Click Text', 'Click Element', 'Click URL', 'Page Section'),
        horizontal=True,
        help="Analyze clicks based on the text, underlying HTML element, destination URL, or page section."
    )

    col_map = {
        'Click Text': 'click_text',
        'Click Element': 'click_element',
        'Click URL': 'click_url',
        'Page Section': 'section'
    }
    selected_col = col_map[group_by_col]

    if selected_col not in click_events.columns or click_events[selected_col].dropna().empty:
        st.warning(f"No data available for grouping by '{group_by_col}'.")
        return

    # Aggregate clicks
    click_counts = click_events.groupby(selected_col).size().reset_index(name='Click Count').sort_values('Click Count', ascending=False).dropna()

    if click_counts.empty:
        st.info(f"No clicks to analyze for the selected dimension: {group_by_col}.")
        return

    # Display results
    st.dataframe(click_counts.head(20), use_container_width=True)

    # Further analysis: Clicks by Page Type
    st.markdown("### Top Clicks by Page Type")
    if 'page_type' in click_events.columns:
        clicks_by_page_type = click_events.groupby(['page_type', selected_col]).size().reset_index(name='Click Count')
        
        # Get top 5 page types by total clicks
        top_page_types = click_events['page_type'].value_counts().nlargest(5).index
        
        for page_type in top_page_types:
            with st.expander(f"Details for '{page_type}' pages"):
                page_type_clicks = clicks_by_page_type[clicks_by_page_type['page_type'] == page_type]
                top_clicks_on_page = page_type_clicks.sort_values('Click Count', ascending=False).head(10)
                
                if not top_clicks_on_page.empty:
                    st.dataframe(top_clicks_on_page[[selected_col, 'Click Count']], use_container_width=True)
                else:
                    st.write("No click data for this page type.")


def _display_faq_analysis(df):
    """Displays FAQ interaction frequency table."""
    st.markdown("## ❓ FAQ Interaction Frequency Table")
    if 'event_name' in df.columns and 'faq_question' in df.columns:
        faq_interactions = df[df['event_name'] == 'faq_interaction']
        if not faq_interactions.empty:
            faq_counts = faq_interactions.groupby('faq_question').size().reset_index(name='Interaction Count').sort_values('Interaction Count', ascending=False)
            st.dataframe(faq_counts, use_container_width=True)
        else:
            st.info("No FAQ interaction events found for the selected filters.")
    else:
        st.info("FAQ interaction data not available in this dataset.")


def _display_search_analysis(df):
    """Displays search term frequency table."""
    st.markdown("## 🔍 Search Term Frequency Table")
    search_df = df.copy()
    if 'search_term' not in search_df.columns:
        def extract_search_term(params_json):
            try:
                if pd.isna(params_json): return None
                params = json.loads(params_json)
                for param in params:
                    if param.get('key') == 'search_term':
                        value = param.get('value', {})
                        return value.get('string_value') or value.get('int_value') or value.get('float_value') or value.get('double_value')
                return None
            except Exception:
                return None
        search_df['search_term'] = search_df['event_params_json'].apply(extract_search_term)

    search_events = search_df[search_df['event_name'] == 'search']
    if not search_events.empty and 'search_term' in search_events.columns:
        search_counts = search_events['search_term'].dropna().value_counts().reset_index().rename(columns={'index': 'Search Term', 'search_term': 'Count'})
        if not search_counts.empty:
            st.dataframe(search_counts, use_container_width=True)
        else:
            st.info("No search terms found for the selected filters.")
    else:
        st.info("No search events found for the selected filters.")


def render(context):
    """Render the Product view."""

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
    
    df_parsed = _parse_ga4_event_params(df)
    df_parsed['page_type'] = df_parsed['page_location'].apply(categorize_page_type)

    filtered_df, selected_product = _display_filters(df_parsed)

    if filtered_df.empty:
        st.warning("No data available for the selected filters.")
        return

    tab_geo, tab_timing, tab_behavior = st.tabs([
        "🌍 Geo & Traffic", 
        "🕒 Timing Analysis", 
        "❓ User Behavior"
    ])

    with tab_geo:
        _display_traffic_source_analysis(filtered_df)
        st.divider()
        _display_map_visualization(filtered_df)

    with tab_timing:
        st.markdown("## 🕒 Product Activity Timing Analysis")
        _display_timing_analysis(filtered_df, selected_product)

    with tab_behavior:
        _display_click_analysis(filtered_df)
        st.divider()
        _display_faq_analysis(filtered_df)
        st.divider()
        _display_search_analysis(filtered_df)
