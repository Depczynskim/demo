import streamlit as st
import pandas as pd
import numpy as np
import pydeck as pdk
import matplotlib.pyplot as plt
import pytz
from datetime import datetime

pd.set_option('future.no_silent_downcasting', True)  # Fix for FutureWarning

def render_location_analysis(df_parsed):
    """Renders the map visualization for unique visitors."""
    st.markdown("### Unique Visitors by Location (Hover for Details)")
    # Use the full GA4 dataset for the map
    map_df = (
        df_parsed.dropna(subset=['user_pseudo_id', 'geo_country'])
        .groupby(['geo_country', 'geo_city'])['user_pseudo_id']
        .nunique()
        .reset_index(name='unique_visitors')
    )
    
    # Expanded city coordinates mapping for better map coverage
    city_coords = {
        # United States - Major cities
        ('United States', 'New York'): (40.7128, -74.0060),
        ('United States', 'Los Angeles'): (34.0522, -118.2437),
        ('United States', 'Chicago'): (41.8781, -87.6298),
        ('United States', 'Houston'): (29.7604, -95.3698),
        ('United States', 'Phoenix'): (33.4484, -112.0740),
        ('United States', 'Philadelphia'): (39.9526, -75.1652),
        ('United States', 'San Antonio'): (29.4241, -98.4936),
        ('United States', 'San Diego'): (32.7157, -117.1611),
        ('United States', 'Dallas'): (32.7767, -96.7970),
        ('United States', 'San Jose'): (37.3382, -121.8863),
        ('United States', 'Austin'): (30.2672, -97.7431),
        ('United States', 'Jacksonville'): (30.3322, -81.6557),
        ('United States', 'Fort Worth'): (32.7555, -97.3308),
        ('United States', 'Columbus'): (39.9612, -82.9988),
        ('United States', 'Charlotte'): (35.2271, -80.8431),
        ('United States', 'San Francisco'): (37.7749, -122.4194),
        ('United States', 'Indianapolis'): (39.7684, -86.1581),
        ('United States', 'Seattle'): (47.6062, -122.3321),
        ('United States', 'Denver'): (39.7392, -104.9903),
        ('United States', 'Washington'): (38.9072, -77.0369),
        ('United States', 'Boston'): (42.3601, -71.0589),
        ('United States', 'Nashville'): (36.1627, -86.7816),
        ('United States', 'Baltimore'): (39.2904, -76.6122),
        ('United States', 'Oklahoma City'): (35.4676, -97.5164),
        ('United States', 'Portland'): (45.5152, -122.6784),
        ('United States', 'Las Vegas'): (36.1699, -115.1398),
        ('United States', 'Louisville'): (38.2527, -85.7585),
        ('United States', 'Milwaukee'): (43.0389, -87.9065),
        ('United States', 'Albuquerque'): (35.0844, -106.6504),
        ('United States', 'Tucson'): (32.2226, -110.9747),
        ('United States', 'Fresno'): (36.7378, -119.7871),
        ('United States', 'Sacramento'): (38.5816, -121.4944),
        ('United States', 'Mesa'): (33.4152, -111.8315),
        ('United States', 'Kansas City'): (39.0997, -94.5786),
        ('United States', 'Atlanta'): (33.7490, -84.3880),
        ('United States', 'Miami'): (25.7617, -80.1918),
        ('United States', 'Raleigh'): (35.7796, -78.6382),
        ('United States', 'Omaha'): (41.2565, -95.9345),
        ('United States', 'Minneapolis'): (44.9778, -93.2650),
        ('United States', 'Tulsa'): (36.1540, -95.9928),
        ('United States', 'Cleveland'): (41.4993, -81.6944),
        ('United States', 'Wichita'): (37.6872, -97.3301),
        ('United States', 'Arlington'): (32.7357, -97.1081),
        ('United States', 'Boardman'): (45.8398, -119.7006),
        # United Kingdom
        ('United Kingdom', 'London'): (51.5074, -0.1278),
        ('United Kingdom', 'Birmingham'): (52.4862, -1.8904),
        ('United Kingdom', 'Manchester'): (53.4808, -2.2426),
        ('United Kingdom', 'Glasgow'): (55.8642, -4.2518),
        ('United Kingdom', 'Liverpool'): (53.4084, -2.9916),
        ('United Kingdom', 'Leeds'): (53.8008, -1.5491),
        ('United Kingdom', 'Sheffield'): (53.3811, -1.4701),
        ('United Kingdom', 'Edinburgh'): (55.9533, -3.1883),
        ('United Kingdom', 'Bristol'): (51.4545, -2.5879),
        ('United Kingdom', 'Cardiff'): (51.4816, -3.1791),
        ('United Kingdom', 'Milton Keynes'): (52.0406, -0.7594),
        # Australia
        ('Australia', 'Sydney'): (-33.8688, 151.2093),
        ('Australia', 'Melbourne'): (-37.8136, 144.9631),
        ('Australia', 'Brisbane'): (-27.4698, 153.0251),
        ('Australia', 'Perth'): (-31.9505, 115.8605),
        ('Australia', 'Adelaide'): (-34.9285, 138.6007),
        ('Australia', 'Gold Coast'): (-28.0167, 153.4000),
        ('Australia', 'Newcastle'): (-32.9283, 151.7817),
        ('Australia', 'Canberra'): (-35.2809, 149.1300),
        # Other countries
        ('Canada', 'Toronto'): (43.6532, -79.3832),
        ('Canada', 'Vancouver'): (49.2827, -123.1207),
        ('Canada', 'Montreal'): (45.5017, -73.5673),
        ('France', 'Paris'): (48.8566, 2.3522),
        ('Germany', 'Berlin'): (52.5200, 13.4050),
        ('Spain', 'Madrid'): (40.4168, -3.7038),
        ('Italy', 'Rome'): (41.9028, 12.4964),
        ('Netherlands', 'Amsterdam'): (52.3676, 4.9041),
    }
    
    # Fallback: country centroids for cities not in our mapping
    country_coords = {
        'United States': (39.8283, -98.5795),  # Geographic center of US
        'United Kingdom': (55.3781, -3.4360),
        'Australia': (-25.2744, 133.7751),
        'Canada': (56.1304, -106.3468),
        'France': (46.2276, 2.2137),
        'Germany': (51.1657, 10.4515),
        'Spain': (40.4637, -3.7492),
        'Italy': (41.8719, 12.5674),
        'Netherlands': (52.1326, 5.2913),
        'Poland': (51.9194, 19.1451),
        'New Zealand': (-40.9006, 174.8860),
        'Sweden': (60.1282, 18.6435),
        'Switzerland': (46.8182, 8.2275),
        'Ireland': (53.4129, -8.2439),
        'Greece': (39.0742, 21.8243),
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
        lats.append(lat)
        lons.append(lon)
    
    map_df['lat'] = lats
    map_df['lon'] = lons
    map_df = map_df.dropna(subset=['lat', 'lon'])
    
    st.write(f"**Showing {len(map_df)} locations** (hover over dots for visitor counts)")
    
    if not map_df.empty:
        st.pydeck_chart(pdk.Deck(
            map_provider='carto',               # no token required
            map_style='light',                  # carto basemap style
            initial_view_state=pdk.ViewState(
                latitude=map_df['lat'].mean(),
                longitude=map_df['lon'].mean(),
                zoom=2,
                pitch=0,
            ),
            layers=[
                pdk.Layer(
                    'ScatterplotLayer',
                    data=map_df,
                    get_position='[lon, lat]',
                    get_radius=30000,
                    get_fill_color='[0, 100, 200, 180]',
                    get_line_color='[255, 255, 255, 255]',
                    line_width_min_pixels=2,
                    pickable=True,
                    auto_highlight=True,
                ),
            ],
            tooltip={
                "html": "<b>{geo_country}</b><br/>"
                       "City: {geo_city}<br/>"
                       "Unique Visitors: <b>{unique_visitors}</b>",
                "style": {
                    "backgroundColor": "steelblue",
                    "color": "white",
                    "fontSize": "14px",
                    "padding": "10px",
                    "borderRadius": "5px"
                }
            }
        ))
        
        st.markdown("### Top 10 Locations by Unique Visitors")
        top_locations = map_df.nlargest(10, 'unique_visitors')[['geo_country', 'geo_city', 'unique_visitors']]
        st.dataframe(top_locations, use_container_width=True)
    else:
        st.warning("No geographic data available for mapping.")

def render_timing_analysis(filtered_df, df_parsed):
    """Renders the campaign timing analysis charts."""
    st.markdown("## 🎯 Google Ads Campaign Timing Analysis")

    st.markdown("### Filter by Country for Timing Analysis")
    top_countries = (
        df_parsed.dropna(subset=['user_pseudo_id', 'geo_country'])
        .groupby('geo_country')['user_pseudo_id']
        .nunique()
        .sort_values(ascending=False)
        .head(5)
        .index.tolist()
    )
    available_countries = ['All Countries'] + top_countries
    selected_country = st.selectbox(
        "Choose country to filter timing patterns:",
        available_countries,
        index=0,
        help="Select a specific country to see timing patterns for those visitors. All times are shown in London time."
    )
    
    if selected_country == 'All Countries':
        analysis_data = filtered_df
        country_label = 'All Countries'
    else:
        analysis_data = filtered_df[filtered_df['geo_country'] == selected_country]
        country_label = selected_country
    
    timezone_str = 'Europe/London'
    
    if 'event_timestamp' in analysis_data.columns and not analysis_data.empty:
        times_utc = pd.to_datetime(analysis_data['event_timestamp'] // 1000_000, unit='s', utc=True)
        times_london = times_utc.dt.tz_convert(timezone_str)
        
        st.markdown("### 📅 Day of Week Analysis (London Time)")
        st.info("**All days of the week below are calculated from the event timestamp, converted to Europe/London time. This ensures perfect alignment with your Google Ads scheduling, which is always based on your account's time zone (London).**\n\nFor example, a late-night event in UTC may count as the next day in London if it crosses midnight local time. This is the only correct way to analyze days of the week for UK-based Google Ads campaigns.")

        dow_user = pd.DataFrame({
            'day_of_week': times_london.dt.day_name(),
            'user_pseudo_id': analysis_data['user_pseudo_id'].values
        })
        unique_per_dow = dow_user.groupby('day_of_week')['user_pseudo_id'].nunique()

        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        unique_per_dow = unique_per_dow.reindex(day_order, fill_value=0)

        fig, ax = plt.subplots(figsize=(12, 6))
        bars = ax.bar(range(len(unique_per_dow)), unique_per_dow.values, 
                     color=['lightcoral' if day in ['Saturday', 'Sunday'] else 'steelblue' 
                           for day in unique_per_dow.index])

        ax.set_title(f'Website Activity by Day of Week - {country_label} (London Time)', fontsize=14, fontweight='bold')
        ax.set_xlabel('Day of Week')
        ax.set_ylabel('Unique Visitors')
        ax.set_xticks(range(len(unique_per_dow)))
        ax.set_xticklabels(unique_per_dow.index, rotation=45)
        ax.grid(True, alpha=0.3, axis='y')

        for i, v in enumerate(unique_per_dow.values):
            ax.text(i, v + max(unique_per_dow.values) * 0.01, str(v), 
                   ha='center', va='bottom', fontweight='bold')

        plt.tight_layout()
        st.pyplot(fig)
        plt.close()

        available_days = [day for day in day_order if unique_per_dow[day] > 0]
        default_day = unique_per_dow.idxmax() if len(available_days) > 0 else day_order[0]
        selected_day = st.selectbox(
            "Select day of week for peak time analysis:",
            available_days,
            index=available_days.index(default_day) if default_day in available_days else 0
        )

        st.markdown(f"### ⏰ Peak Activity Times in London Time ({selected_day})")
        st.info(f"**The chart below shows hour-of-day activity for the selected day of the week: {selected_day}. All times are in London time.")

        mask = times_london.dt.day_name() == selected_day
        day_data = analysis_data[mask]
        day_times = times_london[mask]
        if not day_data.empty:
            hour_user = pd.DataFrame({
                'hour': day_times.dt.hour,
                'user_pseudo_id': day_data['user_pseudo_id'].values
            })
            unique_per_hour = hour_user.groupby('hour')['user_pseudo_id'].nunique()
            all_hours = pd.Series(0, index=range(24))
            all_hours.update(unique_per_hour)
            fig, ax = plt.subplots(figsize=(14, 8))
            bars = ax.bar(all_hours.index, all_hours.values, 
                         color='darkgreen', alpha=0.7, edgecolor='white', linewidth=1)
            top_3_hours = all_hours.nlargest(3)
            colors = ['gold', 'silver', '#CD7F32']
            for i, (hour, value) in enumerate(top_3_hours.items()):
                bars[hour].set_color(colors[i])
                bars[hour].set_alpha(0.9)
            ax.axvspan(9, 18, alpha=0.1, color='blue', label='Business Hours (9 AM - 6 PM)')
            peak_hour = all_hours.idxmax()
            peak_value = all_hours.max()
            ax.set_title(f'Website Activity by Hour - London Time ({selected_day})\n'
                        f'Peak: {peak_hour:02d}:00 ({peak_value} unique visitors)', 
                        fontsize=16, fontweight='bold', pad=20)
            ax.set_xlabel(f'Hour of Day (London Time)', fontsize=12)
            ax.set_ylabel('Unique Visitors', fontsize=12)
            ax.set_xticks(range(24))
            ax.set_xticklabels([f'{h:02d}:00' for h in range(24)], rotation=45)
            ax.grid(True, alpha=0.3, axis='y')
            ax.legend()
            for i, (hour, value) in enumerate(top_3_hours.items()):
                rank = ['1st', '2nd', '3rd'][i]
                ax.annotate(f'{rank}\n{value} visitors', 
                           xy=(hour, value), xytext=(hour, value + max(all_hours) * 0.1),
                           ha='center', va='bottom', fontweight='bold',
                           arrowprops=dict(arrowstyle='->', color='black', alpha=0.7))
            plt.tight_layout()
            st.pyplot(fig)
            plt.close()
        else:
            st.write(f"Not enough data for {selected_day} to show hourly activity.")
    else:
        st.warning(f"No timestamp data available for {country_label} timing analysis.")

def render_performance_summary(df):
    """Renders the high-level performance metrics."""
    st.markdown("### 📈 Performance Snapshot")
    
    # Ensure necessary columns exist
    required_cols = ['impressions', 'clicks', 'cost', 'conversions']
    if not all(col in df.columns for col in required_cols):
        st.info("Performance Snapshot requires 'impressions', 'clicks', 'cost', and 'conversions' data.")
        return

    # Calculate overall metrics
    total_impressions = df['impressions'].sum()
    total_clicks = df['clicks'].sum()
    total_cost = df['cost'].sum()
    total_conversions = df['conversions'].sum()

    # Avoid division by zero
    ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
    avg_cpc = (total_cost / total_clicks) if total_clicks > 0 else 0
    cpa = (total_cost / total_conversions) if total_conversions > 0 else 0

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(label="Total Clicks", value=f"{total_clicks:,.0f}")
        st.metric(label="Total Impressions", value=f"{total_impressions:,.0f}")
    with col2:
        st.metric(label="Total Cost", value=f"£{total_cost:,.2f}")
        st.metric(label="Total Conversions", value=f"{total_conversions:,.0f}")
    with col3:
        st.metric(label="Average CTR", value=f"{ctr:.2f}%")
        st.metric(label="Average CPC", value=f"£{avg_cpc:.2f}")
    with col4:
        st.metric(label="Cost Per Conversion (CPA)", value=f"£{cpa:.2f}")
    st.markdown("---")

def render_performance_analysis(_load_parquet, months):
    """Renders the main data table and performance visualizations."""
    st.markdown("## 📊 Google Ads Performance Table")
    google_ads_df = _load_parquet("google_ads", "google_ads_final", months)
    
    if not google_ads_df.empty and "campaign_name" in google_ads_df.columns:
        
        if 'date' in google_ads_df.columns:
            google_ads_df['date'] = pd.to_datetime(google_ads_df['date'])
        else:
            st.warning("Date column not found in Google Ads data. Cannot perform time-based aggregation for Weekly/Monthly.")

        if 'match_type' in google_ads_df.columns:
            match_type_mapping = {
                'EXACT': 'Exact Match',
                'PHRASE': 'Phrase Match',
                'BROAD': 'Broad Match',
                'UNSPECIFIED': 'Unspecified',
                'UNKNOWN': 'Unknown',
            }
            google_ads_df['match_type'] = google_ads_df['match_type'].map(match_type_mapping).fillna(google_ads_df['match_type'])

        aggregation_level = st.selectbox(
            "Select Aggregation Level",
            ["Daily", "Weekly", "Monthly", "Overall Summary"],
            index=0,
            key="gan_aggregation_level"
        )
        
        core_dimension_cols = ['campaign_name']
        for col in ['ad_group_name', 'keyword_text', 'match_type']:
            if col in google_ads_df.columns:
                core_dimension_cols.append(col)

        processed_df = google_ads_df.copy()

        if aggregation_level != "Daily":
            group_by_cols = list(core_dimension_cols)

            if aggregation_level in ["Weekly", "Monthly"]:
                if 'date' not in google_ads_df.columns:
                    st.error("Date column is required for Weekly/Monthly aggregation. Please select 'Daily' or 'Overall Summary'.")
                    st.stop()
                if aggregation_level == "Weekly":
                    processed_df['time_period'] = processed_df['date'].dt.to_period('W').astype(str)
                    group_by_cols.append('time_period')
                elif aggregation_level == "Monthly":
                    processed_df['time_period'] = processed_df['date'].dt.to_period('M').astype(str)
                    group_by_cols.append('time_period')
            
            agg_metrics = {}
            for metric_col in ['impressions', 'clicks', 'cost', 'conversions']:
                if metric_col in processed_df.columns:
                    agg_metrics[metric_col] = 'sum'
            
            if not agg_metrics:
                st.warning("No standard metric columns (impressions, clicks, cost, conversions) found for aggregation.")
            else:
                valid_group_by_cols = [col for col in group_by_cols if col in processed_df.columns]
                if not valid_group_by_cols:
                    st.error("No valid columns found to group by for aggregation.")
                    st.stop()
                
                aggregated_df = processed_df.groupby(valid_group_by_cols, as_index=False).agg(agg_metrics)

                if 'clicks' in aggregated_df.columns and 'impressions' in aggregated_df.columns:
                    aggregated_df['ctr'] = (aggregated_df['clicks'] / aggregated_df['impressions']).replace([np.inf, -np.inf], 0).fillna(0)
                if 'cost' in aggregated_df.columns and 'clicks' in aggregated_df.columns:
                    aggregated_df['average_cpc'] = (aggregated_df['cost'] / aggregated_df['clicks']).replace([np.inf, -np.inf], 0).fillna(0)
                if 'cost' in aggregated_df.columns and 'conversions' in aggregated_df.columns:
                    aggregated_df['cpa'] = (aggregated_df['cost'] / aggregated_df['conversions']).replace([np.inf, -np.inf], 0).fillna(0)
                
                processed_df = aggregated_df
        
        elif 'date' in processed_df.columns:
             processed_df = processed_df.sort_values(by='date', ascending=False)

        campaigns = sorted(processed_df["campaign_name"].dropna().unique())
        selected_campaign = st.selectbox("Filter by Campaign", ["All"] + campaigns, key="gan_campaign")
        
        filtered_by_campaign_df = processed_df.copy()
        if selected_campaign != "All":
            filtered_by_campaign_df = filtered_by_campaign_df[filtered_by_campaign_df["campaign_name"] == selected_campaign]
        
        filtered_table_df = filtered_by_campaign_df.copy()
        if 'ad_group_name' in filtered_by_campaign_df.columns:
            ad_groups = sorted(filtered_by_campaign_df["ad_group_name"].dropna().unique())
            if ad_groups:
                selected_ad_group = st.selectbox("Filter by Ad Group", ["All"] + ad_groups, key="gan_ad_group")
                if selected_ad_group != "All":
                    filtered_table_df = filtered_by_campaign_df[filtered_by_campaign_df["ad_group_name"] == selected_ad_group]
        
        st.write(f"Aggregation: `{aggregation_level}`")
        if selected_campaign != "All":
            st.write(f"Filtered to campaign: `{selected_campaign}`")
        if 'selected_ad_group' in locals() and selected_ad_group != "All":
            st.write(f"Filtered to ad group: `{selected_ad_group}`")
        st.write(f"Rows: {len(filtered_table_df):,}")

        # --- Performance Summary ---
        render_performance_summary(filtered_table_df)

        # --- Main Data Table ---
        st.markdown("### Detailed Data")
        st.dataframe(filtered_table_df.head(500), use_container_width=True)

        st.markdown("--- \n## 📈 Visualizations")

        if not filtered_table_df.empty:
            # Determine available numeric columns for metric selection, excluding pure IDs or non-aggregatable fields
            potential_metric_cols = [col for col in filtered_table_df.select_dtypes(include=np.number).columns 
                                     if col not in ['campaign_id', 'ad_group_id', 'criterion_id']]
            
            # --- Performance Over Time (Line Chart) ---
            st.markdown("### Performance Over Time")
            time_chart_container = st.container()

            if aggregation_level in ["Daily", "Weekly", "Monthly"]:
                date_col_for_time_chart = 'date' if aggregation_level == "Daily" else 'time_period'
                
                if date_col_for_time_chart in filtered_table_df.columns and potential_metric_cols:
                    # Select metrics for the time chart
                    col1, col2 = time_chart_container.columns(2)
                    with col1:
                        selected_metric_1 = st.selectbox(
                            "Select primary metric:", 
                            potential_metric_cols, 
                            index=potential_metric_cols.index('clicks') if 'clicks' in potential_metric_cols else 0,
                            key="metric_1_time_chart"
                        )
                    with col2:
                        secondary_options = ["None"] + potential_metric_cols
                        selected_metric_2 = st.selectbox(
                            "Select secondary metric (optional):", 
                            secondary_options, 
                            index=secondary_options.index('cost') if 'cost' in secondary_options else 0,
                            key="metric_2_time_chart"
                        )

                    # Group data for the chart
                    group_cols = [selected_metric_1]
                    if selected_metric_2 != "None":
                        group_cols.append(selected_metric_2)
                    
                    time_chart_df = filtered_table_df.groupby(date_col_for_time_chart)[group_cols].sum().reset_index()
                    time_chart_df = time_chart_df.sort_values(by=date_col_for_time_chart)

                    if not time_chart_df.empty:
                        fig, ax1 = plt.subplots(figsize=(12, 6))
                        
                        # Plot primary metric
                        color1 = 'tab:blue'
                        ax1.set_xlabel(date_col_for_time_chart.replace("_", " ").title())
                        ax1.set_ylabel(selected_metric_1.replace("_", " ").title(), color=color1)
                        ax1.plot(time_chart_df[date_col_for_time_chart], time_chart_df[selected_metric_1], color=color1, marker='o', label=selected_metric_1.replace("_", " ").title())
                        ax1.tick_params(axis='y', labelcolor=color1)
                        plt.xticks(rotation=45)

                        # Plot secondary metric if selected
                        if selected_metric_2 != "None":
                            ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis
                            color2 = 'tab:red'
                            ax2.set_ylabel(selected_metric_2.replace("_", " ").title(), color=color2)
                            ax2.plot(time_chart_df[date_col_for_time_chart], time_chart_df[selected_metric_2], color=color2, marker='s', linestyle='--', label=selected_metric_2.replace("_", " ").title())
                            ax2.tick_params(axis='y', labelcolor=color2)

                        # Title and layout
                        title = f'{selected_metric_1.replace("_", " ").title()}'
                        if selected_metric_2 != "None":
                            title += f' vs. {selected_metric_2.replace("_", " ").title()}'
                        title += f' Over {aggregation_level} Period'
                        
                        ax1.set_title(title)
                        ax1.grid(True, alpha=0.3)
                        fig.tight_layout()  # otherwise the right y-label is slightly clipped
                        time_chart_container.pyplot(fig)
                        plt.close(fig)
                    else:
                        time_chart_container.info("No data to display for the selected metric and time period for the line chart.")
                else:
                    time_chart_container.info(f"Line chart for '{aggregation_level}' level requires a time column (date/time_period) and numeric metrics.")
            else:
                time_chart_container.info("Performance over time chart is available for Daily, Weekly, or Monthly aggregation levels.")

            st.markdown("### Top Performers")
            bar_chart_container = st.container()

            potential_dimension_cols = [col for col in ['keyword_text', 'ad_group_name', 'campaign_name'] 
                                        if col in filtered_table_df.columns and filtered_table_df[col].nunique() > 0]

            if potential_dimension_cols and potential_metric_cols:
                selected_dimension_bar = bar_chart_container.selectbox(
                    "Select dimension for bar chart:", 
                    potential_dimension_cols, 
                    index=potential_dimension_cols.index('keyword_text') if 'keyword_text' in potential_dimension_cols else 0,
                    key="dimension_bar_chart"
                )
                selected_metric_bar = bar_chart_container.selectbox(
                    "Select metric for bar chart:", 
                    potential_metric_cols, 
                    index=potential_metric_cols.index('clicks') if 'clicks' in potential_metric_cols else 0,
                    key="metric_bar_chart"
                )
                
                if selected_dimension_bar in filtered_table_df.columns and selected_metric_bar in filtered_table_df.columns:
                    bar_chart_df = filtered_table_df.groupby(selected_dimension_bar)[selected_metric_bar].sum().reset_index()
                    bar_chart_df = bar_chart_df.sort_values(by=selected_metric_bar, ascending=False).head(10)

                    if not bar_chart_df.empty:
                        fig2, ax2 = plt.subplots(figsize=(12, 8))
                        ax2.barh(bar_chart_df[selected_dimension_bar], bar_chart_df[selected_metric_bar], color='skyblue')
                        ax2.set_title(f'Top 10 {selected_dimension_bar.replace("_", " ").title()} by {selected_metric_bar.replace("_", " ").title()}')
                        ax2.set_xlabel(selected_metric_bar.replace("_", " ").title())
                        ax2.set_ylabel(selected_dimension_bar.replace("_", " ").title())
                        ax2.invert_yaxis()
                        plt.tight_layout()
                        bar_chart_container.pyplot(fig2)
                        plt.close(fig2)
                    else:
                        bar_chart_container.info("No data to display for the selected dimension and metric for the bar chart.")
                else:
                    bar_chart_container.warning(f"Selected dimension or metric not found for bar chart.")
            else:
                bar_chart_container.info("Bar chart requires at least one categorical dimension (e.g., keyword, ad group) and numeric metrics.")

            st.markdown("### 🥇 Top 10 Keywords by CTR")
            top_keywords_container = st.container()

            if (
                'keyword_text' in filtered_table_df.columns and 
                'clicks' in filtered_table_df.columns and 
                'impressions' in filtered_table_df.columns and
                'cost' in filtered_table_df.columns
            ):
                # Group by keyword_text and sum metrics
                agg_dict = {
                    'total_clicks': ('clicks', 'sum'),
                    'total_impressions': ('impressions', 'sum'),
                    'total_cost': ('cost', 'sum')
                }
                if 'conversions' in filtered_table_df.columns:
                    agg_dict['total_conversions'] = ('conversions', 'sum')
                
                keyword_performance = filtered_table_df.groupby('keyword_text').agg(**agg_dict).reset_index()

                # Calculate CTR
                if not keyword_performance.empty and keyword_performance['total_impressions'].sum() > 0:
                    keyword_performance['ctr'] = keyword_performance['total_clicks'] / keyword_performance['total_impressions']
                else:
                    keyword_performance['ctr'] = 0.0
                keyword_performance['ctr'] = keyword_performance['ctr'].replace([np.inf, -np.inf], 0).fillna(0)

                # Calculate Average CPC
                if not keyword_performance.empty and keyword_performance['total_clicks'].sum() > 0:
                    keyword_performance['average_cpc'] = keyword_performance['total_cost'] / keyword_performance['total_clicks']
                else:
                    keyword_performance['average_cpc'] = 0.0
                keyword_performance['average_cpc'] = keyword_performance['average_cpc'].replace([np.inf, -np.inf], 0).fillna(0)

                # Calculate CPA (if possible)
                if 'total_conversions' in keyword_performance.columns:
                    if not keyword_performance.empty and keyword_performance['total_conversions'].sum() > 0:
                        keyword_performance['cpa'] = (keyword_performance['total_cost'] / keyword_performance['total_conversions'])
                    else:
                        keyword_performance['cpa'] = 0.0
                    keyword_performance['cpa'] = keyword_performance['cpa'].replace([np.inf, -np.inf], 0).fillna(0)

                # Sort by CTR and get top 15
                top_10_keywords_by_ctr = keyword_performance.sort_values(by='ctr', ascending=False).head(15)

                if not top_10_keywords_by_ctr.empty:
                    # Select and rename columns for display
                    display_cols = ['keyword_text', 'ctr', 'total_clicks', 'total_impressions', 'average_cpc']
                    if 'cpa' in top_10_keywords_by_ctr.columns:
                        display_cols.append('cpa')
                    
                    display_df_top_keywords = top_10_keywords_by_ctr[display_cols].copy()
                    
                    rename_dict = {
                        'keyword_text': 'Keyword',
                        'ctr': 'CTR',
                        'total_clicks': 'Clicks',
                        'total_impressions': 'Impressions',
                        'average_cpc': 'Avg. CPC'
                    }
                    if 'cpa' in display_cols:
                        rename_dict['cpa'] = 'CPA'
                    
                    display_df_top_keywords.rename(columns=rename_dict, inplace=True)
                    
                    # Format columns
                    display_df_top_keywords['CTR'] = (display_df_top_keywords['CTR'] * 100).map('{:.2f}%'.format)
                    display_df_top_keywords['Avg. CPC'] = display_df_top_keywords['Avg. CPC'].map('£{:.2f}'.format)
                    if 'CPA' in display_df_top_keywords.columns:
                         display_df_top_keywords['CPA'] = display_df_top_keywords['CPA'].map('£{:.2f}'.format)
                    
                    top_keywords_container.table(display_df_top_keywords)
                else:
                    top_keywords_container.info("No keyword data available to display top performers by CTR.")
            else:
                top_keywords_container.info("Keyword text, clicks, impressions, or cost data not available for the Top Keywords by CTR table.")

        else:
            st.info("No data available to display visualizations. Adjust filters or check data source.")

    else:
        st.info("No campaign data available for this view, or 'campaign_name' column is missing.")

def render(context):
    """Render the Google Ads view"""
    df = context['df']
    _parse_ga4_event_params = context['_parse_ga4_event_params']
    categorize_page_type = context['categorize_page_type']
    _load_parquet = context['_load_parquet']
    months = context['months']
    
    # Use new date handling utility
    from utils import get_filtered_date_range
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
    
    # When filling NA values, explicitly specify the dtype to avoid FutureWarning
    numeric_columns = df.select_dtypes(include=['float64', 'int64']).columns
    for col in numeric_columns:
        if df[col].isna().any():
            df[col] = df[col].fillna(0).astype(df[col].dtype)
    
    # Parse event parameters
    df_parsed = _parse_ga4_event_params(df)
    df_parsed['page_type'] = df_parsed['page_location'].apply(categorize_page_type)

    # In this view, `filtered_df` is the full parsed dataset
    filtered_df = df_parsed.copy()

    # --- Tabbed Layout ---
    st.title("💰 Google Ads Performance")

    # Force a redeploy on Streamlit Cloud
    with st.expander("ℹ️ About this dashboard", expanded=False):
        st.markdown(
            """
            This view breaks down the performance of your Google Ads campaigns. Use it to see where your advertising 
            budget is having the most impact and find opportunities for optimization.

            **What you can explore:**
            
            *   **Location Insights:** Discover which countries and cities are driving clicks and conversions.
            *   **Timing Analysis:** Identify the days and times when your ads are most effective.
            *   **Campaign Performance:** Compare campaigns, ad groups, and keywords by cost, clicks, and conversions to understand ROI.
            """
        )

    tab1, tab2, tab3 = st.tabs(["📍 Location Insights", "⏰ Timing Analysis", "📊 Campaign Performance"])

    with tab1:
        render_location_analysis(df_parsed)

    with tab2:
        render_timing_analysis(filtered_df, df_parsed)

    with tab3:
        render_performance_analysis(_load_parquet, months)
