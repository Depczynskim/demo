"""
Google Ads Data Extractor V6 - OPTIMIZED VERSION
Intelligent extraction with:
- Smart backfill from January 1, 2025
- Daily-level tracking and gap detection
- Configurable extraction modes
- API rate limiting and error handling
- No redundant processing
"""

import os
import pandas as pd
from datetime import datetime, timedelta, date
from google.oauth2 import service_account
from google.ads.googleads.client import GoogleAdsClient
import logging
import json
from pathlib import Path
from typing import List, Optional, Tuple
import shutil
import time

# Enhanced logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
OUTPUT_PATH = os.getenv('GOOGLE_ADS_OUTPUT_PATH', './data_repo/google_ads')
METADATA_FILE = os.path.join(OUTPUT_PATH, '.google_ads_extraction_metadata.json')
DATA_START_DATE = date(2025, 1, 1)  # Beginning of 2025

class GoogleAdsMetadata:
    """Manages extraction metadata for intelligent incremental updates."""
    
    def __init__(self, metadata_path: str):
        self.metadata_path = metadata_path
        self.metadata = self._load_metadata()
    
    def _load_metadata(self) -> dict:
        """Load existing metadata or create new."""
        if os.path.exists(self.metadata_path):
            try:
                with open(self.metadata_path, 'r') as f:
                    metadata = json.load(f)
                    # Convert date strings back to date objects
                    if 'extracted_dates' in metadata:
                        metadata['extracted_dates'] = [
                            datetime.strptime(d, '%Y-%m-%d').date() 
                            for d in metadata['extracted_dates']
                        ]
                    return metadata
            except (json.JSONDecodeError, FileNotFoundError, ValueError):
                logger.warning("Corrupted metadata file, creating new one.")
        
        return {
            'last_full_extraction': None,
            'last_incremental_update': None,
            'extracted_dates': [],  # List of date objects
            'extracted_months': [],  # List of YYYYMM strings
            'customer_id': None,
            'data_start_date': DATA_START_DATE.isoformat(),
            'total_api_calls': 0,
            'last_api_error': None
        }
    
    def save_metadata(self):
        """Save metadata to file."""
        os.makedirs(os.path.dirname(self.metadata_path), exist_ok=True)
        
        # Convert date objects to strings for JSON serialization
        metadata_to_save = self.metadata.copy()
        metadata_to_save['extracted_dates'] = [
            d.isoformat() if isinstance(d, date) else d 
            for d in self.metadata['extracted_dates']
        ]
        
        with open(self.metadata_path, 'w') as f:
            json.dump(metadata_to_save, f, indent=2, default=str)
    
    def mark_date_extracted(self, date_obj: date):
        """Mark a specific date as successfully extracted."""
        if date_obj not in self.metadata['extracted_dates']:
            self.metadata['extracted_dates'].append(date_obj)
            # Also track the month
            month_str = date_obj.strftime('%Y%m')
            if month_str not in self.metadata['extracted_months']:
                self.metadata['extracted_months'].append(month_str)
    
    def is_date_extracted(self, date_obj: date) -> bool:
        """Check if a specific date has been extracted."""
        return date_obj in self.metadata['extracted_dates']
    
    def get_missing_dates(self, start_date: date, end_date: date) -> List[date]:
        """Get list of dates that haven't been extracted in the given range."""
        missing_dates = []
        current_date = start_date
        
        while current_date <= end_date:
            if not self.is_date_extracted(current_date):
                missing_dates.append(current_date)
            current_date += timedelta(days=1)
        
        return missing_dates
    
    def update_extraction_time(self, extraction_type: str):
        """Update last extraction timestamp."""
        timestamp = datetime.now().isoformat()
        if extraction_type == 'full':
            self.metadata['last_full_extraction'] = timestamp
        elif extraction_type == 'incremental':
            self.metadata['last_incremental_update'] = timestamp
    
    def increment_api_calls(self):
        """Track API usage."""
        self.metadata['total_api_calls'] += 1

def write_parquet(df: pd.DataFrame, base_path: str, table_name: str, report_month: str, 
                 force_overwrite: bool = False) -> bool:
    """Enhanced parquet writer with better control."""
    path = os.path.join(base_path, table_name, f"report_month={report_month}")
    
    if os.path.exists(path) and os.listdir(path) and not force_overwrite:
        logger.debug(f"Data exists for {report_month}, skipping (use force_overwrite=True to override)")
        return False
    
    if force_overwrite and os.path.exists(path):
        logger.info(f"Force overwriting data for {report_month}")
        shutil.rmtree(path)
    
    os.makedirs(path, exist_ok=True)
    filename = f"data_{datetime.now().strftime('%Y%m%dT%H%M%S')}.parquet"
    full_path = os.path.join(path, filename)
    
    df.to_parquet(full_path, index=False)
    logger.info(f"✓ Wrote {len(df):,} rows to {report_month}")
    return True

class GoogleAdsExtractorV6:
    """
    Optimized Google Ads extractor with intelligent extraction strategies:
    - Smart daily-level tracking
    - Configurable extraction modes
    - API rate limiting and error handling
    - Metadata tracking for efficiency
    """

    def __init__(self, customer_id: Optional[str] = None, login_customer_id: Optional[str] = None, 
                 developer_token: Optional[str] = None, credentials_path: Optional[str] = None):
        # Account configuration
        self.customer_id = customer_id or "1940970197"
        self.login_customer_id = login_customer_id or "9017808460"
        self.developer_token = developer_token or os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN", "PAMTTq3nXJXqye9xWgUsVw")
        self.credentials_path = credentials_path or '/Users/bm/Desktop/pops-analytics-v2/new-google-ads-key.json'
        
        self.metadata = GoogleAdsMetadata(METADATA_FILE)
        self._initialize_client()
        
        # Update customer info in metadata
        self.metadata.metadata['customer_id'] = self.customer_id

    def _initialize_client(self) -> None:
        """Initialize Google Ads client with enhanced error handling."""
        try:
            if not os.path.exists(self.credentials_path):
                raise FileNotFoundError(f"Service account file not found at {self.credentials_path}")

            logger.info(f"Loading Google Ads credentials from: {self.credentials_path}")
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path,
                scopes=["https://www.googleapis.com/auth/adwords"]
            )

            self.client = GoogleAdsClient(
                credentials=credentials,
                developer_token=self.developer_token,
                login_customer_id=self.login_customer_id,
                use_proto_plus=True
            )

            logger.info(f"✓ Initialized Google Ads client for customer: {self.customer_id}")
            
        except Exception as e:
            logger.error(f"Failed to initialize Google Ads client: {e}")
            raise

    def get_performance_data_batch(self, start_date: date, end_date: date, 
                                  campaign_id: Optional[str] = None, 
                                  ad_group_id: Optional[str] = None) -> pd.DataFrame:
        """
        Get keyword performance data for a date range with enhanced error handling.
        """
        date_range = {
            "start_date": start_date.strftime('%Y-%m-%d'),
            "end_date": end_date.strftime('%Y-%m-%d')
        }

        # Enhanced GAQL query with more comprehensive metrics
        query = """
        SELECT
            campaign.id,
            campaign.name,
            campaign.status,
            ad_group.id,
            ad_group.name,
            ad_group.status,
            ad_group_criterion.criterion_id,
            ad_group_criterion.keyword.text,
            ad_group_criterion.keyword.match_type,
            ad_group_criterion.status,
            metrics.impressions,
            metrics.clicks,
            metrics.average_cpc,
            metrics.ctr,
            metrics.conversions,
            metrics.conversions_value,
            metrics.cost_micros,
            metrics.value_per_conversion,
            metrics.average_cpm,
            segments.date,
            segments.day_of_week,
            segments.device
        FROM keyword_view
        WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
        """.format(**date_range)

        # Add optional filters
        if campaign_id:
            query += f" AND campaign.id = {campaign_id}"
        if ad_group_id:
            query += f" AND ad_group.id = {ad_group_id}"

        # Add ordering for consistent results
        query += " ORDER BY segments.date, campaign.id, ad_group.id"

        logger.info(f"Extracting Google Ads data: {start_date} to {end_date}")

        try:
            # Rate limiting - Google Ads has strict limits
            time.sleep(0.1)  # Small delay between requests
            
            ga_service = self.client.get_service("GoogleAdsService")
            response = ga_service.search_stream(
                customer_id=self.customer_id,
                query=query
            )

            self.metadata.increment_api_calls()

            # Parse response into DataFrame
            rows = []
            for batch in response:
                for row in batch.results:
                    rows.append({
                        "campaign_id": row.campaign.id,
                        "campaign_name": row.campaign.name,
                        "campaign_status": row.campaign.status.name,
                        "ad_group_id": row.ad_group.id,
                        "ad_group_name": row.ad_group.name,
                        "ad_group_status": row.ad_group.status.name,
                        "criterion_id": row.ad_group_criterion.criterion_id,
                        "keyword_text": row.ad_group_criterion.keyword.text,
                        "match_type": row.ad_group_criterion.keyword.match_type.name,
                        "criterion_status": row.ad_group_criterion.status.name,
                        "impressions": row.metrics.impressions,
                        "clicks": row.metrics.clicks,
                        "average_cpc": float(row.metrics.average_cpc) / 1_000_000 if row.metrics.average_cpc else 0,
                        "ctr": row.metrics.ctr,
                        "conversions": row.metrics.conversions,
                        "conversions_value": row.metrics.conversions_value,
                        "cost": float(row.metrics.cost_micros) / 1_000_000 if row.metrics.cost_micros else 0,
                        "value_per_conversion": row.metrics.value_per_conversion,
                        "average_cpm": float(row.metrics.average_cpm) / 1_000_000 if row.metrics.average_cpm else 0,
                        "date": row.segments.date,
                        "day_of_week": row.segments.day_of_week.name,
                        "device": row.segments.device.name
                    })

            df = pd.DataFrame(rows)
            logger.info(f"✓ Extracted {len(df):,} keyword records")
            return df

        except Exception as e:
            error_msg = f"Google Ads API error: {str(e)}"
            logger.error(error_msg)
            self.metadata.metadata['last_api_error'] = {
                'timestamp': datetime.now().isoformat(),
                'error': error_msg,
                'date_range': f"{start_date} to {end_date}"
            }
            return pd.DataFrame()

    def get_dates_to_extract(self, mode: str, end_date: Optional[date] = None) -> List[date]:
        """Determine which dates need extraction based on mode."""
        today = date.today()
        if end_date is None:
            # Google Ads data has a 3-day delay, so default to 3 days ago
            end_date = today - timedelta(days=3)
        
        dates_to_process = []
        
        if mode == 'full':
            # Full extraction: all dates from data start
            logger.info(f"Full extraction mode: processing from {DATA_START_DATE}")
            current_date = DATA_START_DATE
            while current_date <= end_date:
                dates_to_process.append(current_date)
                current_date += timedelta(days=1)
            
        elif mode == 'smart':
            # Smart mode: only missing dates
            logger.info("Smart extraction mode: checking for missing dates")
            missing_dates = self.metadata.get_missing_dates(DATA_START_DATE, end_date)
            dates_to_process = missing_dates
            logger.info(f"  → Found {len(dates_to_process)} missing dates")
                    
        elif mode == 'current_month':
            # Current month only
            logger.info("Current month mode")
            month_start = today.replace(day=1)
            current_date = month_start
            while current_date <= end_date:
                dates_to_process.append(current_date)
                current_date += timedelta(days=1)
            
        elif mode == 'last_n_days':
            # Last N days (default 7)
            n_days = 7
            start_date = end_date - timedelta(days=n_days-1)
            logger.info(f"Last {n_days} days mode: {start_date} to {end_date}")
            current_date = start_date
            while current_date <= end_date:
                dates_to_process.append(current_date)
                current_date += timedelta(days=1)
                
        elif mode == 'yesterday':
            # Yesterday only (accounting for Google Ads delay)
            yesterday = today - timedelta(days=3)  # 3-day delay
            dates_to_process = [yesterday]
            logger.info(f"Yesterday mode (with delay): {yesterday}")
        
        logger.info(f"Identified {len(dates_to_process)} dates for processing")
        return sorted(dates_to_process)

    def extract_and_save(self, mode: str = 'smart', batch_size: int = 7, 
                        end_date: Optional[date] = None,
                        campaign_id: Optional[str] = None,
                        ad_group_id: Optional[str] = None) -> dict:
        """
        Main extraction method with configurable modes.
        
        Args:
            mode: 'smart' (default), 'full', 'current_month', 'last_n_days', 'yesterday'
            batch_size: Number of days to process in each API call (for rate limiting)
            end_date: Optional end date (defaults to 3 days ago due to Google Ads delay)
            campaign_id: Optional campaign filter
            ad_group_id: Optional ad group filter
            
        Returns:
            dict: Extraction summary statistics
        """
        logger.info(f"Starting Google Ads extraction - Mode: {mode}, Batch size: {batch_size}")
        
        dates_to_process = self.get_dates_to_extract(mode, end_date)
        
        if not dates_to_process:
            logger.info("No dates to process - everything is up to date!")
            return {'status': 'up_to_date', 'dates_processed': 0}

        stats = {
            'status': 'success',
            'dates_processed': 0,
            'total_rows': 0,
            'months_updated': set(),
            'api_calls_made': 0,
            'errors': []
        }

        # Process dates in batches for API rate limiting
        for i in range(0, len(dates_to_process), batch_size):
            batch_dates = dates_to_process[i:i + batch_size]
            start_date = batch_dates[0]
            end_date_batch = batch_dates[-1]
            
            logger.info(f"Processing batch: {start_date} to {end_date_batch} ({len(batch_dates)} days)")
            
            try:
                df = self.get_performance_data_batch(
                    start_date, end_date_batch, campaign_id, ad_group_id
                )
                stats['api_calls_made'] += 1
                
                if df.empty:
                    logger.warning(f"No data for batch {start_date} to {end_date_batch}")
                    # Still mark dates as extracted to avoid re-processing
                    for date_obj in batch_dates:
                        self.metadata.mark_date_extracted(date_obj)
                        stats['dates_processed'] += 1
                    continue

                # Group by month for saving
                df['month'] = pd.to_datetime(df['date'], format='%Y-%m-%d').dt.strftime('%Y%m')
                
                for month, month_df in df.groupby('month'):
                    # Determine if we should overwrite (current month)
                    current_month = date.today().strftime('%Y%m')
                    force_overwrite = (month == current_month)
                    
                    wrote_data = write_parquet(
                        month_df.drop('month', axis=1), 
                        OUTPUT_PATH, 
                        "google_ads_final", 
                        month, 
                        force_overwrite=force_overwrite
                    )
                    
                    if wrote_data:
                        stats['months_updated'].add(month)

                # Mark individual dates as extracted
                for date_obj in batch_dates:
                    self.metadata.mark_date_extracted(date_obj)
                    stats['dates_processed'] += 1
                
                stats['total_rows'] += len(df)

                # Rate limiting between batches
                if i + batch_size < len(dates_to_process):
                    time.sleep(1)  # 1 second delay between batches

            except Exception as e:
                error_msg = f"Error processing batch {start_date} to {end_date_batch}: {str(e)}"
                logger.error(error_msg)
                stats['errors'].append(error_msg)

        # Update metadata
        extraction_type = 'full' if mode == 'full' else 'incremental'
        self.metadata.update_extraction_time(extraction_type)
        self.metadata.save_metadata()

        # Convert set to list for JSON serialization
        stats['months_updated'] = list(stats['months_updated'])

        # Log summary
        logger.info(f"""
Google Ads Extraction Complete!
───────────────────────────────
Dates processed: {stats['dates_processed']}
Months updated: {len(stats['months_updated'])}
Total rows: {stats['total_rows']:,}
API calls made: {stats['api_calls_made']}
Errors: {len(stats['errors'])}
        """.strip())

        return stats

    def status(self) -> dict:
        """Get current extraction status and recommendations."""
        metadata = self.metadata.metadata
        today = date.today()
        # Account for Google Ads 3-day delay
        latest_available_date = today - timedelta(days=3)
        
        # Calculate coverage statistics
        total_possible_days = (latest_available_date - DATA_START_DATE).days + 1
        extracted_days = len(metadata.get('extracted_dates', []))
        coverage_percent = (extracted_days / total_possible_days * 100) if total_possible_days > 0 else 0
        
        # Find gaps
        missing_dates = self.metadata.get_missing_dates(DATA_START_DATE, latest_available_date)
        
        status = {
            'last_full_extraction': metadata.get('last_full_extraction'),
            'last_incremental_update': metadata.get('last_incremental_update'),
            'data_start_date': DATA_START_DATE.isoformat(),
            'latest_available_date': latest_available_date.isoformat(),
            'extracted_dates_count': extracted_days,
            'total_possible_dates': total_possible_days,
            'coverage_percent': round(coverage_percent, 1),
            'missing_dates_count': len(missing_dates),
            'recent_missing_dates': [d.isoformat() for d in missing_dates[-10:]] if missing_dates else [],
            'customer_id': metadata.get('customer_id'),
            'total_api_calls': metadata.get('total_api_calls', 0),
            'last_api_error': metadata.get('last_api_error'),
            'recommendations': []
        }
        
        # Generate recommendations
        if not metadata.get('last_full_extraction'):
            status['recommendations'].append("Run full extraction first: extractor.extract_and_save(mode='full')")
        
        if missing_dates:
            if len(missing_dates) > 10:
                status['recommendations'].append(f"Many missing dates ({len(missing_dates)}). Consider: mode='smart'")
            else:
                status['recommendations'].append("Few missing dates. Run: mode='smart'")
        
        last_update = metadata.get('last_incremental_update')
        if last_update:
            last_update_date = datetime.fromisoformat(last_update).date()
            days_since_update = (today - last_update_date).days
            if days_since_update > 3:  # Account for Google Ads delay
                status['recommendations'].append(f"Consider update (last: {days_since_update} days ago, accounting for 3-day delay)")
        
        return status

# Convenience functions for common use cases
def quick_update():
    """Quick recent update - perfect for daily cron jobs (accounts for Google Ads delay)."""
    extractor = GoogleAdsExtractorV6()
    return extractor.extract_and_save(mode='yesterday')

def full_backfill():
    """Complete historical backfill from January 1, 2025."""
    extractor = GoogleAdsExtractorV6()
    return extractor.extract_and_save(mode='full')

def smart_sync():
    """Intelligent sync - fills gaps and updates recent data."""
    extractor = GoogleAdsExtractorV6()
    return extractor.extract_and_save(mode='smart')

def current_month_refresh():
    """Refresh current month data."""
    extractor = GoogleAdsExtractorV6()
    return extractor.extract_and_save(mode='current_month')

# Example usage
if __name__ == '__main__':
    extractor = GoogleAdsExtractorV6()
    
    # Show current status
    status = extractor.status()
    print("Google Ads Extraction Status:")
    print(json.dumps(status, indent=2, default=str))
    
    # Run smart extraction
    results = extractor.extract_and_save(mode='smart')
    print("\nExtraction Results:")
    print(json.dumps(results, indent=2, default=str))
