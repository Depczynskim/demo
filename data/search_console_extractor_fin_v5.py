# Search Console Data Extractor V6 - OPTIMIZED VERSION
# Intelligent extraction that only processes what's needed:
# - Historical backfill mode for initial setup
# - Smart incremental updates for ongoing maintenance
# - Configurable extraction strategies

import os
import datetime
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from typing import List, Optional
import logging
import config
from dateutil.relativedelta import relativedelta
import shutil
import json
from pathlib import Path

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
OUTPUT_PATH = os.getenv('SEARCH_CONSOLE_OUTPUT_PATH', './data_repo/search_console')
METADATA_FILE = os.path.join(OUTPUT_PATH, '.extraction_metadata.json')

class ExtractionMetadata:
    """Manages extraction metadata for intelligent incremental updates."""
    
    def __init__(self, metadata_path: str):
        self.metadata_path = metadata_path
        self.metadata = self._load_metadata()
    
    def _load_metadata(self) -> dict:
        """Load existing metadata or create new."""
        if os.path.exists(self.metadata_path):
            try:
                with open(self.metadata_path, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                logger.warning("Corrupted metadata file, creating new one.")
        
        return {
            'last_full_extraction': None,
            'last_incremental_update': None,
            'extracted_months': [],
            'site_url': None
        }
    
    def save_metadata(self):
        """Save metadata to file."""
        os.makedirs(os.path.dirname(self.metadata_path), exist_ok=True)
        with open(self.metadata_path, 'w') as f:
            json.dump(self.metadata, f, indent=2, default=str)
    
    def mark_month_extracted(self, month_str: str):
        """Mark a month as successfully extracted."""
        if month_str not in self.metadata['extracted_months']:
            self.metadata['extracted_months'].append(month_str)
    
    def is_month_extracted(self, month_str: str) -> bool:
        """Check if a month has been extracted."""
        return month_str in self.metadata['extracted_months']
    
    def update_extraction_time(self, extraction_type: str):
        """Update last extraction timestamp."""
        timestamp = datetime.datetime.now().isoformat()
        if extraction_type == 'full':
            self.metadata['last_full_extraction'] = timestamp
        elif extraction_type == 'incremental':
            self.metadata['last_incremental_update'] = timestamp

def write_parquet(df: pd.DataFrame, base_path: str, table_name: str, report_month: str, 
                 force_overwrite: bool = False) -> bool:
    """Enhanced parquet writer with better logging and control."""
    path = os.path.join(base_path, table_name, f"report_month={report_month}")
    
    if os.path.exists(path) and os.listdir(path) and not force_overwrite:
        logger.debug(f"Data exists for {report_month}, skipping (use force_overwrite=True to override)")
        return False
    
    if force_overwrite and os.path.exists(path):
        logger.info(f"Force overwriting data for {report_month}")
        shutil.rmtree(path)
    
    os.makedirs(path, exist_ok=True)
    filename = f"data_{datetime.datetime.now().strftime('%Y%m%dT%H%M%S')}.parquet"
    full_path = os.path.join(path, filename)
    
    df.to_parquet(full_path, index=False)
    logger.info(f"✓ Wrote {len(df):,} rows to {report_month}")
    return True

class SearchConsoleExtractorV6:
    """
    Optimized Search Console extractor with intelligent extraction strategies:
    - Smart incremental updates
    - Configurable extraction modes
    - Metadata tracking for efficiency
    """

    def __init__(self, site_url: Optional[str] = None, credentials_path: Optional[str] = None):
        self.site_url = site_url or "sc-domain:pops.studio"
        self.credentials_path = credentials_path or config.GOOGLE_APPLICATION_CREDENTIALS
        self.metadata = ExtractionMetadata(METADATA_FILE)
        self._init_client()
        
        # Update site URL in metadata
        self.metadata.metadata['site_url'] = self.site_url

    def _init_client(self):
        """Initialize Google Search Console API client."""
        try:
            if not os.path.exists(self.credentials_path):
                raise FileNotFoundError(f"Service account file not found at {self.credentials_path}")
                
            logger.info(f"Loading credentials from: {self.credentials_path}")
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path,
                scopes=["https://www.googleapis.com/auth/webmasters"]
            )
            self.client = build('searchconsole', 'v1', credentials=credentials)
            logger.info(f"✓ Initialized Search Console client for: {self.site_url}")
            
        except Exception as e:
            logger.error(f"Failed to initialize Search Console client: {e}")
            raise

    def get_search_analytics(self, start_date: str, end_date: str, 
                           dimensions: List[str]) -> pd.DataFrame:
        """Fetch search analytics data with automatic pagination."""
        all_rows = []
        start_row = 0
        row_limit = 25000
        total_requests = 0

        logger.info(f"Fetching data: {start_date} to {end_date}")
        
        while True:
            total_requests += 1
            request = {
                'startDate': start_date,
                'endDate': end_date,
                'dimensions': dimensions,
                'rowLimit': row_limit,
                'startRow': start_row
            }
            
            try:
                response = self.client.searchanalytics().query(
                    siteUrl=self.site_url, body=request
                ).execute()
                new_rows = response.get('rows', [])
                
            except Exception as e:
                logger.error(f"API error on request {total_requests}: {e}")
                break

            if not new_rows:
                break

            all_rows.extend(new_rows)
            
            if len(new_rows) < row_limit:
                break

            start_row += len(new_rows)
            
            # Progress logging for large extractions
            if total_requests % 5 == 0:
                logger.info(f"  → Fetched {len(all_rows):,} rows so far...")

        if not all_rows:
            logger.warning(f"No data returned for period {start_date} to {end_date}")
            return pd.DataFrame()

        # Convert to DataFrame
        data = []
        for row in all_rows:
            entry = {dim: key for dim, key in zip(dimensions, row['keys'])}
            entry.update({
                'clicks': row.get('clicks', 0),
                'impressions': row.get('impressions', 0),
                'ctr': row.get('ctr', 0.0),
                'position': row.get('position', 0.0)
            })
            data.append(entry)
            
        logger.info(f"✓ Processed {len(data):,} rows from {total_requests} API requests")
        return pd.DataFrame(data)

    def get_months_to_extract(self, mode: str, lookback_months: int = 16) -> List[tuple]:
        """Determine which months need extraction based on mode and existing data."""
        today = datetime.date.today()
        current_month_str = today.strftime('%Y%m')
        months_to_process = []
        
        if mode == 'full':
            # Full extraction: all months regardless of existing data
            logger.info(f"Full extraction mode: processing {lookback_months} months")
            for i in range(lookback_months):
                target_month_start = (today - relativedelta(months=i)).replace(day=1)
                months_to_process.append((target_month_start, i == 0))
                
        elif mode == 'smart':
            # Smart mode: only missing months + current month
            logger.info("Smart extraction mode: checking for missing months")
            for i in range(lookback_months):
                target_month_start = (today - relativedelta(months=i)).replace(day=1)
                month_str = target_month_start.strftime('%Y%m')
                is_current = (i == 0)
                
                if is_current or not self.metadata.is_month_extracted(month_str):
                    months_to_process.append((target_month_start, is_current))
                    if not is_current:
                        logger.info(f"  → Missing month detected: {month_str}")
                        
        elif mode == 'current_only':
            # Current month only
            logger.info("Current month only mode")
            current_month_start = today.replace(day=1)
            months_to_process.append((current_month_start, True))
            
        elif mode == 'last_n_days':
            # Last N days regardless of month boundaries
            logger.info(f"Last {lookback_months} days mode")
            start_date = today - datetime.timedelta(days=lookback_months)
            # Group into month chunks for processing
            current_date = start_date
            processed_months = set()
            
            while current_date <= today:
                month_start = current_date.replace(day=1)
                month_str = month_start.strftime('%Y%m')
                if month_str not in processed_months:
                    is_current = (month_str == current_month_str)
                    months_to_process.append((month_start, is_current))
                    processed_months.add(month_str)
                current_date += datetime.timedelta(days=1)
        
        logger.info(f"Identified {len(months_to_process)} months for processing")
        return months_to_process

    def extract_and_save(self, mode: str = 'smart', dimensions: List[str] = None, 
                        lookback_months: int = 16) -> dict:
        """
        Main extraction method with configurable modes.
        
        Args:
            mode: 'smart' (default), 'full', 'current_only', or 'last_n_days'
            dimensions: List of dimensions to extract
            lookback_months: How many months/days to look back
            
        Returns:
            dict: Extraction summary statistics
        """
        if dimensions is None:
            dimensions = ['query', 'page', 'country', 'device', 'date']

        logger.info(f"Starting extraction - Mode: {mode}, Dimensions: {dimensions}")
        
        months_to_process = self.get_months_to_extract(mode, lookback_months)
        
        if not months_to_process:
            logger.info("No months to process - everything is up to date!")
            return {'status': 'up_to_date', 'months_processed': 0}

        stats = {
            'status': 'success',
            'months_processed': 0,
            'total_rows': 0,
            'months_skipped': 0,
            'errors': []
        }

        for target_month_start, is_current in months_to_process:
            next_month = target_month_start + relativedelta(months=1)
            today = datetime.date.today()
            target_month_end = min(next_month - datetime.timedelta(days=1), today)

            start_date_str = target_month_start.strftime('%Y-%m-%d')
            end_date_str = target_month_end.strftime('%Y-%m-%d')
            report_month_str = target_month_start.strftime('%Y%m')

            logger.info(f"Processing {report_month_str} ({'current' if is_current else 'historical'})")

            try:
                df = self.get_search_analytics(start_date_str, end_date_str, dimensions)
                
                if df.empty:
                    logger.warning(f"No data for {report_month_str}")
                    stats['months_skipped'] += 1
                    continue

                # Write data
                wrote_data = write_parquet(
                    df, OUTPUT_PATH, "search_console_final", 
                    report_month_str, force_overwrite=is_current
                )
                
                if wrote_data:
                    stats['months_processed'] += 1
                    stats['total_rows'] += len(df)
                    self.metadata.mark_month_extracted(report_month_str)
                else:
                    stats['months_skipped'] += 1

            except Exception as e:
                error_msg = f"Error processing {report_month_str}: {str(e)}"
                logger.error(error_msg)
                stats['errors'].append(error_msg)

        # Update metadata
        extraction_type = 'full' if mode == 'full' else 'incremental'
        self.metadata.update_extraction_time(extraction_type)
        self.metadata.save_metadata()

        # Log summary
        logger.info(f"""
Extraction Complete!
─────────────────────
Months processed: {stats['months_processed']}
Months skipped: {stats['months_skipped']}
Total rows: {stats['total_rows']:,}
Errors: {len(stats['errors'])}
        """.strip())

        return stats

    def status(self) -> dict:
        """Get current extraction status and recommendations."""
        metadata = self.metadata.metadata
        today = datetime.date.today()
        
        status = {
            'last_full_extraction': metadata.get('last_full_extraction'),
            'last_incremental_update': metadata.get('last_incremental_update'),
            'extracted_months_count': len(metadata.get('extracted_months', [])),
            'site_url': metadata.get('site_url'),
            'current_month': today.strftime('%Y%m'),
            'recommendations': []
        }
        
        # Generate recommendations
        if not metadata.get('last_full_extraction'):
            status['recommendations'].append("Run full extraction first: extractor.extract_and_save(mode='full')")
        
        last_update = metadata.get('last_incremental_update')
        if last_update:
            last_update_date = datetime.datetime.fromisoformat(last_update).date()
            days_since_update = (today - last_update_date).days
            if days_since_update > 7:
                status['recommendations'].append(f"Consider incremental update (last: {days_since_update} days ago)")
        
        return status

# Convenience functions for common use cases
def quick_update():
    """Quick current month update - perfect for daily/weekly cron jobs."""
    extractor = SearchConsoleExtractorV6()
    return extractor.extract_and_save(mode='current_only')

def full_backfill(months: int = 16):
    """Complete historical backfill."""
    extractor = SearchConsoleExtractorV6()
    return extractor.extract_and_save(mode='full', lookback_months=months)

def smart_sync():
    """Intelligent sync - fills gaps and updates current month."""
    extractor = SearchConsoleExtractorV6()
    return extractor.extract_and_save(mode='smart')

# Example usage
if __name__ == '__main__':
    extractor = SearchConsoleExtractorV6()
    
    # Show current status
    status = extractor.status()
    print("Current Status:", json.dumps(status, indent=2, default=str))
    
    # Run smart extraction
    results = extractor.extract_and_save(mode='smart')
    print("Results:", json.dumps(results, indent=2))
