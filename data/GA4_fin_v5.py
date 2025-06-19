"""
GA4 BigQuery Data Extractor V6 - OPTIMIZED VERSION
Intelligent extraction with:
- Smart backfill from March 1, 2025
- Daily-level tracking and gap detection
- Configurable extraction modes
- BigQuery cost optimization
- No redundant processing
"""

import os
import datetime
import logging
from typing import Optional, List, Tuple
import pandas as pd
import json
from pathlib import Path

from google.cloud import bigquery
from google.oauth2 import service_account
from dateutil.relativedelta import relativedelta
import shutil

import config
from utils.logging import get_logger

logger = get_logger(__name__)

# Configuration
OUTPUT_PATH = os.getenv('GA4_OUTPUT_PATH', './data_repo/ga4')
METADATA_FILE = os.path.join(OUTPUT_PATH, '.ga4_extraction_metadata.json')
DATA_START_DATE = datetime.date(2025, 3, 1)  # When you started collecting data

class GA4ExtractionMetadata:
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
                    # Convert date strings back to date objects for extracted_dates
                    if 'extracted_dates' in metadata:
                        metadata['extracted_dates'] = [
                            datetime.datetime.strptime(d, '%Y-%m-%d').date() 
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
            'project_id': None,
            'dataset_id': None,
            'data_start_date': DATA_START_DATE.isoformat()
        }
    
    def save_metadata(self):
        """Save metadata to file."""
        os.makedirs(os.path.dirname(self.metadata_path), exist_ok=True)
        
        # Convert date objects to strings for JSON serialization
        metadata_to_save = self.metadata.copy()
        metadata_to_save['extracted_dates'] = [
            d.isoformat() if isinstance(d, datetime.date) else d 
            for d in self.metadata['extracted_dates']
        ]
        
        with open(self.metadata_path, 'w') as f:
            json.dump(metadata_to_save, f, indent=2, default=str)
    
    def mark_date_extracted(self, date_obj: datetime.date):
        """Mark a specific date as successfully extracted."""
        if date_obj not in self.metadata['extracted_dates']:
            self.metadata['extracted_dates'].append(date_obj)
            # Also track the month
            month_str = date_obj.strftime('%Y%m')
            if month_str not in self.metadata['extracted_months']:
                self.metadata['extracted_months'].append(month_str)
    
    def is_date_extracted(self, date_obj: datetime.date) -> bool:
        """Check if a specific date has been extracted."""
        return date_obj in self.metadata['extracted_dates']
    
    def is_month_extracted(self, month_str: str) -> bool:
        """Check if a month has been extracted."""
        return month_str in self.metadata['extracted_months']
    
    def get_missing_dates(self, start_date: datetime.date, end_date: datetime.date) -> List[datetime.date]:
        """Get list of dates that haven't been extracted in the given range."""
        missing_dates = []
        current_date = start_date
        
        while current_date <= end_date:
            if not self.is_date_extracted(current_date):
                missing_dates.append(current_date)
            current_date += datetime.timedelta(days=1)
        
        return missing_dates
    
    def update_extraction_time(self, extraction_type: str):
        """Update last extraction timestamp."""
        timestamp = datetime.datetime.now().isoformat()
        if extraction_type == 'full':
            self.metadata['last_full_extraction'] = timestamp
        elif extraction_type == 'incremental':
            self.metadata['last_incremental_update'] = timestamp

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
    filename = f"data_{datetime.datetime.now().strftime('%Y%m%dT%H%M%S')}.parquet"
    full_path = os.path.join(path, filename)
    
    df.to_parquet(full_path, index=False)
    logger.info(f"✓ Wrote {len(df):,} rows to {report_month}")
    return True

class GA4ExtractorV6:
    """
    Optimized GA4 BigQuery extractor with intelligent extraction strategies:
    - Smart daily-level tracking
    - Configurable extraction modes
    - BigQuery cost optimization
    - Metadata tracking for efficiency
    """

    def __init__(
        self,
        project_id: Optional[str] = None,
        dataset_id: Optional[str] = None,
        credentials_path: Optional[str] = None,
        property_id: Optional[str] = None
    ):
        self.project_id = project_id or config.GOOGLE_CLOUD_PROJECT
        self.dataset_id = dataset_id or config.BIGQUERY_DATASET_ID
        self.property_id = property_id or config.GA4_PROPERTY_ID
        self.credentials_path = credentials_path or config.GOOGLE_APPLICATION_CREDENTIALS
        
        self.metadata = GA4ExtractionMetadata(METADATA_FILE)
        self._init_client()
        
        # Update project info in metadata
        self.metadata.metadata['project_id'] = self.project_id
        self.metadata.metadata['dataset_id'] = self.dataset_id

    def _init_client(self):
        """Initialize BigQuery client."""
        try:
            if self.credentials_path and os.path.exists(self.credentials_path):
                logger.info(f"Loading Google credentials from: {self.credentials_path}")
                credentials = service_account.Credentials.from_service_account_file(self.credentials_path)
                self.bq_client = bigquery.Client(project=self.project_id, credentials=credentials)
            else:
                logger.info("Using default credentials")
                self.bq_client = bigquery.Client(project=self.project_id)
                
            logger.info(f"✓ Initialized BigQuery client for project: {self.project_id}")
            
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery client: {e}")
            raise

    def check_table_exists(self, date_str: str) -> bool:
        """Check if GA4 table exists for a specific date."""
        table_id = f"{self.project_id}.{self.dataset_id}.events_{date_str}"
        try:
            self.bq_client.get_table(table_id)
            return True
        except:
            return False

    def get_available_dates(self, start_date: datetime.date, end_date: datetime.date) -> List[datetime.date]:
        """Get list of dates that have GA4 tables available."""
        available_dates = []
        current_date = start_date
        
        logger.info(f"Checking table availability from {start_date} to {end_date}")
        
        while current_date <= end_date:
            date_str = current_date.strftime('%Y%m%d')
            if self.check_table_exists(date_str):
                available_dates.append(current_date)
            current_date += datetime.timedelta(days=1)
        
        logger.info(f"Found {len(available_dates)} available GA4 tables")
        return available_dates

    def extract_date_range(self, start_date: datetime.date, end_date: datetime.date) -> pd.DataFrame:
        """Extract GA4 data for a specific date range with optimized query."""
        start_date_str = start_date.strftime('%Y%m%d')
        end_date_str = end_date.strftime('%Y%m%d')
        
        # Optimized query with only necessary fields
        query = f"""
        SELECT
          event_date,
          event_name,
          event_timestamp,
          user_pseudo_id,
          user_id,
          event_bundle_sequence_id,
          device.category AS device_category,
          device.mobile_model_name AS device_model,
          device.mobile_brand_name AS device_brand,
          device.operating_system AS os,
          geo.country AS geo_country,
          geo.city AS geo_city,
          traffic_source.source AS traffic_source,
          traffic_source.medium AS traffic_medium,
          app_info.id AS app_id,
          app_info.version AS app_version,
          batch_event_index,
          batch_page_id,
          batch_ordering_id,
          is_active_user,
          TO_JSON_STRING(items) AS items_json,
          TO_JSON_STRING(event_params) AS event_params_json,
          TO_JSON_STRING(user_properties) AS user_properties_json,
          TO_JSON_STRING(user_ltv) AS user_ltv_json,
          TO_JSON_STRING(collected_traffic_source) AS collected_traffic_source_json,
          TO_JSON_STRING(session_traffic_source_last_click) AS session_traffic_source_last_click_json,
          publisher,
          TO_JSON_STRING(privacy_info) AS privacy_info_json
        FROM
          `{self.project_id}.{self.dataset_id}.events_*`
        WHERE
          _TABLE_SUFFIX BETWEEN '{start_date_str}' AND '{end_date_str}'
        ORDER BY
          event_date, event_timestamp
        """
        
        logger.info(f"Extracting GA4 data: {start_date} to {end_date}")
        
        try:
            # Configure job to optimize costs
            job_config = bigquery.QueryJobConfig(
                use_query_cache=True,
                use_legacy_sql=False
            )
            
            query_job = self.bq_client.query(query, job_config=job_config)
            df = query_job.to_dataframe()
            
            # Log query statistics
            if query_job.total_bytes_processed:
                gb_processed = query_job.total_bytes_processed / (1024**3)
                logger.info(f"Query processed {gb_processed:.2f} GB")
            
            logger.info(f"✓ Extracted {len(df):,} events")
            return df
            
        except Exception as e:
            logger.error(f"BigQuery extraction failed: {e}")
            return pd.DataFrame()

    def get_dates_to_extract(self, mode: str, end_date: Optional[datetime.date] = None) -> List[datetime.date]:
        """Determine which dates need extraction based on mode."""
        today = datetime.date.today()
        if end_date is None:
            end_date = today
        
        dates_to_process = []
        
        if mode == 'full':
            # Full extraction: all dates from data start
            logger.info(f"Full extraction mode: processing from {DATA_START_DATE}")
            available_dates = self.get_available_dates(DATA_START_DATE, end_date)
            dates_to_process = available_dates
            
        elif mode == 'smart':
            # Smart mode: only missing dates + recent dates
            logger.info("Smart extraction mode: checking for missing dates")
            available_dates = self.get_available_dates(DATA_START_DATE, end_date)
            
            for date_obj in available_dates:
                if not self.metadata.is_date_extracted(date_obj):
                    dates_to_process.append(date_obj)
            
            logger.info(f"  → Found {len(dates_to_process)} missing dates")
                    
        elif mode == 'current_month':
            # Current month only
            logger.info("Current month mode")
            month_start = today.replace(day=1)
            available_dates = self.get_available_dates(month_start, end_date)
            dates_to_process = available_dates
            
        elif mode == 'last_n_days':
            # Last N days
            n_days = 7  # Default to 7 days
            start_date = today - datetime.timedelta(days=n_days)
            logger.info(f"Last {n_days} days mode: {start_date} to {end_date}")
            available_dates = self.get_available_dates(start_date, end_date)
            dates_to_process = available_dates
            
        elif mode == 'yesterday':
            # Yesterday only (perfect for daily cron)
            yesterday = today - datetime.timedelta(days=1)
            if self.check_table_exists(yesterday.strftime('%Y%m%d')):
                dates_to_process = [yesterday]
            logger.info(f"Yesterday mode: {yesterday}")
        
        logger.info(f"Identified {len(dates_to_process)} dates for processing")
        return sorted(dates_to_process)

    def extract_and_save(self, mode: str = 'smart', batch_size: int = 7, 
                        end_date: Optional[datetime.date] = None) -> dict:
        """
        Main extraction method with configurable modes.
        
        Args:
            mode: 'smart' (default), 'full', 'current_month', 'last_n_days', 'yesterday'
            batch_size: Number of days to process in each query (for cost optimization)
            end_date: Optional end date (defaults to today)
            
        Returns:
            dict: Extraction summary statistics
        """
        logger.info(f"Starting GA4 extraction - Mode: {mode}, Batch size: {batch_size}")
        
        dates_to_process = self.get_dates_to_extract(mode, end_date)
        
        if not dates_to_process:
            logger.info("No dates to process - everything is up to date!")
            return {'status': 'up_to_date', 'dates_processed': 0}

        stats = {
            'status': 'success',
            'dates_processed': 0,
            'total_rows': 0,
            'months_updated': set(),
            'errors': []
        }

        # Process dates in batches for cost optimization
        for i in range(0, len(dates_to_process), batch_size):
            batch_dates = dates_to_process[i:i + batch_size]
            start_date = batch_dates[0]
            end_date = batch_dates[-1]
            
            logger.info(f"Processing batch: {start_date} to {end_date} ({len(batch_dates)} days)")
            
            try:
                df = self.extract_date_range(start_date, end_date)
                
                if df.empty:
                    logger.warning(f"No data for batch {start_date} to {end_date}")
                    continue

                # Group by month for saving
                df['month'] = pd.to_datetime(df['event_date'], format='%Y%m%d').dt.strftime('%Y%m')
                
                for month, month_df in df.groupby('month'):
                    # Determine if we should overwrite (current month)
                    current_month = datetime.date.today().strftime('%Y%m')
                    force_overwrite = (month == current_month)
                    
                    wrote_data = write_parquet(
                        month_df.drop('month', axis=1), 
                        OUTPUT_PATH, 
                        "analytics_events_final", 
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

            except Exception as e:
                error_msg = f"Error processing batch {start_date} to {end_date}: {str(e)}"
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
GA4 Extraction Complete!
────────────────────────
Dates processed: {stats['dates_processed']}
Months updated: {len(stats['months_updated'])}
Total rows: {stats['total_rows']:,}
Errors: {len(stats['errors'])}
        """.strip())

        return stats

    def status(self) -> dict:
        """Get current extraction status and recommendations."""
        metadata = self.metadata.metadata
        today = datetime.date.today()
        
        # Calculate coverage statistics
        total_possible_days = (today - DATA_START_DATE).days + 1
        extracted_days = len(metadata.get('extracted_dates', []))
        coverage_percent = (extracted_days / total_possible_days * 100) if total_possible_days > 0 else 0
        
        # Find gaps
        missing_dates = self.metadata.get_missing_dates(DATA_START_DATE, today - datetime.timedelta(days=1))
        
        status = {
            'last_full_extraction': metadata.get('last_full_extraction'),
            'last_incremental_update': metadata.get('last_incremental_update'),
            'data_start_date': DATA_START_DATE.isoformat(),
            'extracted_dates_count': extracted_days,
            'total_possible_dates': total_possible_days,
            'coverage_percent': round(coverage_percent, 1),
            'missing_dates_count': len(missing_dates),
            'recent_missing_dates': [d.isoformat() for d in missing_dates[-10:]] if missing_dates else [],
            'project_id': metadata.get('project_id'),
            'dataset_id': metadata.get('dataset_id'),
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
            last_update_date = datetime.datetime.fromisoformat(last_update).date()
            days_since_update = (today - last_update_date).days
            if days_since_update > 1:
                status['recommendations'].append(f"Consider daily update (last: {days_since_update} days ago)")
        
        return status

# Convenience functions for common use cases
def quick_update():
    """Quick yesterday update - perfect for daily cron jobs."""
    extractor = GA4ExtractorV6()
    return extractor.extract_and_save(mode='yesterday')

def full_backfill():
    """Complete historical backfill from March 1, 2025."""
    extractor = GA4ExtractorV6()
    return extractor.extract_and_save(mode='full')

def smart_sync():
    """Intelligent sync - fills gaps and updates recent data."""
    extractor = GA4ExtractorV6()
    return extractor.extract_and_save(mode='smart')

def current_month_refresh():
    """Refresh current month data."""
    extractor = GA4ExtractorV6()
    return extractor.extract_and_save(mode='current_month')

# Example usage
if __name__ == '__main__':
    extractor = GA4ExtractorV6()
    
    # Show current status
    status = extractor.status()
    print("GA4 Extraction Status:")
    print(json.dumps(status, indent=2, default=str))
    
    # Run smart extraction
    results = extractor.extract_and_save(mode='smart')
    print("\nExtraction Results:")
    print(json.dumps(results, indent=2, default=str))
