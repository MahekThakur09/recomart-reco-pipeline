#!/usr/bin/env python3
"""
RecoMart Data Ingestion Pipeline - CSV Data Ingester
This script handles automated ingestion of user interaction data from CSV files
with comprehensive error handling, retry mechanisms, and audit logging.
"""

import pandas as pd
import logging
import time
import shutil
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import hashlib
import json
import yaml
import schedule
import sys

# Import storage manager
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from storage.storage_manager import DataLakeStorageManager

class CSVDataIngester:
    """
    Handles automated ingestion of CSV data sources with error handling and logging
    """
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """Initialize the CSV data ingester"""
        self.config = self._load_config(config_path)
        self.setup_directories()
        self.setup_logging()
        self.storage_manager = DataLakeStorageManager()
        self.ingestion_stats = {
            'start_time': None,
            'end_time': None,
            'files_processed': 0,
            'files_failed': 0,
            'total_records': 0,
            'errors': []
        }
        
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file"""
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
                return config
        except FileNotFoundError:
            # Default configuration
            return {
                'data': {
                    'raw_path': 'data/raw',
                    'archive_path': 'data/archive',
                    'staging_path': 'data/staging'
                },
                'ingestion': {
                    'retry_attempts': 3,
                    'retry_delay': 5,
                    'chunk_size': 10000,
                    'backup_files': True
                },
                'logging': {
                    'level': 'INFO',
                    'file': 'ingestion/logs/csv_ingestion.log',
                    'max_size_mb': 10
                }
            }
    
    def setup_directories(self):
        """Create necessary directories for data storage"""
        directories = [
            self.config['data']['raw_path'],
            self.config['data'].get('archive_path', 'data/archive'),
            self.config['data'].get('staging_path', 'data/staging'),
            'logs',
            'data/ingestion_metadata'
        ]
        
        for directory in directories:
            Path(directory).mkdir(parents=True, exist_ok=True)
    
    def setup_logging(self):
        """Configure comprehensive logging for ingestion process"""
        log_config = self.config.get('logging', {})
        log_level = getattr(logging, log_config.get('level', 'INFO'))
        log_file = log_config.get('file', 'ingestion/logs/csv_ingestion.log')
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
        )
        
        # Setup file handler with rotation
        from logging.handlers import RotatingFileHandler
        max_size = log_config.get('max_size_mb', 10) * 1024 * 1024  # Convert to bytes
        file_handler = RotatingFileHandler(log_file, maxBytes=max_size, backupCount=5)
        file_handler.setFormatter(formatter)
        file_handler.setLevel(log_level)
        
        # Setup console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(log_level)
        
        # Configure logger
        self.logger = logging.getLogger(f"{__name__}.CSVIngester")
        self.logger.setLevel(log_level)
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        # Prevent duplicate logs
        self.logger.propagate = False
    
    def calculate_file_hash(self, file_path: str) -> str:
        """Calculate MD5 hash of file for integrity checking"""
        hash_md5 = hashlib.md5()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            self.logger.error(f"Error calculating hash for {file_path}: {e}")
            return None
    
    def validate_csv_structure(self, file_path: str, expected_columns: List[str] = None) -> Tuple[bool, str]:
        """
        Validate CSV file structure and basic integrity
        
        Args:
            file_path: Path to CSV file
            expected_columns: List of expected column names
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            # Read just the header to check structure
            df_sample = pd.read_csv(file_path, nrows=5, encoding='latin1', engine='python')
            
            if df_sample.empty:
                return False, "CSV file is empty"
            
            if expected_columns:
                missing_cols = set(expected_columns) - set(df_sample.columns)
                if missing_cols:
                    return False, f"Missing required columns: {missing_cols}"
            
            # Check for basic data integrity
            if df_sample.isnull().all().any():
                self.logger.warning(f"Found completely null columns in {file_path}")
            
            return True, "Valid CSV structure"
            
        except pd.errors.EmptyDataError:
            return False, "CSV file is empty or corrupted"
        except pd.errors.ParserError as e:
            return False, f"CSV parsing error: {str(e)}"
        except Exception as e:
            return False, f"Validation error: {str(e)}"
    
    def ingest_events_csv(self, file_path: Optional[str] = None, retry_count: int = 0) -> Dict:
        """
        Ingest user interaction events from CSV file with retry mechanism
        
        Args:
            file_path: Path to events CSV file
            retry_count: Current retry attempt number
            
        Returns:
            Dictionary containing ingestion results
        """
        max_retries = self.config.get('ingestion', {}).get('retry_attempts', 3)
        retry_delay = self.config.get('ingestion', {}).get('retry_delay', 5)
        
        if not file_path:
            file_path = f"{self.config['data']['raw_path']}/events.csv"
        
        result = {
            'source_file': file_path,
            'status': 'failed',
            'records_count': 0,
            'file_size_mb': 0,
            'processing_time_seconds': 0,
            'errors': [],
            'metadata': {}
        }
        
        start_time = time.time()
        
        try:
            self.logger.info(f"Starting ingestion of events data from {file_path} (attempt {retry_count + 1})")
            
            # Check if file exists
            if not Path(file_path).exists():
                raise FileNotFoundError(f"Events file not found: {file_path}")
            
            # Get file metadata
            file_stat = Path(file_path).stat()
            result['file_size_mb'] = round(file_stat.st_size / (1024 * 1024), 2)
            result['metadata']['file_modified'] = datetime.fromtimestamp(file_stat.st_mtime).isoformat()
            result['metadata']['file_hash'] = self.calculate_file_hash(file_path)
            
            # Validate CSV structure
            expected_columns = ['timestamp', 'itemid', 'visitorid', 'event', 'transactionid']
            is_valid, validation_msg = self.validate_csv_structure(file_path, expected_columns)
            if not is_valid:
                raise ValueError(f"CSV validation failed: {validation_msg}")
            
            self.logger.info(f"CSV validation passed: {validation_msg}")
            
            # Read CSV with error handling
            chunk_size = self.config.get('ingestion', {}).get('chunk_size', 10000)
            
            try:
                # Read in chunks for memory efficiency
                chunks = []
                chunk_count = 0
                
                for chunk in pd.read_csv(
                    file_path,
                    encoding="latin1",
                    engine="python",
                    on_bad_lines="skip",
                    chunksize=chunk_size
                ):
                    chunk_count += 1
                    self.logger.debug(f"Processing chunk {chunk_count} with {len(chunk)} records")
                    
                    # Basic chunk validation
                    if chunk.empty:
                        self.logger.warning(f"Empty chunk {chunk_count} encountered")
                        continue
                    
                    chunks.append(chunk)
                
                if not chunks:
                    raise ValueError("No valid data chunks found in CSV file")
                
                # Combine all chunks
                df = pd.concat(chunks, ignore_index=True)
                
            except pd.errors.ParserError as e:
                raise ValueError(f"CSV parsing error: {str(e)}")
            
            # Add ingestion metadata
            df['ingestion_timestamp'] = datetime.now()
            df['ingestion_source'] = 'csv_events'
            df['ingestion_batch_id'] = f"events_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Data quality checks
            initial_records = len(df)
            
            # Remove completely duplicate rows
            df_dedupe = df.drop_duplicates()
            duplicates_removed = initial_records - len(df_dedupe)
            if duplicates_removed > 0:
                self.logger.warning(f"Removed {duplicates_removed} duplicate records")
            
            # Save processed data using storage manager with partitioning
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Use storage manager to organize data with partitioning
            output_path = self.storage_manager.organize_data(
                data=df_dedupe,
                data_type='events',
                source='csv_ingestion',
                timestamp=datetime.now(),
                metadata={
                    'original_file': file_path,
                    'ingestion_timestamp': timestamp,
                    'duplicates_removed': duplicates_removed,
                    'file_size_mb': result['file_size_mb']
                }
            )
            
            # Also save as latest version for easy access
            latest_path = f"{self.config['data']['raw_path']}/events_latest.parquet"
            df_dedupe.to_parquet(latest_path, index=False)
            
            # Archive original file if configured
            if self.config.get('ingestion', {}).get('backup_files', True):
                archive_path = f"{self.config['data'].get('archive_path', 'data/archive')}/events_{timestamp}.csv"
                shutil.copy2(file_path, archive_path)
                self.logger.info(f"Archived original file to {archive_path}")
            
            # Update result
            result.update({
                'status': 'success',
                'records_count': len(df_dedupe),
                'output_file': output_path,
                'duplicates_removed': duplicates_removed,
                'processing_time_seconds': round(time.time() - start_time, 2)
            })
            
            self.logger.info(f"Successfully ingested {len(df_dedupe)} events records")
            return result
            
        except Exception as e:
            error_msg = f"Error ingesting events CSV: {str(e)}"
            self.logger.error(error_msg)
            result['errors'].append(error_msg)
            result['processing_time_seconds'] = round(time.time() - start_time, 2)
            
            # Retry logic
            if retry_count < max_retries:
                self.logger.warning(f"Retrying ingestion in {retry_delay} seconds (attempt {retry_count + 2}/{max_retries + 1})")
                time.sleep(retry_delay)
                return self.ingest_events_csv(file_path, retry_count + 1)
            else:
                self.logger.error(f"Max retries ({max_retries}) exceeded for events ingestion")
                self.ingestion_stats['errors'].append(error_msg)
                return result
    
    def ingest_item_properties_csv(self, file_paths: Optional[List[str]] = None, retry_count: int = 0) -> Dict:
        """
        Ingest item properties from multiple CSV files with retry mechanism
        
        Args:
            file_paths: List of paths to item properties CSV files
            retry_count: Current retry attempt number
            
        Returns:
            Dictionary containing ingestion results
        """
        max_retries = self.config.get('ingestion', {}).get('retry_attempts', 3)
        retry_delay = self.config.get('ingestion', {}).get('retry_delay', 5)
        
        if not file_paths:
            raw_path = self.config['data']['raw_path']
            file_paths = [
                f"{raw_path}/item_properties_part1.csv",
                f"{raw_path}/item_properties_part2.csv"
            ]
        
        result = {
            'source_files': file_paths,
            'status': 'failed',
            'records_count': 0,
            'files_processed': 0,
            'total_file_size_mb': 0,
            'processing_time_seconds': 0,
            'errors': [],
            'metadata': {}
        }
        
        start_time = time.time()
        
        try:
            self.logger.info(f"Starting ingestion of item properties from {len(file_paths)} files (attempt {retry_count + 1})")
            
            combined_chunks = []
            files_processed = 0
            total_size = 0
            
            for file_path in file_paths:
                try:
                    if not Path(file_path).exists():
                        self.logger.warning(f"Item properties file not found: {file_path}")
                        continue
                    
                    # File metadata
                    file_stat = Path(file_path).stat()
                    file_size_mb = round(file_stat.st_size / (1024 * 1024), 2)
                    total_size += file_size_mb
                    
                    self.logger.info(f"Processing item properties file: {file_path} ({file_size_mb} MB)")
                    
                    # Validate structure
                    expected_columns = ['itemid', 'property', 'value']
                    is_valid, validation_msg = self.validate_csv_structure(file_path, expected_columns)
                    if not is_valid:
                        self.logger.warning(f"Skipping invalid file {file_path}: {validation_msg}")
                        continue
                    
                    # Read CSV in chunks
                    chunk_size = self.config.get('ingestion', {}).get('chunk_size', 10000)
                    file_chunks = []
                    
                    for chunk in pd.read_csv(
                        file_path,
                        encoding="latin1",
                        engine="python",
                        on_bad_lines="skip",
                        chunksize=chunk_size
                    ):
                        if not chunk.empty:
                            file_chunks.append(chunk)
                    
                    if file_chunks:
                        file_df = pd.concat(file_chunks, ignore_index=True)
                        combined_chunks.append(file_df)
                        files_processed += 1
                        self.logger.info(f"Successfully processed {file_path}: {len(file_df)} records")
                    
                except Exception as e:
                    error_msg = f"Error processing file {file_path}: {str(e)}"
                    self.logger.error(error_msg)
                    result['errors'].append(error_msg)
            
            if not combined_chunks:
                raise ValueError("No valid item properties files were processed")
            
            # Combine all data
            combined_df = pd.concat(combined_chunks, ignore_index=True)
            
            # Add ingestion metadata
            combined_df['ingestion_timestamp'] = datetime.now()
            combined_df['ingestion_source'] = 'csv_item_properties'
            combined_df['ingestion_batch_id'] = f"items_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Data quality processing
            initial_records = len(combined_df)
            
            # Remove duplicates
            combined_df = combined_df.drop_duplicates(subset=['itemid', 'property'], keep='last')
            duplicates_removed = initial_records - len(combined_df)
            if duplicates_removed > 0:
                self.logger.warning(f"Removed {duplicates_removed} duplicate item-property pairs")
            
            # Save processed data using storage manager with partitioning
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Use storage manager to organize data with partitioning
            output_path = self.storage_manager.organize_data(
                data=combined_df,
                data_type='item_properties',
                source='csv_ingestion',
                timestamp=datetime.now(),
                metadata={
                    'source_files': [os.path.basename(p) for p in file_paths],
                    'ingestion_timestamp': timestamp,
                    'duplicates_removed': duplicates_removed,
                    'total_records': len(combined_df)
                }
            )
            
            # Also save as latest version for easy access
            latest_path = f"{self.config['data']['raw_path']}/item_properties_latest.parquet"
            combined_df.to_parquet(latest_path, index=False)
            
            # Archive original files
            if self.config.get('ingestion', {}).get('backup_files', True):
                archive_dir = Path(self.config['data'].get('archive_path', 'data/archive'))
                for i, file_path in enumerate(file_paths):
                    if Path(file_path).exists():
                        archive_path = archive_dir / f"item_properties_part{i+1}_{timestamp}.csv"
                        shutil.copy2(file_path, archive_path)
                        self.logger.info(f"Archived {file_path} to {archive_path}")
            
            # Update result
            result.update({
                'status': 'success',
                'records_count': len(combined_df),
                'files_processed': files_processed,
                'total_file_size_mb': total_size,
                'output_file': output_path,
                'duplicates_removed': duplicates_removed,
                'processing_time_seconds': round(time.time() - start_time, 2)
            })
            
            self.logger.info(f"Successfully ingested {len(combined_df)} item properties from {files_processed} files")
            return result
            
        except Exception as e:
            error_msg = f"Error ingesting item properties: {str(e)}"
            self.logger.error(error_msg)
            result['errors'].append(error_msg)
            result['processing_time_seconds'] = round(time.time() - start_time, 2)
            
            # Retry logic
            if retry_count < max_retries:
                self.logger.warning(f"Retrying item properties ingestion in {retry_delay} seconds")
                time.sleep(retry_delay)
                return self.ingest_item_properties_csv(file_paths, retry_count + 1)
            else:
                self.logger.error(f"Max retries exceeded for item properties ingestion")
                self.ingestion_stats['errors'].append(error_msg)
                return result
    
    def save_ingestion_metadata(self, results: Dict):
        """Save detailed metadata about the ingestion process"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            metadata_path = f"data/ingestion_metadata/csv_ingestion_{timestamp}.json"
            
            metadata = {
                'ingestion_id': f"csv_batch_{timestamp}",
                'timestamp': datetime.now().isoformat(),
                'ingester_type': 'CSVDataIngester',
                'configuration': self.config,
                'statistics': self.ingestion_stats,
                'results': results
            }
            
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2, default=str)
            
            self.logger.info(f"Ingestion metadata saved to {metadata_path}")
            
        except Exception as e:
            self.logger.error(f"Error saving ingestion metadata: {e}")
    
    def run_full_csv_ingestion(self) -> Dict:
        """
        Run complete CSV data ingestion pipeline with comprehensive logging
        """
        self.logger.info("="*60)
        self.logger.info("STARTING CSV DATA INGESTION PIPELINE")
        self.logger.info("="*60)
        
        self.ingestion_stats['start_time'] = datetime.now()
        
        try:
            results = {}
            
            # Ingest events data
            self.logger.info("Phase 1: Ingesting user events data")
            events_result = self.ingest_events_csv()
            results['events'] = events_result
            
            if events_result['status'] == 'success':
                self.ingestion_stats['files_processed'] += 1
                self.ingestion_stats['total_records'] += events_result['records_count']
            else:
                self.ingestion_stats['files_failed'] += 1
            
            # Ingest item properties data
            self.logger.info("Phase 2: Ingesting item properties data")
            items_result = self.ingest_item_properties_csv()
            results['item_properties'] = items_result
            
            if items_result['status'] == 'success':
                self.ingestion_stats['files_processed'] += items_result['files_processed']
                self.ingestion_stats['total_records'] += items_result['records_count']
            else:
                self.ingestion_stats['files_failed'] += 1
            
            self.ingestion_stats['end_time'] = datetime.now()
            
            # Save metadata
            self.save_ingestion_metadata(results)
            
            # Final summary
            total_time = (self.ingestion_stats['end_time'] - self.ingestion_stats['start_time']).total_seconds()
            
            self.logger.info("="*60)
            self.logger.info("CSV INGESTION PIPELINE COMPLETED")
            self.logger.info(f"Total time: {total_time:.2f} seconds")
            self.logger.info(f"Files processed: {self.ingestion_stats['files_processed']}")
            self.logger.info(f"Files failed: {self.ingestion_stats['files_failed']}")
            self.logger.info(f"Total records: {self.ingestion_stats['total_records']:,}")
            self.logger.info(f"Errors encountered: {len(self.ingestion_stats['errors'])}")
            self.logger.info("="*60)
            
            return results
            
        except Exception as e:
            self.logger.error(f"Critical error in CSV ingestion pipeline: {e}")
            self.ingestion_stats['end_time'] = datetime.now()
            self.ingestion_stats['errors'].append(str(e))
            raise


def main():
    """Main entry point for CSV data ingestion"""
    try:
        ingester = CSVDataIngester()
        results = ingester.run_full_csv_ingestion()
        
        # Print summary to console
        print("\n" + "="*60)
        print("CSV DATA INGESTION SUMMARY")
        print("="*60)
        for source, result in results.items():
            status_symbol = "✅" if result['status'] == 'success' else "❌"
            print(f"{status_symbol} {source.upper()}: {result['status']}")
            if result['status'] == 'success':
                print(f"   Records: {result['records_count']:,}")
                print(f"   Time: {result['processing_time_seconds']}s")
            else:
                print(f"   Errors: {len(result['errors'])}")
        print("="*60)
        
        return 0 if all(r['status'] == 'success' for r in results.values()) else 1
        
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())