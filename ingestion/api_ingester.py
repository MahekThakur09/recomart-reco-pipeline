#!/usr/bin/env python3
"""
RecoMart Data Ingestion Pipeline - REST API Data Ingester
This script handles automated ingestion of product data from REST APIs
with comprehensive error handling, retry mechanisms, and audit logging.
"""

import pandas as pd
import requests
import logging
import time
import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
import hashlib
import yaml
import sys
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import urllib3

# Disable SSL warnings for testing
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Import storage manager
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from storage.storage_manager import DataLakeStorageManager

class APIDataIngester:
    """
    Handles automated ingestion of product data from REST APIs with error handling and logging
    """
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """Initialize the API data ingester"""
        self.config = self._load_config(config_path)
        self.setup_directories()
        self.setup_logging()
        self.setup_http_session()
        self.storage_manager = DataLakeStorageManager()
        self.ingestion_stats = {
            'start_time': None,
            'end_time': None,
            'apis_processed': 0,
            'apis_failed': 0,
            'total_records': 0,
            'errors': []
        }
        
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file"""
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
                # Add API-specific defaults if not present
                if 'api_ingestion' not in config:
                    config['api_ingestion'] = {
                        'retry_attempts': 3,
                        'retry_delay': 5,
                        'timeout': 30,
                        'batch_size': 100,
                        'rate_limit_delay': 1,
                        'backup_responses': True
                    }
                return config
        except FileNotFoundError:
            # Default configuration for API ingestion
            return {
                'data': {
                    'raw_path': 'data/raw',
                    'archive_path': 'data/archive',
                    'staging_path': 'data/staging'
                },
                'api_ingestion': {
                    'retry_attempts': 3,
                    'retry_delay': 5,
                    'timeout': 30,
                    'batch_size': 100,
                    'rate_limit_delay': 1,
                    'backup_responses': True
                },
                'logging': {
                    'level': 'INFO',
                    'file': 'ingestion/logs/api_ingestion.log',
                    'max_size_mb': 10
                },
                # Sample API endpoints
                'api_endpoints': {
                    'products': {
                        'url': 'https://dummyjson.com/products',
                        'method': 'GET',
                        'params': {'limit': 100, 'skip': 0},
                        'headers': {'Content-Type': 'application/json'},
                        'auth': None
                    },
                    'categories': {
                        'url': 'https://dummyjson.com/products/categories',
                        'method': 'GET',
                        'params': {},
                        'headers': {'Content-Type': 'application/json'},
                        'auth': None
                    }
                }
            }
    
    def setup_directories(self):
        """Create necessary directories for data storage"""
        directories = [
            self.config['data']['raw_path'],
            self.config['data'].get('archive_path', 'data/archive'),
            self.config['data'].get('staging_path', 'data/staging'),
            'logs',
            'data/ingestion_metadata',
            'data/api_responses'
        ]
        
        for directory in directories:
            Path(directory).mkdir(parents=True, exist_ok=True)
    
    def setup_logging(self):
        """Configure comprehensive logging for API ingestion process"""
        log_config = self.config.get('logging', {})
        log_level = getattr(logging, log_config.get('level', 'INFO'))
        log_file = log_config.get('file', 'ingestion/logs/api_ingestion.log')
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
        )
        
        # Setup file handler with rotation
        from logging.handlers import RotatingFileHandler
        max_size = log_config.get('max_size_mb', 10) * 1024 * 1024
        file_handler = RotatingFileHandler(log_file, maxBytes=max_size, backupCount=5)
        file_handler.setFormatter(formatter)
        file_handler.setLevel(log_level)
        
        # Setup console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(log_level)
        
        # Configure logger
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.logger.setLevel(log_level)
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        # Prevent duplicate logs
        self.logger.propagate = False
        
        self.logger.info("API Data Ingester initialized successfully")
    
    def setup_http_session(self):
        """Setup HTTP session with retry strategy"""
        self.session = requests.Session()
        
        retry_strategy = Retry(
            total=self.config['api_ingestion']['retry_attempts'],
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST"]  # Updated parameter name
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Set default timeout
        self.session.timeout = self.config['api_ingestion']['timeout']
    
    def make_api_request(self, endpoint_name: str, endpoint_config: Dict) -> Tuple[bool, Optional[Dict], str]:
        """
        Make API request with error handling and retry logic
        
        Returns:
            Tuple of (success, response_data, error_message)
        """
        url = endpoint_config['url']
        method = endpoint_config.get('method', 'GET')
        params = endpoint_config.get('params', {})
        headers = endpoint_config.get('headers', {})
        auth = endpoint_config.get('auth')
        
        self.logger.info(f"Making {method} request to {url}")
        
        for attempt in range(1, self.config['api_ingestion']['retry_attempts'] + 1):
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    headers=headers,
                    auth=auth,
                    timeout=self.config['api_ingestion']['timeout'],
                    verify=False  # Disable SSL verification for testing
                )
                
                if response.status_code == 200:
                    data = response.json()
                    self.logger.info(f"Successfully fetched data from {endpoint_name}")
                    return True, data, ""
                elif response.status_code == 429:
                    self.logger.warning(f"Rate limit hit for {endpoint_name}, attempt {attempt}")
                    time.sleep(self.config['api_ingestion']['retry_delay'] * attempt)
                    continue
                else:
                    error_msg = f"HTTP {response.status_code}: {response.text}"
                    self.logger.error(f"API request failed for {endpoint_name}: {error_msg}")
                    return False, None, error_msg
                    
            except requests.exceptions.RequestException as e:
                error_msg = f"Request exception: {str(e)}"
                self.logger.error(f"Attempt {attempt} failed for {endpoint_name}: {error_msg}")
                
                if attempt < self.config['api_ingestion']['retry_attempts']:
                    delay = self.config['api_ingestion']['retry_delay'] * attempt
                    self.logger.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    return False, None, error_msg
        
        return False, None, f"All {self.config['api_ingestion']['retry_attempts']} attempts failed"
    
    def process_api_data(self, endpoint_name: str, raw_data: Dict) -> Optional[pd.DataFrame]:
        """
        Process raw API response data into structured DataFrame
        
        Args:
            endpoint_name: Name of the API endpoint
            raw_data: Raw JSON response from API
            
        Returns:
            Processed DataFrame or None if processing fails
        """
        try:
            if endpoint_name == 'products':
                # Extract products array from DummyJSON response
                if 'products' in raw_data:
                    df = pd.DataFrame(raw_data['products'])
                    
                    # Data quality enhancements
                    df['ingested_at'] = datetime.now()
                    df['data_source'] = 'api_products'
                    
                    # Handle nested fields
                    if 'tags' in df.columns:
                        df['tags'] = df['tags'].apply(lambda x: ','.join(x) if isinstance(x, list) else x)
                    
                    if 'images' in df.columns:
                        df['primary_image'] = df['images'].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else None)
                        df['image_count'] = df['images'].apply(lambda x: len(x) if isinstance(x, list) else 0)
                    
                    self.logger.info(f"Processed {len(df)} product records")
                    return df
                else:
                    self.logger.error("No 'products' key found in API response")
                    return None
                    
            elif endpoint_name == 'categories':
                # Process categories data
                if isinstance(raw_data, list):
                    df = pd.DataFrame([{'category': cat, 'ingested_at': datetime.now(), 'data_source': 'api_categories'} for cat in raw_data])
                    self.logger.info(f"Processed {len(df)} category records")
                    return df
                else:
                    self.logger.error("Expected list format for categories data")
                    return None
            
            else:
                self.logger.warning(f"No specific processing logic for endpoint: {endpoint_name}")
                # Generic processing
                if isinstance(raw_data, list):
                    df = pd.DataFrame(raw_data)
                elif isinstance(raw_data, dict):
                    df = pd.DataFrame([raw_data])
                else:
                    self.logger.error(f"Unsupported data format for {endpoint_name}")
                    return None
                
                df['ingested_at'] = datetime.now()
                df['data_source'] = f'api_{endpoint_name}'
                return df
                
        except Exception as e:
            self.logger.error(f"Error processing data for {endpoint_name}: {str(e)}")
            return None
    
    def save_api_data(self, df: pd.DataFrame, endpoint_name: str) -> Dict:
        """Save processed API data to storage using storage manager"""
        result = {
            'status': 'failed',
            'records_count': 0,
            'output_file': '',
            'backup_file': '',
            'error': ''
        }
        
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Use storage manager to organize data with partitioning
            output_path = self.storage_manager.organize_data(
                data=df,
                data_type=endpoint_name,
                source='api_ingestion',
                timestamp=datetime.now(),
                metadata={
                    'endpoint': endpoint_name,
                    'ingestion_timestamp': timestamp,
                    'api_source': 'dummyjson',
                    'total_records': len(df)
                }
            )
            
            # Save as latest version for easy access
            latest_path = f"{self.config['data']['raw_path']}/{endpoint_name}_latest.parquet"
            df.to_parquet(latest_path, index=False)
            
            # Backup raw API response if configured
            backup_file = ""
            if self.config['api_ingestion'].get('backup_responses', True):
                backup_path = f"data/api_responses/{endpoint_name}_{timestamp}.json"
                backup_file = backup_path
            
            result.update({
                'status': 'success',
                'records_count': len(df),
                'output_file': output_path,
                'backup_file': backup_file
            })
            
            self.logger.info(f"Successfully saved {len(df)} records from {endpoint_name} to {output_path}")
            
        except Exception as e:
            error_msg = f"Failed to save data for {endpoint_name}: {str(e)}"
            self.logger.error(error_msg)
            result['error'] = error_msg
            
        return result
    
    def ingest_endpoint(self, endpoint_name: str, endpoint_config: Dict) -> Dict:
        """Ingest data from a single API endpoint"""
        self.logger.info(f"Starting ingestion for endpoint: {endpoint_name}")
        
        result = {
            'endpoint': endpoint_name,
            'status': 'failed',
            'records_count': 0,
            'output_file': '',
            'error': '',
            'processing_time': 0
        }
        
        start_time = time.time()
        
        try:
            # Make API request
            success, raw_data, error_msg = self.make_api_request(endpoint_name, endpoint_config)
            
            if not success:
                result['error'] = error_msg
                self.ingestion_stats['errors'].append(f"{endpoint_name}: {error_msg}")
                return result
            
            # Backup raw response
            if self.config['api_ingestion'].get('backup_responses', True):
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_path = f"data/api_responses/{endpoint_name}_{timestamp}.json"
                with open(backup_path, 'w') as f:
                    json.dump(raw_data, f, indent=2, default=str)
                self.logger.info(f"Backed up raw response to {backup_path}")
            
            # Process data
            df = self.process_api_data(endpoint_name, raw_data)
            
            if df is None:
                result['error'] = "Failed to process API response data"
                self.ingestion_stats['errors'].append(f"{endpoint_name}: Data processing failed")
                return result
            
            # Save processed data
            save_result = self.save_api_data(df, endpoint_name)
            
            if save_result['status'] == 'success':
                result.update({
                    'status': 'success',
                    'records_count': save_result['records_count'],
                    'output_file': save_result['output_file'],
                    'processing_time': time.time() - start_time
                })
                
                self.ingestion_stats['total_records'] += save_result['records_count']
                self.logger.info(f"Successfully ingested {save_result['records_count']} records from {endpoint_name}")
            else:
                result['error'] = save_result['error']
                self.ingestion_stats['errors'].append(f"{endpoint_name}: {save_result['error']}")
            
        except Exception as e:
            error_msg = f"Unexpected error during {endpoint_name} ingestion: {str(e)}"
            self.logger.error(error_msg)
            result['error'] = error_msg
            self.ingestion_stats['errors'].append(f"{endpoint_name}: {error_msg}")
        
        result['processing_time'] = time.time() - start_time
        
        # Add rate limiting delay
        time.sleep(self.config['api_ingestion'].get('rate_limit_delay', 1))
        
        return result
    
    def run_ingestion(self) -> Dict:
        """Run complete API ingestion pipeline"""
        self.logger.info("Starting API data ingestion pipeline")
        self.ingestion_stats['start_time'] = datetime.now()
        
        results = {}
        
        # Get API endpoints from config
        endpoints = self.config.get('api_endpoints', {})
        
        if not endpoints:
            self.logger.warning("No API endpoints configured")
            return {'status': 'failed', 'error': 'No API endpoints configured'}
        
        for endpoint_name, endpoint_config in endpoints.items():
            try:
                self.logger.info(f"Processing endpoint: {endpoint_name}")
                result = self.ingest_endpoint(endpoint_name, endpoint_config)
                results[endpoint_name] = result
                
                if result['status'] == 'success':
                    self.ingestion_stats['apis_processed'] += 1
                else:
                    self.ingestion_stats['apis_failed'] += 1
                
            except Exception as e:
                error_msg = f"Critical error processing {endpoint_name}: {str(e)}"
                self.logger.error(error_msg)
                results[endpoint_name] = {
                    'endpoint': endpoint_name,
                    'status': 'failed',
                    'error': error_msg
                }
                self.ingestion_stats['apis_failed'] += 1
        
        self.ingestion_stats['end_time'] = datetime.now()
        
        # Save ingestion metadata
        self._save_ingestion_metadata(results)
        
        # Print summary
        self._print_summary(results)
        
        return results
    
    def _save_ingestion_metadata(self, results: Dict):
        """Save ingestion run metadata"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            metadata = {
                'ingestion_type': 'api',
                'timestamp': timestamp,
                'start_time': self.ingestion_stats['start_time'].isoformat(),
                'end_time': self.ingestion_stats['end_time'].isoformat(),
                'duration_seconds': (self.ingestion_stats['end_time'] - self.ingestion_stats['start_time']).total_seconds(),
                'endpoints_processed': self.ingestion_stats['apis_processed'],
                'endpoints_failed': self.ingestion_stats['apis_failed'],
                'total_records': self.ingestion_stats['total_records'],
                'errors': self.ingestion_stats['errors'],
                'detailed_results': results
            }
            
            metadata_path = f"data/ingestion_metadata/api_ingestion_{timestamp}.json"
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2, default=str)
            
            self.logger.info(f"Ingestion metadata saved to {metadata_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to save ingestion metadata: {str(e)}")
    
    def _print_summary(self, results: Dict):
        """Print ingestion summary"""
        total_time = (self.ingestion_stats['end_time'] - self.ingestion_stats['start_time']).total_seconds()
        
        self.logger.info("=" * 60)
        self.logger.info("API INGESTION PIPELINE COMPLETED")
        self.logger.info(f"Total time: {total_time:.2f} seconds")
        self.logger.info(f"Endpoints processed: {self.ingestion_stats['apis_processed']}")
        self.logger.info(f"Endpoints failed: {self.ingestion_stats['apis_failed']}")
        self.logger.info(f"Total records: {self.ingestion_stats['total_records']:,}")
        self.logger.info(f"Errors encountered: {len(self.ingestion_stats['errors'])}")
        self.logger.info("=" * 60)
        
        # Print detailed results
        print("\n" + "=" * 60)
        print("API DATA INGESTION SUMMARY")
        print("=" * 60)
        
        for endpoint_name, result in results.items():
            status_icon = "✅" if result['status'] == 'success' else "❌"
            print(f"{status_icon} {endpoint_name.upper()}: {result['status']}")
            if result['status'] == 'success':
                print(f"   Records: {result['records_count']:,}")
                print(f"   Time: {result['processing_time']:.2f}s")
            else:
                print(f"   Error: {result.get('error', 'Unknown error')}")
        
        print("=" * 60)

if __name__ == "__main__":
    try:
        ingester = APIDataIngester()
        results = ingester.run_ingestion()
        
        # Exit with appropriate code
        failed_count = sum(1 for r in results.values() if r.get('status') != 'success')
        sys.exit(1 if failed_count > 0 else 0)
        
    except KeyboardInterrupt:
        print("\nIngestion interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Critical error: {str(e)}")
        sys.exit(1)