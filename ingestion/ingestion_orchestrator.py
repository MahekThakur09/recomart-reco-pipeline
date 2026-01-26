#!/usr/bin/env python3
"""
RecoMart Data Ingestion Pipeline - Main Orchestrator
This script serves as the central orchestrator for all data ingestion processes,
coordinating CSV and API ingestion with comprehensive error handling and monitoring.
"""

import argparse
import sys
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import yaml
import logging

# Import our ingestion modules
from csv_ingester import CSVDataIngester
from api_ingester import APIDataIngester
from scheduler import IngestionScheduler

class IngestionOrchestrator:
    """
    Main orchestrator for the RecoMart data ingestion pipeline
    """
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """Initialize the ingestion orchestrator"""
        self.config_path = config_path
        self.config = self._load_config()
        self.setup_logging()
        
        # Initialize components
        self.csv_ingester = None
        self.api_ingester = None
        self.scheduler = None
        
        self.logger.info("Ingestion Orchestrator initialized")
    
    def _load_config(self) -> Dict:
        """Load configuration from YAML file"""
        try:
            with open(self.config_path, 'r') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            self.logger.error(f"Configuration file {self.config_path} not found")
            return {}
    
    def setup_logging(self):
        """Configure logging for the orchestrator"""
        log_config = self.config.get('logging', {})
        log_level = getattr(logging, log_config.get('level', 'INFO'))
        
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
        )
        
        # Setup console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(log_level)
        
        # Configure logger
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.logger.setLevel(log_level)
        self.logger.addHandler(console_handler)
        self.logger.propagate = False
    
    def initialize_ingesters(self):
        """Initialize ingestion components"""
        try:
            self.logger.info("Initializing ingestion components...")
            
            self.csv_ingester = CSVDataIngester(self.config_path)
            self.logger.info("✅ CSV ingester initialized")
            
            self.api_ingester = APIDataIngester(self.config_path)
            self.logger.info("✅ API ingester initialized")
            
            self.scheduler = IngestionScheduler(self.config_path)
            self.logger.info("✅ Scheduler initialized")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize ingesters: {str(e)}")
            return False
    
    def run_csv_ingestion(self) -> Tuple[bool, Dict]:
        """Run CSV ingestion process"""
        self.logger.info("Starting CSV data ingestion...")
        
        try:
            if not self.csv_ingester:
                self.csv_ingester = CSVDataIngester(self.config_path)
            
            results = self.csv_ingester.run_full_csv_ingestion()
            
            # Check if ingestion was successful
            success = True
            if isinstance(results, dict):
                for source, result in results.items():
                    if result.get('status') != 'success':
                        success = False
                        break
            
            if success:
                self.logger.info("✅ CSV ingestion completed successfully")
            else:
                self.logger.error("❌ CSV ingestion completed with errors")
            
            return success, results
            
        except Exception as e:
            error_msg = f"CSV ingestion failed: {str(e)}"
            self.logger.error(error_msg)
            return False, {'error': error_msg}
    
    def run_api_ingestion(self) -> Tuple[bool, Dict]:
        """Run API ingestion process"""
        self.logger.info("Starting API data ingestion...")
        
        try:
            if not self.api_ingester:
                self.api_ingester = APIDataIngester(self.config_path)
            
            results = self.api_ingester.run_ingestion()
            
            # Check if ingestion was successful
            success = True
            if isinstance(results, dict):
                for endpoint, result in results.items():
                    if result.get('status') != 'success':
                        success = False
                        break
            
            if success:
                self.logger.info("✅ API ingestion completed successfully")
            else:
                self.logger.error("❌ API ingestion completed with errors")
            
            return success, results
            
        except Exception as e:
            error_msg = f"API ingestion failed: {str(e)}"
            self.logger.error(error_msg)
            return False, {'error': error_msg}
    
    def run_full_ingestion(self) -> Dict:
        """Run complete ingestion pipeline (CSV + API)"""
        self.logger.info("=" * 60)
        self.logger.info("STARTING FULL INGESTION PIPELINE")
        self.logger.info("=" * 60)
        
        start_time = datetime.now()
        pipeline_results = {
            'start_time': start_time.isoformat(),
            'csv_ingestion': {},
            'api_ingestion': {},
            'overall_status': 'failed',
            'total_duration': 0,
            'summary': {}
        }
        
        # Initialize components
        if not self.initialize_ingesters():
            pipeline_results['error'] = "Failed to initialize ingestion components"
            return pipeline_results
        
        # Run CSV ingestion
        csv_success, csv_results = self.run_csv_ingestion()
        pipeline_results['csv_ingestion'] = {
            'success': csv_success,
            'results': csv_results
        }
        
        # Add delay between ingestions
        self.logger.info("Waiting 10 seconds before API ingestion...")
        time.sleep(10)
        
        # Run API ingestion
        api_success, api_results = self.run_api_ingestion()
        pipeline_results['api_ingestion'] = {
            'success': api_success,
            'results': api_results
        }
        
        # Calculate overall status
        end_time = datetime.now()
        total_duration = (end_time - start_time).total_seconds()
        
        overall_success = csv_success and api_success
        pipeline_results.update({
            'end_time': end_time.isoformat(),
            'total_duration': total_duration,
            'overall_status': 'success' if overall_success else 'failed'
        })
        
        # Generate summary
        summary = self._generate_pipeline_summary(pipeline_results)
        pipeline_results['summary'] = summary
        
        # Save pipeline metadata
        self._save_pipeline_metadata(pipeline_results)
        
        # Print final summary
        self._print_pipeline_summary(pipeline_results)
        
        return pipeline_results
    
    def _generate_pipeline_summary(self, results: Dict) -> Dict:
        """Generate pipeline execution summary"""
        summary = {
            'total_sources': 0,
            'successful_sources': 0,
            'failed_sources': 0,
            'total_records': 0,
            'csv_records': 0,
            'api_records': 0,
            'errors': []
        }
        
        # Process CSV results
        csv_results = results['csv_ingestion'].get('results', {})
        if results['csv_ingestion'].get('success', False):
            summary['successful_sources'] += 1
            if 'events' in csv_results:
                summary['csv_records'] += csv_results['events'].get('records_count', 0)
            if 'item_properties' in csv_results:
                summary['csv_records'] += csv_results['item_properties'].get('records_count', 0)
        else:
            summary['failed_sources'] += 1
            if isinstance(csv_results, dict) and 'error' in csv_results:
                summary['errors'].append(f"CSV: {csv_results['error']}")
        
        summary['total_sources'] += 1
        
        # Process API results
        api_results = results['api_ingestion'].get('results', {})
        if results['api_ingestion'].get('success', False):
            summary['successful_sources'] += 1
            for endpoint, result in api_results.items():
                if result.get('status') == 'success':
                    summary['api_records'] += result.get('records_count', 0)
        else:
            summary['failed_sources'] += 1
            if isinstance(api_results, dict):
                if 'error' in api_results:
                    summary['errors'].append(f"API: {api_results['error']}")
                else:
                    for endpoint, result in api_results.items():
                        if result.get('status') != 'success':
                            summary['errors'].append(f"API ({endpoint}): {result.get('error', 'Unknown error')}")
        
        summary['total_sources'] += 1
        summary['total_records'] = summary['csv_records'] + summary['api_records']
        
        return summary
    
    def _save_pipeline_metadata(self, results: Dict):
        """Save pipeline execution metadata"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            metadata_path = f"data/ingestion_metadata/pipeline_execution_{timestamp}.json"
            
            # Ensure directory exists
            Path(metadata_path).parent.mkdir(parents=True, exist_ok=True)
            
            with open(metadata_path, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            
            self.logger.info(f"Pipeline metadata saved to {metadata_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to save pipeline metadata: {str(e)}")
    
    def _print_pipeline_summary(self, results: Dict):
        """Print comprehensive pipeline summary"""
        summary = results['summary']
        duration = results['total_duration']
        status = results['overall_status']
        
        self.logger.info("=" * 60)
        self.logger.info("INGESTION PIPELINE EXECUTION SUMMARY")
        self.logger.info("=" * 60)
        self.logger.info(f"Overall Status: {'✅ SUCCESS' if status == 'success' else '❌ FAILED'}")
        self.logger.info(f"Total Duration: {duration:.2f} seconds")
        self.logger.info(f"Sources Processed: {summary['total_sources']}")
        self.logger.info(f"Successful Sources: {summary['successful_sources']}")
        self.logger.info(f"Failed Sources: {summary['failed_sources']}")
        self.logger.info(f"Total Records Ingested: {summary['total_records']:,}")
        self.logger.info(f"  - CSV Records: {summary['csv_records']:,}")
        self.logger.info(f"  - API Records: {summary['api_records']:,}")
        
        if summary['errors']:
            self.logger.info("Errors Encountered:")
            for error in summary['errors']:
                self.logger.info(f"  - {error}")
        
        self.logger.info("=" * 60)
    
    def start_scheduler(self):
        """Start the automated scheduler"""
        self.logger.info("Starting automated ingestion scheduler...")
        
        try:
            if not self.scheduler:
                self.scheduler = IngestionScheduler(self.config_path)
            
            self.scheduler.start()
            
        except Exception as e:
            self.logger.error(f"Scheduler failed: {str(e)}")
            return False
        
        return True
    
    def get_status(self) -> Dict:
        """Get current pipeline status"""
        status = {
            'timestamp': datetime.now().isoformat(),
            'components': {
                'csv_ingester': self.csv_ingester is not None,
                'api_ingester': self.api_ingester is not None,
                'scheduler': self.scheduler is not None
            }
        }
        
        # Get scheduler status if available
        if self.scheduler:
            scheduler_status = self.scheduler.get_status()
            status['scheduler_details'] = scheduler_status
        
        # Check data directories
        data_dirs = {}
        for dir_name, dir_path in self.config.get('data', {}).items():
            path_obj = Path(dir_path)
            data_dirs[dir_name] = {
                'exists': path_obj.exists(),
                'file_count': len(list(path_obj.glob('*'))) if path_obj.exists() else 0
            }
        
        status['data_directories'] = data_dirs
        
        return status

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='RecoMart Data Ingestion Pipeline Orchestrator',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python ingestion_orchestrator.py --run csv           # Run only CSV ingestion
  python ingestion_orchestrator.py --run api           # Run only API ingestion  
  python ingestion_orchestrator.py --run full          # Run complete pipeline
  python ingestion_orchestrator.py --schedule          # Start automated scheduler
  python ingestion_orchestrator.py --status            # Show pipeline status
        """
    )
    
    parser.add_argument('--config', default='config/config.yaml',
                       help='Configuration file path (default: config/config.yaml)')
    parser.add_argument('--run', choices=['csv', 'api', 'full'],
                       help='Run specific ingestion process')
    parser.add_argument('--schedule', action='store_true',
                       help='Start automated scheduler')
    parser.add_argument('--status', action='store_true',
                       help='Show pipeline status')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    try:
        # Initialize orchestrator
        orchestrator = IngestionOrchestrator(args.config)
        
        if args.status:
            # Show status
            status = orchestrator.get_status()
            print(json.dumps(status, indent=2, default=str))
            return 0
        
        elif args.schedule:
            # Start scheduler
            success = orchestrator.start_scheduler()
            return 0 if success else 1
        
        elif args.run:
            # Run specific ingestion
            if args.run == 'csv':
                success, results = orchestrator.run_csv_ingestion()
            elif args.run == 'api':
                success, results = orchestrator.run_api_ingestion()
            elif args.run == 'full':
                results = orchestrator.run_full_ingestion()
                success = results['overall_status'] == 'success'
            
            return 0 if success else 1
        
        else:
            parser.print_help()
            return 1
    
    except KeyboardInterrupt:
        print("\nOperation interrupted by user")
        return 0
    except Exception as e:
        print(f"Critical error: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main())