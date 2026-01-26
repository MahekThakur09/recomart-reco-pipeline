#!/usr/bin/env python3
"""
RecoMart Data Ingestion Pipeline - Automation and Scheduling System
This script provides automated and periodic data fetching capabilities
for both CSV and API data sources with comprehensive monitoring.
"""

import schedule
import time
import logging
import sys
import threading
import signal
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Callable
import yaml
import json
from concurrent.futures import ThreadPoolExecutor
import subprocess

# Import our ingestion modules
from csv_ingester import CSVDataIngester
from api_ingester import APIDataIngester

class IngestionScheduler:
    """
    Automated scheduling system for data ingestion pipeline
    """
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """Initialize the ingestion scheduler"""
        self.config = self._load_config(config_path)
        self.setup_logging()
        self.running = False
        self.jobs_status = {}
        self.setup_signal_handlers()
        
        # Initialize ingesters
        try:
            self.csv_ingester = CSVDataIngester(config_path)
            self.api_ingester = APIDataIngester(config_path)
        except Exception as e:
            self.logger.error(f"Failed to initialize ingesters: {str(e)}")
            raise
        
        self.logger.info("Ingestion Scheduler initialized successfully")
    
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file"""
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
                
                # Add scheduling defaults if not present
                if 'scheduling' not in config:
                    config['scheduling'] = {
                        'csv_ingestion': {
                            'enabled': True,
                            'interval': '6h',  # every 6 hours
                            'start_time': '08:00'
                        },
                        'api_ingestion': {
                            'enabled': True,
                            'interval': '2h',  # every 2 hours
                            'start_time': '09:00'
                        },
                        'monitoring': {
                            'health_check_interval': '30m',
                            'cleanup_interval': '24h',
                            'log_retention_days': 30
                        }
                    }
                
                return config
        except FileNotFoundError:
            self.logger.warning(f"Config file {config_path} not found, using defaults")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict:
        """Get default configuration"""
        return {
            'logging': {
                'level': 'INFO',
                'file': 'ingestion/logs/scheduler.log',
                'max_size_mb': 10
            },
            'scheduling': {
                'csv_ingestion': {
                    'enabled': True,
                    'interval': '6h',
                    'start_time': '08:00'
                },
                'api_ingestion': {
                    'enabled': True,
                    'interval': '2h',
                    'start_time': '09:00'
                },
                'monitoring': {
                    'health_check_interval': '30m',
                    'cleanup_interval': '24h',
                    'log_retention_days': 30
                }
            }
        }
    
    def setup_logging(self):
        """Configure comprehensive logging for scheduler"""
        log_config = self.config.get('logging', {})
        log_level = getattr(logging, log_config.get('level', 'INFO'))
        log_file = log_config.get('file', 'ingestion/logs/scheduler.log')
        
        # Ensure log directory exists
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        
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
    
    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        self.logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.stop()
    
    def parse_interval(self, interval_str: str) -> int:
        """Parse interval string to minutes"""
        if interval_str.endswith('m'):
            return int(interval_str[:-1])
        elif interval_str.endswith('h'):
            return int(interval_str[:-1]) * 60
        elif interval_str.endswith('d'):
            return int(interval_str[:-1]) * 1440
        else:
            # Default to minutes
            return int(interval_str)
    
    def schedule_job(self, job_name: str, job_func: Callable, interval_str: str, start_time: Optional[str] = None):
        """Schedule a job with specified interval"""
        interval_minutes = self.parse_interval(interval_str)
        
        if start_time:
            # Schedule daily at specific time
            schedule.every().day.at(start_time).do(self._run_job_safe, job_name, job_func)
            self.logger.info(f"Scheduled {job_name} daily at {start_time}")
        
        if interval_minutes < 60:
            # Every N minutes
            schedule.every(interval_minutes).minutes.do(self._run_job_safe, job_name, job_func)
            self.logger.info(f"Scheduled {job_name} every {interval_minutes} minutes")
        elif interval_minutes < 1440:
            # Every N hours
            hours = interval_minutes // 60
            schedule.every(hours).hours.do(self._run_job_safe, job_name, job_func)
            self.logger.info(f"Scheduled {job_name} every {hours} hours")
        else:
            # Every N days
            days = interval_minutes // 1440
            schedule.every(days).days.do(self._run_job_safe, job_name, job_func)
            self.logger.info(f"Scheduled {job_name} every {days} days")
    
    def _run_job_safe(self, job_name: str, job_func: Callable):
        """Run job with error handling and status tracking"""
        self.logger.info(f"Starting scheduled job: {job_name}")
        start_time = datetime.now()
        
        # Initialize job status
        self.jobs_status[job_name] = {
            'status': 'running',
            'start_time': start_time,
            'last_run': start_time,
            'success_count': self.jobs_status.get(job_name, {}).get('success_count', 0),
            'failure_count': self.jobs_status.get(job_name, {}).get('failure_count', 0),
            'last_error': None
        }
        
        try:
            # Run the job function
            result = job_func()
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # Update status based on result
            if isinstance(result, dict):
                failed_count = sum(1 for r in result.values() if r.get('status') != 'success')
                success = failed_count == 0
            else:
                success = result is not False and result is not None
            
            if success:
                self.jobs_status[job_name].update({
                    'status': 'success',
                    'end_time': end_time,
                    'duration': duration,
                    'success_count': self.jobs_status[job_name]['success_count'] + 1,
                    'last_result': result
                })
                self.logger.info(f"Job {job_name} completed successfully in {duration:.2f}s")
            else:
                self.jobs_status[job_name].update({
                    'status': 'failed',
                    'end_time': end_time,
                    'duration': duration,
                    'failure_count': self.jobs_status[job_name]['failure_count'] + 1,
                    'last_error': 'Job function returned failure result'
                })
                self.logger.error(f"Job {job_name} failed in {duration:.2f}s")
                
        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            error_msg = str(e)
            
            self.jobs_status[job_name].update({
                'status': 'error',
                'end_time': end_time,
                'duration': duration,
                'failure_count': self.jobs_status[job_name]['failure_count'] + 1,
                'last_error': error_msg
            })
            
            self.logger.error(f"Job {job_name} failed with error in {duration:.2f}s: {error_msg}")
        
        finally:
            # Save job status
            self._save_job_status()
    
    def csv_ingestion_job(self):
        """CSV ingestion job"""
        self.logger.info("Executing CSV ingestion job")
        return self.csv_ingester.run_ingestion()
    
    def api_ingestion_job(self):
        """API ingestion job"""
        self.logger.info("Executing API ingestion job")
        return self.api_ingester.run_ingestion()
    
    def health_check_job(self):
        """Health check and monitoring job"""
        self.logger.info("Executing health check job")
        
        try:
            # Check disk space
            raw_data_path = Path(self.config['data']['raw_path'])
            if raw_data_path.exists():
                # Simple disk usage check
                total_size = sum(f.stat().st_size for f in raw_data_path.rglob('*') if f.is_file())
                self.logger.info(f"Raw data directory size: {total_size / 1024 / 1024:.2f} MB")
            
            # Check recent ingestion status
            recent_jobs = {name: status for name, status in self.jobs_status.items() 
                          if status.get('last_run') and 
                          (datetime.now() - status['last_run']).total_seconds() < 3600}
            
            if recent_jobs:
                self.logger.info(f"Recent jobs status: {len(recent_jobs)} jobs executed in last hour")
                for job_name, status in recent_jobs.items():
                    self.logger.info(f"  {job_name}: {status.get('status', 'unknown')}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Health check failed: {str(e)}")
            return False
    
    def cleanup_job(self):
        """Cleanup old logs and temporary files"""
        self.logger.info("Executing cleanup job")
        
        try:
            retention_days = self.config['scheduling']['monitoring'].get('log_retention_days', 30)
            cutoff_date = datetime.now() - timedelta(days=retention_days)
            
            # Cleanup old log files
            logs_dir = Path('logs')
            if logs_dir.exists():
                old_logs = [f for f in logs_dir.glob('*.log.*') 
                           if f.stat().st_mtime < cutoff_date.timestamp()]
                
                for log_file in old_logs:
                    try:
                        log_file.unlink()
                        self.logger.info(f"Removed old log file: {log_file}")
                    except Exception as e:
                        self.logger.warning(f"Failed to remove {log_file}: {str(e)}")
            
            # Cleanup old metadata files
            metadata_dir = Path('data/ingestion_metadata')
            if metadata_dir.exists():
                old_metadata = [f for f in metadata_dir.glob('*.json') 
                               if f.stat().st_mtime < cutoff_date.timestamp()]
                
                for metadata_file in old_metadata[:10]:  # Keep at least recent files
                    try:
                        metadata_file.unlink()
                        self.logger.info(f"Removed old metadata file: {metadata_file}")
                    except Exception as e:
                        self.logger.warning(f"Failed to remove {metadata_file}: {str(e)}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Cleanup job failed: {str(e)}")
            return False
    
    def setup_schedules(self):
        """Setup all scheduled jobs based on configuration"""
        schedule_config = self.config.get('scheduling', {})
        
        # CSV ingestion
        if schedule_config.get('csv_ingestion', {}).get('enabled', True):
            csv_config = schedule_config['csv_ingestion']
            self.schedule_job(
                'csv_ingestion',
                self.csv_ingestion_job,
                csv_config.get('interval', '6h'),
                csv_config.get('start_time')
            )
        
        # API ingestion
        if schedule_config.get('api_ingestion', {}).get('enabled', True):
            api_config = schedule_config['api_ingestion']
            self.schedule_job(
                'api_ingestion',
                self.api_ingestion_job,
                api_config.get('interval', '2h'),
                api_config.get('start_time')
            )
        
        # Monitoring jobs
        monitoring_config = schedule_config.get('monitoring', {})
        
        self.schedule_job(
            'health_check',
            self.health_check_job,
            monitoring_config.get('health_check_interval', '30m')
        )
        
        self.schedule_job(
            'cleanup',
            self.cleanup_job,
            monitoring_config.get('cleanup_interval', '24h')
        )
        
        self.logger.info("All scheduled jobs configured successfully")
    
    def _save_job_status(self):
        """Save current job status to file"""
        try:
            status_file = 'ingestion/logs/scheduler_status.json'
            with open(status_file, 'w') as f:
                json.dump(self.jobs_status, f, indent=2, default=str)
        except Exception as e:
            self.logger.warning(f"Failed to save job status: {str(e)}")
    
    def get_status(self) -> Dict:
        """Get current scheduler status"""
        return {
            'running': self.running,
            'scheduled_jobs': len(schedule.jobs),
            'job_status': self.jobs_status,
            'next_run': str(min([job.next_run for job in schedule.jobs], default='No jobs scheduled'))
        }
    
    def run_once(self, job_name: Optional[str] = None):
        """Run ingestion jobs once (for testing)"""
        if job_name:
            if job_name == 'csv':
                return self.csv_ingestion_job()
            elif job_name == 'api':
                return self.api_ingestion_job()
            elif job_name == 'health':
                return self.health_check_job()
            elif job_name == 'cleanup':
                return self.cleanup_job()
            else:
                self.logger.error(f"Unknown job name: {job_name}")
                return False
        else:
            # Run all ingestion jobs
            self.logger.info("Running all ingestion jobs once")
            csv_result = self.csv_ingestion_job()
            api_result = self.api_ingestion_job()
            return {'csv': csv_result, 'api': api_result}
    
    def start(self):
        """Start the scheduler"""
        if self.running:
            self.logger.warning("Scheduler is already running")
            return
        
        self.logger.info("Starting ingestion scheduler...")
        self.running = True
        
        # Setup all scheduled jobs
        self.setup_schedules()
        
        self.logger.info("Scheduler started. Waiting for scheduled jobs...")
        self.logger.info(f"Total scheduled jobs: {len(schedule.jobs)}")
        
        try:
            while self.running:
                schedule.run_pending()
                time.sleep(30)  # Check every 30 seconds
                
        except KeyboardInterrupt:
            self.logger.info("Scheduler interrupted by user")
        finally:
            self.stop()
    
    def stop(self):
        """Stop the scheduler"""
        if not self.running:
            return
        
        self.logger.info("Stopping scheduler...")
        self.running = False
        schedule.clear()
        self._save_job_status()
        self.logger.info("Scheduler stopped")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='RecoMart Data Ingestion Scheduler')
    parser.add_argument('--config', default='config/config.yaml', help='Configuration file path')
    parser.add_argument('--run-once', choices=['csv', 'api', 'health', 'cleanup', 'all'], 
                       help='Run a specific job once and exit')
    parser.add_argument('--status', action='store_true', help='Show scheduler status')
    parser.add_argument('--daemon', action='store_true', help='Run as daemon process')
    
    args = parser.parse_args()
    
    try:
        scheduler = IngestionScheduler(args.config)
        
        if args.status:
            status = scheduler.get_status()
            print(json.dumps(status, indent=2, default=str))
            
        elif args.run_once:
            if args.run_once == 'all':
                result = scheduler.run_once()
            else:
                result = scheduler.run_once(args.run_once)
            
            print(f"Job execution result: {result}")
            sys.exit(0 if result else 1)
            
        else:
            # Start the scheduler
            scheduler.start()
            
    except KeyboardInterrupt:
        print("\nScheduler interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"Critical error: {str(e)}")
        sys.exit(1)