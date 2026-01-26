#!/usr/bin/env python3
"""
RecoMart Data Lake Storage Manager
This script manages the data lake storage structure with proper partitioning,
organization, and lifecycle management according to the documented storage strategy.
"""

import os
import shutil
import json
import logging
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import pandas as pd
import yaml

class DataLakeStorageManager:
    """
    Manages the data lake storage structure with partitioning and lifecycle policies
    """
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """Initialize the storage manager"""
        self.config = self._load_config(config_path)
        self.base_path = Path(self.config['data']['raw_path']).parent
        self.setup_logging()
        
        # Define storage structure
        self.storage_structure = {
            'raw': {
                'csv': ['events', 'item_properties', 'categories'],
                'api': ['products', 'categories', 'users']
            },
            'staging': {
                'csv': ['events', 'item_properties', 'categories'],
                'api': ['products', 'categories', 'users']
            },
            'processed': ['events', 'products', 'categories', 'item_properties', 'users'],
            'features': ['user_features', 'item_features', 'interaction_features', 'content_features'],
            'archive': ['csv_backups', 'api_responses'],
            'metadata': ['ingestion_logs', 'data_lineage', 'schema_registry', 'quality_reports'],
            'models': ['collaborative_filtering', 'content_based', 'hybrid']
        }
        
        self.logger.info("Storage Manager initialized successfully")
    
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file"""
        try:
            with open(config_path, 'r') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            return {
                'data': {'raw_path': 'data/raw'},
                'storage': {
                    'retention_days': {
                        'raw': 90,
                        'processed': 365,
                        'features': 180,
                        'metadata': 730
                    }
                }
            }
    
    def setup_logging(self):
        """Configure logging for storage manager"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('storage/logs/storage_manager.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def create_partitioned_path(self, base_dir: str, data_type: str, source: str, timestamp: datetime = None) -> Path:
        """
        Create a partitioned path following source, type, and timestamp partitioning scheme
        
        Args:
            base_dir: Base directory (e.g., 'raw', 'processed')
            data_type: Type of data (e.g., 'events', 'products')
            source: Source of data (e.g., 'csv_ingestion', 'api_ingestion')
            timestamp: Timestamp for partitioning (defaults to now)
        
        Returns:
            Path object for the partitioned directory
        """
        if timestamp is None:
            timestamp = datetime.now()
        
        timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")
        
        partitioned_path = self.base_path / base_dir / f"source={source}" / f"type={data_type}" / f"timestamp={timestamp_str}"
        
        return partitioned_path
    
    def initialize_storage_structure(self):
        """Initialize the complete data lake storage structure"""
        self.logger.info("Initializing data lake storage structure...")
        
        try:
            # Create main directories
            for main_dir, subdirs in self.storage_structure.items():
                main_path = self.base_path / main_dir
                main_path.mkdir(parents=True, exist_ok=True)
                
                if isinstance(subdirs, dict):
                    # Nested structure (like raw/csv, raw/api)
                    for sub_dir, types in subdirs.items():
                        for data_type in types:
                            type_path = main_path / sub_dir / data_type
                            type_path.mkdir(parents=True, exist_ok=True)
                            
                            # Create sample partitioned directories
                            sample_path = self.create_partitioned_path(f"{main_dir}/{sub_dir}", data_type)
                            sample_path.mkdir(parents=True, exist_ok=True)
                            
                elif isinstance(subdirs, list):
                    # Flat structure (like processed, features)
                    for data_type in subdirs:
                        type_path = main_path / data_type
                        type_path.mkdir(parents=True, exist_ok=True)
                        
                        # Create sample partitioned directories for time-based data
                        if main_dir in ['processed', 'features']:
                            sample_path = self.create_partitioned_path(main_dir, data_type)
                            sample_path.mkdir(parents=True, exist_ok=True)
            
            # Create logs directory
            logs_path = Path('logs')
            logs_path.mkdir(exist_ok=True)
            
            self.logger.info("✅ Storage structure initialized successfully")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to initialize storage structure: {str(e)}")
            raise
    
    def organize_data(self, data: 'pd.DataFrame', data_type: str, source: str, 
                     timestamp: datetime = None, metadata: Dict = None) -> str:
        """
        Save DataFrame with proper partitioning and organization
        
        Args:
            data: Pandas DataFrame to save
            data_type: Type of data (events, products, etc.)
            source: Source of the data (csv_ingestion, api_ingestion, etc.)
            timestamp: Timestamp for partitioning
            metadata: Additional metadata to save
            
        Returns:
            Path to saved file
        """
        try:
            if timestamp is None:
                timestamp = datetime.now()
            
            # Create partitioned path
            partitioned_path = self.create_partitioned_path("raw", data_type, source, timestamp)
            partitioned_path.mkdir(parents=True, exist_ok=True)
            
            # Generate filename with timestamp
            filename = f"{data_type}_{source}_{timestamp.strftime('%Y%m%d_%H%M%S')}.parquet"
            output_file = partitioned_path / filename
            
            # Save data as parquet
            data.to_parquet(output_file, index=False, compression='snappy')
            
            # Save metadata if provided
            if metadata:
                metadata_file = output_file.with_suffix('.metadata.json')
                with open(metadata_file, 'w') as f:
                    json.dump({
                        'file_info': {
                            'filename': filename,
                            'path': str(output_file),
                            'size_bytes': output_file.stat().st_size,
                            'created_at': timestamp.isoformat()
                        },
                        'data_info': {
                            'rows': len(data),
                            'columns': len(data.columns),
                            'data_type': data_type,
                            'source': source
                        },
                        'metadata': metadata
                    }, f, indent=2)
            
            self.logger.info(f"✅ Organized {len(data)} records to {output_file}")
            return str(output_file)
            
        except Exception as e:
            self.logger.error(f"❌ Failed to organize data: {str(e)}")
            raise
    
    def organize_file(self, source_path: str, data_type: str, source_type: str, target_dir: str = "raw") -> str:
        """
        Organize a file into the proper partitioned structure
        
        Args:
            source_path: Path to the source file
            data_type: Type of data (events, products, etc.)
            source_type: Source type (csv_ingestion, api_ingestion, etc.)
            target_dir: Target directory (raw, processed, etc.)
        
        Returns:
            Path to the organized file
        """
        source_path = Path(source_path)
        
        if not source_path.exists():
            raise FileNotFoundError(f"Source file not found: {source_path}")
        
        # Extract timestamp from filename or use current time
        timestamp = self._extract_timestamp_from_filename(source_path.name)
        if timestamp is None:
            timestamp = datetime.now()
        
        # Create partitioned target path using new scheme
        target_path = self.create_partitioned_path(target_dir, data_type, source_type, timestamp)
        target_path.mkdir(parents=True, exist_ok=True)
        
        # Generate target filename
        target_file = target_path / source_path.name
        
        # Copy or move file
        shutil.copy2(source_path, target_file)
        
        # Also create/update latest file in base directory
        latest_dir = self.base_path / target_dir
        latest_dir.mkdir(parents=True, exist_ok=True)
        latest_file = latest_dir / f"{data_type}_latest{source_path.suffix}"
        shutil.copy2(source_path, latest_file)
        
        self.logger.info(f"Organized {source_path} to {target_file}")
        return str(target_file)
    
    def _extract_timestamp_from_filename(self, filename: str) -> Optional[datetime]:
        """Extract timestamp from filename if present"""
        import re
        
        # Look for pattern like YYYYMMDD_HHMMSS
        pattern = r'(\d{8}_\d{6})'
        match = re.search(pattern, filename)
        
        if match:
            timestamp_str = match.group(1)
            try:
                return datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
            except ValueError:
                pass
        
        return None
    
    def migrate_existing_data(self):
        """Migrate existing data to the new partitioned structure"""
        self.logger.info("Starting data migration to partitioned structure...")
        
        try:
            # Define migration mappings
            migration_mappings = [
                # Raw data migrations
                {
                    'source_pattern': 'data/raw/events_*.parquet',
                    'data_type': 'events',
                    'source_type': 'csv',
                    'target_dir': 'raw'
                },
                {
                    'source_pattern': 'data/raw/item_properties_*.parquet',
                    'data_type': 'item_properties', 
                    'source_type': 'csv',
                    'target_dir': 'raw'
                },
                {
                    'source_pattern': 'data/raw/products_*.parquet',
                    'data_type': 'products',
                    'source_type': 'api',
                    'target_dir': 'raw'
                },
                {
                    'source_pattern': 'data/raw/categories_*.parquet',
                    'data_type': 'categories',
                    'source_type': 'api',
                    'target_dir': 'raw'
                },
                # Archive data migrations
                {
                    'source_pattern': 'data/archive/*.csv',
                    'data_type': 'csv_backups',
                    'source_type': 'csv',
                    'target_dir': 'archive'
                },
                {
                    'source_pattern': 'data/api_responses/*.json',
                    'data_type': 'api_responses',
                    'source_type': 'api',
                    'target_dir': 'archive'
                },
                # Metadata migrations
                {
                    'source_pattern': 'data/ingestion_metadata/*_ingestion_*.json',
                    'data_type': 'ingestion_logs',
                    'source_type': 'metadata',
                    'target_dir': 'metadata'
                }
            ]
            
            migrated_count = 0
            for mapping in migration_mappings:
                files = list(Path('.').glob(mapping['source_pattern']))
                
                for file_path in files:
                    try:
                        if mapping['target_dir'] == 'archive':
                            # Special handling for archive files
                            self._organize_archive_file(file_path, mapping['data_type'])
                        elif mapping['target_dir'] == 'metadata':
                            # Special handling for metadata files
                            self._organize_metadata_file(file_path, mapping['data_type'])
                        else:
                            # Regular data files
                            self.organize_file(
                                str(file_path),
                                mapping['data_type'],
                                mapping['source_type'],
                                mapping['target_dir']
                            )
                        migrated_count += 1
                        
                    except Exception as e:
                        self.logger.warning(f"Failed to migrate {file_path}: {str(e)}")
            
            self.logger.info(f"✅ Migration completed: {migrated_count} files migrated")
            
        except Exception as e:
            self.logger.error(f"❌ Migration failed: {str(e)}")
            raise
    
    def _organize_archive_file(self, file_path: Path, data_type: str):
        """Organize archive files with proper partitioning"""
        timestamp = self._extract_timestamp_from_filename(file_path.name) or datetime.now()
        
        target_path = self.create_partitioned_path("archive", data_type, timestamp)
        target_path.mkdir(parents=True, exist_ok=True)
        
        target_file = target_path / file_path.name
        shutil.copy2(file_path, target_file)
        
        self.logger.info(f"Archived {file_path} to {target_file}")
    
    def _organize_metadata_file(self, file_path: Path, data_type: str):
        """Organize metadata files with proper partitioning"""
        timestamp = self._extract_timestamp_from_filename(file_path.name) or datetime.now()
        
        # Determine ingestion type from filename
        ingestion_type = 'csv_ingestion' if 'csv_ingestion' in file_path.name else 'api_ingestion'
        
        target_path = self.create_partitioned_path(f"metadata/{data_type}", ingestion_type, timestamp)
        target_path.mkdir(parents=True, exist_ok=True)
        
        target_file = target_path / file_path.name
        shutil.copy2(file_path, target_file)
        
        self.logger.info(f"Organized metadata {file_path} to {target_file}")
    
    def cleanup_old_data(self, dry_run: bool = True):
        """
        Clean up old data based on retention policies
        
        Args:
            dry_run: If True, only show what would be deleted
        """
        self.logger.info(f"Starting data cleanup (dry_run={dry_run})...")
        
        retention_config = self.config.get('storage', {}).get('retention_days', {})
        
        cleanup_actions = []
        
        for data_dir, retention_days in retention_config.items():
            cutoff_date = datetime.now() - timedelta(days=retention_days)
            data_path = self.base_path / data_dir
            
            if not data_path.exists():
                continue
            
            # Find old partitioned directories
            for year_dir in data_path.rglob("year=*"):
                try:
                    # Extract date from partition path
                    year = int(year_dir.name.split("=")[1])
                    month_dirs = year_dir.glob("month=*")
                    
                    for month_dir in month_dirs:
                        month = int(month_dir.name.split("=")[1])
                        day_dirs = month_dir.glob("day=*")
                        
                        for day_dir in day_dirs:
                            day = int(day_dir.name.split("=")[1])
                            dir_date = datetime(year, month, day)
                            
                            if dir_date < cutoff_date:
                                cleanup_actions.append({
                                    'path': day_dir,
                                    'date': dir_date,
                                    'type': data_dir,
                                    'size': self._get_directory_size(day_dir)
                                })
                                
                except (ValueError, IndexError):
                    continue
        
        # Sort by date (oldest first)
        cleanup_actions.sort(key=lambda x: x['date'])
        
        if not cleanup_actions:
            self.logger.info("No data to cleanup")
            return
        
        total_size = sum(action['size'] for action in cleanup_actions)
        
        self.logger.info(f"Found {len(cleanup_actions)} directories to cleanup")
        self.logger.info(f"Total size to be freed: {total_size / 1024 / 1024:.2f} MB")
        
        if not dry_run:
            for action in cleanup_actions:
                try:
                    shutil.rmtree(action['path'])
                    self.logger.info(f"Deleted {action['path']} ({action['date']})")
                except Exception as e:
                    self.logger.error(f"Failed to delete {action['path']}: {str(e)}")
        else:
            self.logger.info("DRY RUN - Would delete:")
            for action in cleanup_actions:
                size_mb = action['size'] / 1024 / 1024
                self.logger.info(f"  {action['path']} ({action['date']}, {size_mb:.2f} MB)")
    
    def _get_directory_size(self, directory: Path) -> int:
        """Get total size of directory in bytes"""
        total_size = 0
        try:
            for file_path in directory.rglob('*'):
                if file_path.is_file():
                    total_size += file_path.stat().st_size
        except (OSError, PermissionError):
            pass
        return total_size
    
    def get_storage_statistics(self) -> Dict:
        """Get comprehensive storage statistics"""
        stats = {
            'directories': {},
            'file_counts': {},
            'total_size': 0,
            'timestamp': datetime.now().isoformat()
        }
        
        for main_dir in self.storage_structure.keys():
            dir_path = self.base_path / main_dir
            if dir_path.exists():
                dir_size = self._get_directory_size(dir_path)
                file_count = len([f for f in dir_path.rglob('*') if f.is_file()])
                
                stats['directories'][main_dir] = {
                    'size_bytes': dir_size,
                    'size_mb': dir_size / 1024 / 1024,
                    'file_count': file_count
                }
                
                stats['total_size'] += dir_size
        
        stats['total_size_mb'] = stats['total_size'] / 1024 / 1024
        
        return stats
    
    def validate_storage_structure(self) -> Dict:
        """Validate the storage structure and report issues"""
        validation_results = {
            'valid': True,
            'issues': [],
            'recommendations': []
        }
        
        try:
            # Check main directories exist
            for main_dir in self.storage_structure.keys():
                dir_path = self.base_path / main_dir
                if not dir_path.exists():
                    validation_results['issues'].append(f"Missing directory: {dir_path}")
                    validation_results['valid'] = False
            
            # Check for orphaned files in root directories
            for main_dir in ['raw', 'processed', 'features']:
                dir_path = self.base_path / main_dir
                if dir_path.exists():
                    for file in dir_path.iterdir():
                        if file.is_file():
                            validation_results['issues'].append(f"Orphaned file in {main_dir}: {file.name}")
                            validation_results['recommendations'].append(f"Move {file.name} to proper partitioned structure")
            
            # Check partition structure
            for main_dir in ['raw', 'processed', 'features']:
                dir_path = self.base_path / main_dir
                if dir_path.exists():
                    self._validate_partitions(dir_path, validation_results)
            
        except Exception as e:
            validation_results['issues'].append(f"Validation error: {str(e)}")
            validation_results['valid'] = False
        
        return validation_results
    
    def _validate_partitions(self, base_path: Path, results: Dict):
        """Validate partition structure in a directory"""
        for data_type_dir in base_path.rglob("*"):
            if data_type_dir.is_dir() and "year=" in data_type_dir.name:
                # Check if year partition has proper structure
                if not any(month_dir.name.startswith("month=") for month_dir in data_type_dir.iterdir() if month_dir.is_dir()):
                    results['issues'].append(f"Invalid partition structure: {data_type_dir}")
    
    def create_storage_report(self) -> str:
        """Create a comprehensive storage report"""
        stats = self.get_storage_statistics()
        validation = self.validate_storage_structure()
        
        report = f"""
# RecoMart Data Lake Storage Report
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Storage Statistics
Total Size: {stats['total_size_mb']:.2f} MB

### Directory Breakdown:
"""
        
        for dir_name, dir_stats in stats['directories'].items():
            report += f"- **{dir_name}**: {dir_stats['size_mb']:.2f} MB ({dir_stats['file_count']} files)\n"
        
        report += f"""
## Storage Validation
Status: {'✅ VALID' if validation['valid'] else '❌ INVALID'}

"""
        
        if validation['issues']:
            report += "### Issues Found:\n"
            for issue in validation['issues']:
                report += f"- {issue}\n"
        
        if validation['recommendations']:
            report += "\n### Recommendations:\n"
            for rec in validation['recommendations']:
                report += f"- {rec}\n"
        
        return report

def main():
    """Main entry point for storage management"""
    parser = argparse.ArgumentParser(description='RecoMart Data Lake Storage Manager')
    parser.add_argument('--config', default='config/config.yaml', help='Configuration file path')
    parser.add_argument('--init', action='store_true', help='Initialize storage structure')
    parser.add_argument('--migrate', action='store_true', help='Migrate existing data')
    parser.add_argument('--cleanup', action='store_true', help='Cleanup old data')
    parser.add_argument('--dry-run', action='store_true', help='Dry run for cleanup')
    parser.add_argument('--stats', action='store_true', help='Show storage statistics')
    parser.add_argument('--validate', action='store_true', help='Validate storage structure')
    parser.add_argument('--report', action='store_true', help='Generate storage report')
    
    args = parser.parse_args()
    
    try:
        manager = DataLakeStorageManager(args.config)
        
        if args.init:
            manager.initialize_storage_structure()
        
        if args.migrate:
            manager.migrate_existing_data()
        
        if args.cleanup:
            manager.cleanup_old_data(dry_run=args.dry_run)
        
        if args.stats:
            stats = manager.get_storage_statistics()
            print(json.dumps(stats, indent=2))
        
        if args.validate:
            validation = manager.validate_storage_structure()
            print(json.dumps(validation, indent=2))
        
        if args.report:
            report = manager.create_storage_report()
            print(report)
            
            # Save to file
            report_path = f"docs/storage_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
            Path(report_path).parent.mkdir(exist_ok=True)
            with open(report_path, 'w') as f:
                f.write(report)
            print(f"\nReport saved to: {report_path}")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())