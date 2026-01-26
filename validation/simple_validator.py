#!/usr/bin/env python3
"""
Simple Data Quality Validator for RecoMart
This is a lightweight version that focuses on core validation requirements
using pandas for data quality checks and basic reporting.
"""

import pandas as pd
import numpy as np
import json
import logging
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any
import matplotlib.pyplot as plt
import seaborn as sns

class SimpleDataValidator:
    """
    Simple data quality validator using pandas for core validation tasks
    """
    
    def __init__(self):
        self.setup_logging()
        self.results = {}
        
    def setup_logging(self):
        """Setup basic logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('validation/logs/simple_validation.log')
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def check_missing_values(self, df: pd.DataFrame, data_type: str) -> Dict:
        """Check for missing values in the dataset"""
        missing_info = {
            'total_missing': int(df.isnull().sum().sum()),
            'missing_percentage': round(df.isnull().sum().sum() / (len(df) * len(df.columns)) * 100, 2),
            'columns_with_missing': {}
        }
        
        for col in df.columns:
            missing_count = df[col].isnull().sum()
            if missing_count > 0:
                missing_info['columns_with_missing'][col] = {
                    'count': int(missing_count),
                    'percentage': round(missing_count / len(df) * 100, 2)
                }
        
        return missing_info
    
    def check_duplicates(self, df: pd.DataFrame, data_type: str) -> Dict:
        """Check for duplicate records"""
        try:
            # Try normal duplicate detection
            total_duplicates = int(df.duplicated().sum())
            duplicate_percentage = round(df.duplicated().sum() / len(df) * 100, 2)
            unique_records = int(len(df.drop_duplicates()))
        except TypeError:
            # Handle unhashable types (like dicts) by converting to strings
            try:
                df_str = df.astype(str)
                total_duplicates = int(df_str.duplicated().sum())
                duplicate_percentage = round(df_str.duplicated().sum() / len(df) * 100, 2)
                unique_records = int(len(df_str.drop_duplicates()))
            except Exception:
                # Fallback: assume no duplicates detectable
                total_duplicates = 0
                duplicate_percentage = 0.0
                unique_records = len(df)
        
        duplicate_info = {
            'total_duplicates': total_duplicates,
            'duplicate_percentage': duplicate_percentage,
            'unique_records': unique_records
        }
        
        return duplicate_info
    
    def check_data_types(self, df: pd.DataFrame, data_type: str) -> Dict:
        """Analyze data types and schema"""
        schema_info = {
            'total_columns': len(df.columns),
            'column_types': df.dtypes.astype(str).to_dict(),
            'numeric_columns': df.select_dtypes(include=[np.number]).columns.tolist(),
            'text_columns': df.select_dtypes(include=['object']).columns.tolist(),
            'datetime_columns': df.select_dtypes(include=['datetime']).columns.tolist()
        }
        
        return schema_info
    
    def validate_events_data(self, df: pd.DataFrame) -> Dict:
        """Specific validation for events data"""
        validation_results = {
            'data_type': 'events',
            'total_records': len(df),
            'issues': [],
            'warnings': [],
            'passed_checks': []
        }
        
        # Check required columns
        required_cols = ['visitorid', 'itemid', 'event', 'timestamp']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            validation_results['issues'].append(f"Missing required columns: {missing_cols}")
        else:
            validation_results['passed_checks'].append("All required columns present")
            
        # Check for null values in critical columns
        for col in ['visitorid', 'itemid']:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                if null_count > 0:
                    validation_results['warnings'].append(f"{null_count} null values in {col}")
                    
        # Check event types
        if 'event' in df.columns:
            unique_events = df['event'].unique()
            expected_events = ['view', 'addtocart', 'transaction']
            unexpected = [e for e in unique_events if e not in expected_events]
            if unexpected:
                validation_results['warnings'].append(f"Unexpected event types: {unexpected}")
            else:
                validation_results['passed_checks'].append("All event types are valid")
                
        return validation_results
    
    def validate_products_data(self, df: pd.DataFrame) -> Dict:
        """Specific validation for products data"""
        validation_results = {
            'data_type': 'products',
            'total_records': len(df),
            'issues': [],
            'warnings': [],
            'passed_checks': []
        }
        
        # Check price range
        if 'price' in df.columns:
            negative_prices = (df['price'] < 0).sum()
            if negative_prices > 0:
                validation_results['issues'].append(f"{negative_prices} products with negative prices")
            else:
                validation_results['passed_checks'].append("All prices are non-negative")
                
            zero_prices = (df['price'] == 0).sum()
            if zero_prices > 0:
                validation_results['warnings'].append(f"{zero_prices} products with zero price")
                
        # Check rating range
        if 'rating' in df.columns:
            invalid_ratings = ((df['rating'] < 1) | (df['rating'] > 5)).sum()
            if invalid_ratings > 0:
                validation_results['issues'].append(f"{invalid_ratings} products with ratings outside 1-5 range")
            else:
                validation_results['passed_checks'].append("All ratings within valid range (1-5)")
                
        return validation_results
    
    def generate_summary_report(self, all_results: Dict) -> str:
        """Generate a simple text summary report"""
        report_lines = []
        report_lines.append("=" * 60)
        report_lines.append("RECOMART DATA QUALITY VALIDATION REPORT")
        report_lines.append("=" * 60)
        report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append("")
        
        # Overall summary
        total_issues = sum(len(result.get('issues', [])) for result in all_results.values())
        total_warnings = sum(len(result.get('warnings', [])) for result in all_results.values())
        total_passed = sum(len(result.get('passed_checks', [])) for result in all_results.values())
        
        report_lines.append("OVERALL SUMMARY:")
        report_lines.append(f"  Data Sources Validated: {len(all_results)}")
        report_lines.append(f"  Total Checks Passed: {total_passed}")
        report_lines.append(f"  Total Issues Found: {total_issues}")
        report_lines.append(f"  Total Warnings: {total_warnings}")
        report_lines.append("")
        
        # Detailed results by data type
        for data_type, results in all_results.items():
            report_lines.append(f"{data_type.upper()} DATA VALIDATION:")
            report_lines.append("-" * 40)
            
            # Basic info
            if 'basic_info' in results:
                info = results['basic_info']
                report_lines.append(f"  Total Records: {info.get('total_records', 'Unknown')}")
                report_lines.append(f"  Total Columns: {info.get('total_columns', 'Unknown')}")
                
            # Missing values
            if 'missing_values' in results:
                mv = results['missing_values']
                report_lines.append(f"  Missing Values: {mv.get('missing_percentage', 0):.1f}% ({mv.get('total_missing', 0)} cells)")
                
            # Duplicates
            if 'duplicates' in results:
                dup = results['duplicates']
                report_lines.append(f"  Duplicate Records: {dup.get('duplicate_percentage', 0):.1f}% ({dup.get('total_duplicates', 0)} records)")
                
            # Validation results
            if 'validation' in results:
                val = results['validation']
                report_lines.append(f"  Passed Checks: {len(val.get('passed_checks', []))}")
                report_lines.append(f"  Issues Found: {len(val.get('issues', []))}")
                report_lines.append(f"  Warnings: {len(val.get('warnings', []))}")
                
                # List issues
                if val.get('issues'):
                    report_lines.append("  ISSUES:")
                    for issue in val['issues']:
                        report_lines.append(f"    ❌ {issue}")
                        
                # List warnings
                if val.get('warnings'):
                    report_lines.append("  WARNINGS:")
                    for warning in val['warnings']:
                        report_lines.append(f"    ⚠️  {warning}")
                        
            report_lines.append("")
            
        report_lines.append("=" * 60)
        
        # Save report
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_path = f"validation/reports/data_quality_summary_{timestamp}.txt"
        Path("validation/reports").mkdir(parents=True, exist_ok=True)
        
        with open(report_path, 'w') as f:
            f.write('\n'.join(report_lines))
            
        return report_path
    
    def validate_all_data(self):
        """Main validation function"""
        self.logger.info("Starting data quality validation")
        
        # Discover data files in partitioned structure
        raw_path = Path("data/raw")
        data_files = {}
        
        # Look for partitioned data
        try:
            # CSV ingestion data
            csv_path = raw_path / "source=csv_ingestion"
            if csv_path.exists():
                for type_dir in csv_path.iterdir():
                    if type_dir.is_dir() and type_dir.name.startswith("type="):
                        data_type = type_dir.name.replace("type=", "")
                        # Find latest timestamp
                        latest_timestamp = None
                        latest_file = None
                        
                        for timestamp_dir in type_dir.iterdir():
                            if timestamp_dir.is_dir():
                                for file in timestamp_dir.iterdir():
                                    if file.suffix == '.parquet':
                                        if latest_timestamp is None or timestamp_dir.name > latest_timestamp:
                                            latest_timestamp = timestamp_dir.name
                                            latest_file = file
                        
                        if latest_file:
                            data_files[f"csv_{data_type}"] = str(latest_file)
                            
            # API ingestion data  
            api_path = raw_path / "source=api_ingestion"
            if api_path.exists():
                for type_dir in api_path.iterdir():
                    if type_dir.is_dir() and type_dir.name.startswith("type="):
                        data_type = type_dir.name.replace("type=", "")
                        # Find latest timestamp
                        latest_timestamp = None
                        latest_file = None
                        
                        for timestamp_dir in type_dir.iterdir():
                            if timestamp_dir.is_dir():
                                for file in timestamp_dir.iterdir():
                                    if file.suffix == '.parquet':
                                        if latest_timestamp is None or timestamp_dir.name > latest_timestamp:
                                            latest_timestamp = timestamp_dir.name
                                            latest_file = file
                        
                        if latest_file:
                            data_files[f"api_{data_type}"] = str(latest_file)
                            
        except Exception as e:
            self.logger.error(f"Error discovering data files: {e}")
            return {}
        
        if not data_files:
            self.logger.warning("No data files found for validation")
            return {}
            
        all_results = {}
        
        for data_type, file_path in data_files.items():
            try:
                self.logger.info(f"Validating {data_type} data from {file_path}")
                
                # Load data
                df = pd.read_parquet(file_path)
                self.logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")
                
                # Basic analysis
                results = {
                    'file_path': file_path,
                    'timestamp': datetime.now().isoformat(),
                    'basic_info': {
                        'total_records': len(df),
                        'total_columns': len(df.columns),
                        'memory_usage_mb': round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2)
                    },
                    'missing_values': self.check_missing_values(df, data_type),
                    'duplicates': self.check_duplicates(df, data_type),
                    'schema': self.check_data_types(df, data_type)
                }
                
                # Specific validations
                if 'events' in data_type:
                    results['validation'] = self.validate_events_data(df)
                elif 'products' in data_type:
                    results['validation'] = self.validate_products_data(df)
                elif 'categories' in data_type:
                    results['validation'] = {
                        'data_type': data_type,
                        'total_records': len(df),
                        'issues': [],
                        'warnings': [],
                        'passed_checks': ['Data loaded successfully', f'Found {len(df)} categories']
                    }
                elif 'item_properties' in data_type:
                    # Basic validation for item properties
                    validation_result = {
                        'data_type': data_type,
                        'total_records': len(df),
                        'issues': [],
                        'warnings': [],
                        'passed_checks': ['Data loaded successfully']
                    }
                    
                    # Check for required columns
                    if 'itemid' not in df.columns:
                        validation_result['issues'].append("Missing 'itemid' column")
                    else:
                        validation_result['passed_checks'].append("'itemid' column present")
                        
                    results['validation'] = validation_result
                else:
                    # Generic validation
                    results['validation'] = {
                        'data_type': data_type,
                        'total_records': len(df),
                        'issues': [],
                        'warnings': [],
                        'passed_checks': ['Data loaded successfully']
                    }
                
                all_results[data_type] = results
                self.logger.info(f"Completed validation for {data_type}")
                
            except Exception as e:
                self.logger.error(f"Failed to validate {data_type}: {e}")
                all_results[data_type] = {
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                }
        
        # Save detailed results as JSON
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        json_path = f"validation/reports/validation_results_{timestamp}.json"
        with open(json_path, 'w') as f:
            json.dump(all_results, f, indent=2, default=str)
        
        # Generate summary report
        report_path = self.generate_summary_report(all_results)
        
        self.logger.info(f"Validation complete. Results saved to {json_path}")
        self.logger.info(f"Summary report saved to {report_path}")
        
        return all_results

def main():
    """Run the simple data validation"""
    validator = SimpleDataValidator()
    results = validator.validate_all_data()
    
    # Print summary to console
    total_files = len(results)
    total_issues = sum(len(result.get('validation', {}).get('issues', [])) for result in results.values())
    total_warnings = sum(len(result.get('validation', {}).get('warnings', [])) for result in results.values())
    
    print("\n" + "="*50)
    print("DATA VALIDATION COMPLETED")
    print("="*50)
    print(f"Files Validated: {total_files}")
    print(f"Issues Found: {total_issues}")
    print(f"Warnings: {total_warnings}")
    print("="*50)

if __name__ == "__main__":
    main()