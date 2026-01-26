# RecoMart Data Lake Storage Structure Documentation

## Overview
This document describes the data lake storage structure for the RecoMart recommendation system pipeline. The storage follows a structured approach with partitioning by source, type, and timestamp as per assignment requirements to ensure efficient data organization and retrieval.

## Storage Architecture

### Directory Structure
```
data/
├── raw/                          # Raw ingested data (landing zone)
│   ├── source=csv_ingestion/     # CSV ingestion source partition
│   │   ├── type=events/          # User interaction events type
│   │   │   ├── timestamp=20260126_140937/
│   │   │   │   ├── events_csv_ingestion_20260126_140937.parquet
│   │   │   │   └── events_csv_ingestion_20260126_140937.metadata.json
│   │   │   └── timestamp=20260127_091234/
│   │   │       ├── events_csv_ingestion_20260127_091234.parquet
│   │   │       └── events_csv_ingestion_20260127_091234.metadata.json
│   │   └── type=item_properties/ # Product metadata type
│   │       ├── timestamp=20260126_141011/
│   │       │   ├── item_properties_csv_ingestion_20260126_141011.parquet
│   │       │   └── item_properties_csv_ingestion_20260126_141011.metadata.json
│   │       └── timestamp=20260127_092145/
│   │           ├── item_properties_csv_ingestion_20260127_092145.parquet
│   │           └── item_properties_csv_ingestion_20260127_092145.metadata.json
│   ├── source=api_ingestion/     # API ingestion source partition
│   │   ├── type=products/        # Product catalog type
│   │   │   ├── timestamp=20260126_141053/
│   │   │   │   ├── products_api_ingestion_20260126_141053.parquet
│   │   │   │   └── products_api_ingestion_20260126_141053.metadata.json
│   │   │   └── timestamp=20260127_093045/
│   │   │       ├── products_api_ingestion_20260127_093045.parquet
│   │   │       └── products_api_ingestion_20260127_093045.metadata.json
│   │   └── type=categories/      # Product categories type
│   │       ├── timestamp=20260126_141054/
│   │       │   ├── categories_api_ingestion_20260126_141054.parquet
│   │       │   └── categories_api_ingestion_20260126_141054.metadata.json
│   │       └── timestamp=20260127_093046/
│   │           ├── categories_api_ingestion_20260127_093046.parquet
│   │           └── categories_api_ingestion_20260127_093046.metadata.json
│   ├── events_latest.parquet     # Latest files for quick access
│   ├── item_properties_latest.parquet
│   ├── products_latest.parquet
│   └── categories_latest.parquet
├── staging/                      # Temporary processing area
│   └── source=temp_processing/
│       └── type=intermediate/
│           └── timestamp=20260126_140000/
├── processed/                    # Cleaned and validated data
│   ├── source=data_processing/
│   │   ├── type=clean_events/
│   │   │   └── timestamp=20260126_150000/
│   │   │       ├── clean_events_data_processing_20260126_150000.parquet
│   │   │       └── clean_events_data_processing_20260126_150000.metadata.json
│   │   ├── type=clean_products/
│   │   │   └── timestamp=20260126_150100/
│   │   │       ├── clean_products_data_processing_20260126_150100.parquet
│   │   │       └── clean_products_data_processing_20260126_150100.metadata.json
│   │   └── type=clean_categories/
│   │       └── timestamp=20260126_150200/
│   │           ├── clean_categories_data_processing_20260126_150200.parquet
│   │           └── clean_categories_data_processing_20260126_150200.metadata.json
│   └── source=feature_engineering/
│       ├── type=user_features/
│       │   └── timestamp=20260126_151000/
│       │       ├── user_features_feature_engineering_20260126_151000.parquet
│       │       └── user_features_feature_engineering_20260126_151000.metadata.json
│       ├── type=item_features/
│       │   └── timestamp=20260126_151100/
│       │       ├── item_features_feature_engineering_20260126_151100.parquet
│       │       └── item_features_feature_engineering_20260126_151100.metadata.json
│       └── type=interaction_features/
│           └── timestamp=20260126_151200/
│               ├── interaction_features_feature_engineering_20260126_151200.parquet
│               └── interaction_features_feature_engineering_20260126_151200.metadata.json
├── features/                     # Feature store organized data
│   ├── source=feature_store/
│   │   ├── type=user_features/
│   │   │   └── timestamp=20260126_152000/
│   │   ├── type=item_features/
│   │   │   └── timestamp=20260126_152100/
│   │   └── type=interaction_features/
│   │       └── timestamp=20260126_152200/
├── archive/                      # Backup and historical data
│   ├── events_20260126_140937.csv
│   ├── item_properties_part1_20260126_141011.csv
│   ├── item_properties_part2_20260126_141011.csv
│   └── backup_logs/
├── metadata/                     # Pipeline and data lineage metadata
│   └── ingestion_logs/
│       ├── csv_ingestion_20260126_141019.json
│       ├── api_ingestion_20260126_141055.json
│       └── pipeline_execution_20260126_141029.json
```
│   │   └── api_ingestion/
│   │       ├── year=2026/
│   │       │   ├── month=01/
│   │       │   │   └── day=26/
│   │       │   │       ├── api_ingestion_20260126_134452.json
│   │       │   │       └── api_ingestion_20260126_134416.json
│   ├── data_lineage/
│   ├── schema_registry/
│   └── quality_reports/
└── models/                       # Trained models and artifacts
    ├── collaborative_filtering/
    ├── content_based/
    └── hybrid/
```

## Partitioning Strategy

The data lake follows a **source-type-timestamp partitioning scheme** as required by the assignment:

### 1. Source-Based Partitioning
- **source=csv_ingestion/**: Data sourced from CSV file ingestion
- **source=api_ingestion/**: Data sourced from REST API ingestion  
- **source=data_processing/**: Data from processing and cleaning pipelines
- **source=feature_engineering/**: Data from feature engineering processes
- **source=feature_store/**: Data managed in the feature store

### 2. Type-Based Partitioning
- **type=events/**: User interaction data (clicks, views, purchases)
- **type=products/**: Product catalog information
- **type=categories/**: Product category hierarchies  
- **type=item_properties/**: Detailed product attributes
- **type=user_features/**: User-based features for ML
- **type=item_features/**: Item-based features for ML
- **type=interaction_features/**: Interaction-based features for ML

### 3. Timestamp-Based Partitioning
- **timestamp=YYYYMMDD_HHMMSS/**: Specific timestamp partitions for data organization
- **timestamp suffix**: Files include timestamp for unique identification
- **latest files**: Current snapshot files for easy access to most recent data

### Partitioning Benefits
1. **Source Tracking**: Clear lineage of data origin and ingestion method
2. **Type Organization**: Logical separation for different data types and use cases
3. **Temporal Management**: Chronological organization for time-travel queries and lifecycle management
4. **Query Optimization**: Partition pruning improves query performance significantly
5. **Parallel Processing**: Natural boundaries for distributed processing
6. **Data Governance**: Clear audit trails and data lineage tracking

## File Naming Conventions

### Raw Data Files
```
{data_type}_{source}_{timestamp}.parquet
{data_type}_{source}_{timestamp}.metadata.json
{data_type}_latest.parquet
```
Examples:
- `events_csv_ingestion_20260126_140937.parquet`
- `products_api_ingestion_20260126_141053.parquet`
- `events_latest.parquet`

### Processed Data Files
```
{data_type}_{source}_{timestamp}.parquet
{data_type}_{source}_{timestamp}.metadata.json
```

### Feature Files
```
{feature_type}_{source}_{timestamp}.parquet
{feature_type}_{source}_{timestamp}.metadata.json
```

### Backup Files
```
{original_filename}_{timestamp}.{extension}
```

### Metadata Files
```
{process_type}_{timestamp}.json
```

## Storage Format Standards

### Data Files
- **Format**: Apache Parquet for columnar storage and compression
- **Compression**: Snappy compression for balance of speed and size
- **Schema Evolution**: Backward compatible schema changes supported

### Metadata Files
- **Format**: JSON for human readability and easy parsing
- **Content**: Ingestion statistics, data lineage, quality metrics

### Backup Files
- **Format**: Original format preserved (CSV, JSON)
- **Purpose**: Recovery and audit trails

## Data Lifecycle Management

### 1. Ingestion Phase
1. Data lands in `data/raw/{source}/{type}/year=YYYY/month=MM/day=DD/`
2. Latest snapshot updated in `data/raw/{source}/{type}/`
3. Original data backed up to `data/archive/`

### 2. Processing Phase
1. Raw data validated and cleaned
2. Results stored in `data/processed/{type}/year=YYYY/month=MM/day=DD/`
3. Quality reports generated in `data/metadata/quality_reports/`

### 3. Feature Engineering Phase
1. Features extracted and engineered
2. Stored in `data/features/{feature_type}/year=YYYY/month=MM/day=DD/`
3. Feature metadata tracked in `data/metadata/schema_registry/`

### 4. Archival Phase
1. Old raw data (>30 days) moved to long-term archive
2. Processed data compressed for long-term storage
3. Metadata retained for data lineage

## Access Patterns

### 1. Real-time Access
- **Latest Files**: For current data access (`*_latest.parquet`)
- **Path**: Direct file access without date traversal

### 2. Historical Analysis
- **Date Partitions**: Efficient date range queries
- **Path**: `year=YYYY/month=MM/day=DD/` for specific date ranges

### 3. Data Lineage
- **Metadata**: Complete tracking from source to features
- **Path**: `data/metadata/` for all audit and lineage information

## Security and Access Control

### File Permissions
- Read access: Data scientists, analysts, ML engineers
- Write access: Data pipeline services only
- Admin access: Data platform team

### Data Sensitivity
- **Public**: Product catalogs, categories
- **Internal**: User interactions, behavioral data
- **Restricted**: PII data (if any) with encryption at rest

## Monitoring and Maintenance

### Storage Metrics
- Disk usage by partition and data type
- File count and size trends
- Access pattern analysis

### Cleanup Policies
- Raw data: Retain for 90 days, then archive
- Processed data: Retain for 365 days
- Features: Retain for 180 days
- Metadata: Retain for 730 days (2 years)

## Integration Points

### 1. Data Catalog
- Automatic metadata registration
- Schema evolution tracking
- Data discovery and documentation

### 2. Query Engines
- Spark SQL with Hive metastore
- Pandas for local processing
- Delta Lake for ACID transactions (future)

### 3. ML Platforms
- Feature store integration
- Model artifact storage
- Experiment tracking

## Disaster Recovery

### Backup Strategy
- Daily incremental backups
- Weekly full backups
- Cross-region replication (for cloud deployments)

### Recovery Procedures
- Point-in-time recovery capability
- Automated backup validation
- Recovery time objectives (RTO): 4 hours
- Recovery point objectives (RPO): 1 hour

## Future Enhancements

### Cloud Storage Integration
- AWS S3 with proper IAM roles
- Azure Data Lake Storage
- Google Cloud Storage

### Advanced Partitioning
- Dynamic partitioning based on data volume
- Intelligent archival policies
- Automated data tiering

### Performance Optimization
- Columnar storage optimization
- Partition pruning strategies
- Caching layer implementation