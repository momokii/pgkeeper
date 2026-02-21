# PostgreSQL Backup Scheduler

A Docker-based automated backup system for PostgreSQL databases. Now with a unified Python application replacing the previous Bash + Python architecture.

## Features

- **Unified Python application** - Single codebase for all operations
- Automated daily and weekly backups (APScheduler-based, no cron dependency)
- Compressed custom-format dumps (`pg_dump -Fc`) with configurable compression level
- SHA-256 checksums and JSON metadata for every backup
- Configurable retention policy (default: 7 daily, 4 weekly)
- **Cloud backup integration** with Google Cloud Storage (optional)
- **Automatic upload to cloud storage** with duplicate prevention
- **Cloud download for disaster recovery**
- Isolated restore testing environment with zero production impact
- General-purpose 9-check restore validation (works with any PostgreSQL database)

## Architecture

### Current (v2.0) - Python-Only

```
┌─────────────────────────────────────────────────────────────┐
│              Single Python Application                       │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────────────────────────────────────────┐      │
│  │      BackupManager (pg_dump via subprocess)      │      │
│  └──────────────────────────────────────────────────┘      │
│                                                             │
│  ┌──────────────────────────────────────────────────┐      │
│  │     RestoreManager (pg_restore + validation)      │      │
│  └──────────────────────────────────────────────────┘      │
│                                                             │
│  ┌──────────────────────────────────────────────────┐      │
│  │    CloudStorageManager (google-cloud-storage)      │      │
│  └──────────────────────────────────────────────────┘      │
│                                                             │
│  ┌──────────────────────────────────────────────────┐      │
│  │      JobScheduler (APScheduler)                  │      │
│  └──────────────────────────────────────────────────┘      │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Directory Structure

```
backup-postgres/
├── docker-compose-unified.yaml   # New unified service
├── Dockerfile                   # Python 3.12 + postgresql-client
├── pyproject.toml               # Python dependencies
├── .env                         # Database & GCS credentials (gitignored)
├── src/backup_postgres/         # Python application
│   ├── config/
│   │   └── settings.py         # Configuration management
│   ├── core/
│   │   ├── backup.py           # BackupManager
│   │   ├── restore.py          # RestoreManager + 9-point validation
│   │   ├── retention.py        # RetentionPolicy
│   │   ├── metadata.py         # Metadata generator
│   │   └── models.py          # Data models
│   ├── cloud/
│   │   ├── gcs_storage.py     # GCS operations
│   │   └── registry.py        # Upload registry
│   ├── scheduler/
│   │   └── jobs.py            # APScheduler setup
│   └── utils/
│       ├── logging.py          # Structured logging
│       ├── checksum.py         # SHA-256 calculation
│       ├── subprocess.py       # pg_dump/pg_restore wrappers
│       └── exceptions.py      # Custom exceptions
├── scripts-new/
│   ├── entrypoint.py         # Main daemon entry point
│   └── cli.py               # CLI commands
├── scripts/                     # Legacy scripts (v1.x)
├── restore-test-setup/          # Isolated restore test environment
├── backups/                     # Backup storage (gitignored)
│   ├── daily/                   # Daily backups (.dump + .json)
│   ├── weekly/                  # Weekly backups (.dump + .json)
│   └── manual/                  # Manual backups (.dump + .json)
└── docs/                        # Documentation
    ├── GCP_SETUP.md             # Google Cloud setup guide
    ├── MIGRATION_GUIDE.md        # Migration from v1.x
    └── ...                       # Other documentation
```

## Quick Start (v2.0)

### 1. Configure Environment

```bash
# For PostgreSQL (required)
export POSTGRES_USER=your_user
export POSTGRES_PASSWORD=your_password
export POSTGRES_DB=your_database

# For GCS (optional - for cloud backup)
export GCS_BUCKET_NAME=your-bucket-name
```

### 2. Start Backup Service

```bash
# Using the new unified service
docker compose -f docker-compose-unified.yaml up -d
```

### 3. Manual Operations

```bash
# Create a manual backup
docker exec postgres_backup_unified python /app/scripts/cli.py backup --type manual

# List local backups
docker exec postgres_backup_unified python /app/scripts/cli.py list

# List cloud backups
docker exec postgres_backup_unified python /app/scripts/cli.py list --cloud

# Test GCS connection
docker exec postgres_backup_unified python /app/scripts/cli.py test
```

## Backup Schedule

| Type | Schedule | Retention |
|------|----------|-----------|
| Daily | 2:00 AM UTC | 7 most recent |
| Weekly | 3:00 AM Sunday UTC | 4 most recent |
| Manual | On-demand | No auto-cleanup |

## Configuration

### Database (Required)

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `POSTGRES_USER` | — | Database user |
| `POSTGRES_PASSWORD` | — | Database password |
| `POSTGRES_DB` | — | Database name |
| `POSTGRES_HOST` | `postgres` | Database hostname |
| `POSTGRES_PORT` | `5432` | Database port |

### Backup Options

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `BACKUP_RETENTION_DAILY` | `7` | Number of daily backups to keep |
| `BACKUP_RETENTION_WEEKLY` | `4` | Number of weekly backups to keep |
| `BACKUP_COMPRESSION_LEVEL` | `9` | pg_dump compression level (0-9) |

### GCS Cloud Backup (Optional)

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `GCS_BUCKET_NAME` | — | GCS bucket name |
| `GCS_CREDENTIALS_PATH` | `/gcs-credentials/credentials.json` | Service account JSON path |
| `GCS_BACKUP_PREFIX` | `backups/postgres` | Path prefix in bucket |
| `GCS_UPLOAD_RETRY_MAX` | `3` | Max upload retry attempts |

### Logging

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `LOG_LEVEL` | `INFO` | Log level (DEBUG, INFO, WARNING, ERROR) |

## Cloud Backup Setup (GCS)

### 1. Create GCS Resources

See [GCP Setup Guide](docs/GCP_SETUP.md) for detailed instructions.

### 2. Create Service Account

```bash
# Create service account with Storage Object Admin role
# Download JSON key file
```

### 3. Mount Credentials

```yaml
volumes:
  - ./gcs-credentials.json:/gcs-credentials/credentials.json:ro
```

### 4. Start Service

```bash
docker compose -f docker-compose-unified.yaml up -d
```

## Restore Testing

The restore test runs in a fully isolated environment (separate database, network, and volume) with no connection to production.

### Run Full Restore Test

```bash
cd restore-test-setup
docker compose up --abort-on-container-exit
```

### View Results

```bash
cat restore-test-setup/restore-results/validation-report.json
```

### Cleanup

```bash
cd restore-test-setup
docker compose down -v
```

### Validation Checks

The validation system runs 9 checks against the restored database:

| # | Check | Description |
|---|-------|-------------|
| 1 | Migration Version | Verifies `schema_migrations` version matches metadata |
| 2 | Migration State | Confirms migration is not in dirty state |
| 3 | Tables Exist | Verifies all expected tables are present |
| 4 | ENUM Types | Validates custom ENUM types exist |
| 5 | Indexes | Verifies indexes exist in public schema |
| 6 | Foreign Keys | Confirms foreign key constraints are present |
| 7 | Row Counts | Compares per-table row counts against metadata |
| 8 | API Health | Optional HTTP health check |
| 9 | Orphan Records | Checks for orphaned records in FK relationships |

## CLI Commands

```bash
# Create backups
python scripts/cli.py backup --type daily
python scripts/cli.py backup --type weekly
python scripts/cli.py backup --type manual

# Restore from backup
python scripts/cli.py restore /path/to/backup.dump

# List backups
python scripts/cli.py list
python scripts/cli.py list --type daily
python scripts/cli.py list --cloud --json

# Upload to cloud
python scripts/cli.py upload --file /path/to/backup.dump
python scripts/cli.py upload  # Sync all pending

# Download from cloud
python scripts/cli.py download backups/postgres/daily/backup.dump

# Test GCS connection
python scripts/cli.py test
```

## Requirements

- Docker Engine 20.10+
- Docker Compose v2+
- PostgreSQL 12+ (client tools in container)
- Python 3.12+
- Google Cloud project (for cloud backup integration)

