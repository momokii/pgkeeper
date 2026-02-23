# PostgreSQL Restore Test Setup

Isolated environment for testing PostgreSQL backup restoration and validation.

## Overview

This setup creates a completely isolated environment for testing backup restoration:
- **Separate PostgreSQL instance** on port 5434
- **Separate Docker network** - no connection to production
- **Separate volumes** - no data shared with production
- **Python-based validation** using the same RestoreManager as production

## Quick Start

### 1. Configure Environment

```bash
cd restore-test-setup
cp .env.example .env

# Edit .env to specify the backup file to test
# RESTORE_BACKUP_FILE=daily/postgres_db_20260221_030000_v7_daily.dump
```

### 2. Run Restore Test

```bash
docker compose -f compose.yaml up --abort-on-container-exit
```

### 3. View Results

```bash
cat restore-results/validation-report.json
```

### 4. Cleanup

```bash
docker compose -f compose.yaml down -v
```

## Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `POSTGRES_USER` | `postgres_restore_test` | Test database user |
| `POSTGRES_PASSWORD` | `test_password_restore` | Test database password |
| `POSTGRES_DB` | `postgres_restore_test` | Test database name |
| `POSTGRES_TEST_PORT` | `5434` | Port for test database |
| `RESTORE_BACKUP_FILE` | - | Backup file to restore (relative path) |

## Validation Checks

The restore test runs the same 9-point validation as production:

| # | Check | Description |
|---|-------|-------------|
| 1 | Migration Version | Verifies schema_migrations version matches metadata |
| 2 | Migration State | Confirms migration is not in dirty state |
| 3 | Tables Exist | Verifies all expected tables are present |
| 4 | ENUM Types | Validates custom ENUM types exist |
| 5 | Indexes | Verifies indexes exist in public schema |
| 6 | Foreign Keys | Confirms foreign key constraints are present |
| 7 | Row Counts | Compares per-table row counts against metadata |
| 8 | API Health | Optional HTTP health check |
| 9 | Orphan Records | Checks for orphaned records in FK relationships |

## Example Report

```json
{
  "timestamp": "2026-02-21T10:30:00Z",
  "backup_file": "/backups/daily/postgres_db_20260221_030000_v7_daily.dump",
  "database": "postgres_restore_test",
  "restore_success": true,
  "restore_duration_seconds": 12.34,
  "validation_passed": true,
  "validation_errors": [],
  "error": null
}
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│              Isolated Restore Test Network                  │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────────────────────────────────────────┐      │
│  │     postgres-restore-test (Port 5434)            │      │
│  │     - Fresh PostgreSQL 15 instance               │      │
│  │     - Isolated volume                            │      │
│  └──────────────────────────────────────────────────┘      │
│                           ↑                                 │
│                           │                                 │
│  ┌──────────────────────────────────────────────────┐      │
│  │     restore-executor (Python)                    │      │
│  │     - Runs restore_test.py                       │      │
│  │     - Restores backup from /backups              │      │
│  │     - Runs 9-point validation                    │      │
│  │     - Writes report to /results                  │      │
│  └──────────────────────────────────────────────────┘      │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Troubleshooting

### Container exits immediately

Check that `RESTORE_BACKUP_FILE` points to an existing backup file:

```bash
ls -la ../backups/postgres/daily/
```

### Validation fails

Review the validation report for specific errors:

```bash
cat restore-results/validation-report.json | jq '.validation_errors'
```

### Port conflict

If port 5434 is already in use, change it in `.env`:

```
POSTGRES_TEST_PORT=5435
```
