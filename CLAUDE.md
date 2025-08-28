# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a MongoDB database management scripts repository containing Python utilities for database migrations, data transformations, and maintenance operations. The scripts are designed for production-grade MongoDB operations with comprehensive logging, error handling, and dry-run capabilities.

## Core Architecture

### Script Structure
All scripts follow a consistent architecture pattern:
- **Configuration Management**: Environment variables + command-line arguments
- **Connection Handling**: Robust MongoDB connection with timeout and error handling
- **Safety Features**: Mandatory dry-run mode, comprehensive logging, progress tracking
- **Batch Processing**: Configurable batch sizes for large datasets
- **Error Recovery**: Detailed error reporting and graceful failure handling

### Key Components
- `PhoneFieldsCopier`: Copies phone fields to new field names (phone → phoneNumber, phoneVerification → phoneNumberVerified)
- `UserIdMigrator`: Migrates userId fields to match MongoDB _id values across collections
- `MongoDBDuplicator`: Duplicates production databases to test environments safely
- `OffersFieldRenamer`: Renames offer.id to offer.offerId in orders collection
- `UsersNameUpdater`: Creates name field by concatenating firstName + lastName

## Development Environment

### Python Environment
- Python 3.6+ required
- Virtual environment in `venv/` directory
- Dependencies managed via `requirements.txt` (contains `pymongo>=4.0.0`)

### Setup Commands
```bash
# Activate virtual environment
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run any script with preview
python script_name.py --dry-run

# Run actual operation (always run dry-run first)
python script_name.py
```

### Configuration
All scripts use consistent environment variable patterns:
- `MONGO_URI`: MongoDB connection string
- `MONGO_DB`: Database name
- `MONGO_BATCH_SIZE`: Documents per batch (default: 1000)
- `MONGO_DRY_RUN`: Set to 'true' for dry-run mode

Script-specific variables:
- `MONGO_PROD_URI`, `MONGO_TEST_URI`: For database duplication
- `MONGO_EXCLUDED_COLLECTIONS`: Collections to exclude from operations
- `MONGO_SKIP_EXISTING`: Skip documents with existing target fields

## Run Configurations

The `.run/` directory contains IntelliJ/PyCharm run configurations with pre-configured environment variables for production operations. These include database URIs and specific collection exclusions.

## Safety Protocols

### Mandatory Practices
1. **Always run with `--dry-run` first** to preview changes
2. **Check log files** for detailed operation reports (each script generates its own log)
3. **Verify environment variables** before running operations
4. **Use read-only production access** for data copying operations

### Log Files
Each script generates detailed logs:
- `migrate_user_ids.log`: User ID migration operations
- `phone_fields_copy.log`: Phone field copy operations  
- `mongo_duplication.log`: Database duplication operations
- `rename_offers_id_field.log`: Field renaming operations
- `users_name_update.log`: Name field update operations

## Collections Involved

### Primary Collections
- `users`: Main user collection with userId, firstName, lastName, phone fields
- `orders`: Contains offers array with id/offerId fields  
- `addresses`, `reviews`, `rewards`: Reference collections with userId fields

### Database Operations
- **User ID Migration**: Updates userId across users, addresses, orders, reviews, rewards
- **Phone Field Migration**: Copies phone → phoneNumber, phoneVerification → phoneNumberVerified
- **Field Renaming**: Renames offers.id → offers.offerId in orders
- **Name Field Creation**: Concatenates firstName + lastName → name
- **Database Duplication**: Complete production → test environment copying

## Common Patterns

### Running Operations
```bash
# Preview any operation
export MONGO_URI="mongodb://localhost:27017"
export MONGO_DB="database_name"
python script_name.py --dry-run

# Execute after review
python script_name.py
```

### Batch Processing
All scripts support configurable batch processing:
```bash
export MONGO_BATCH_SIZE="5000"  # Larger batches for performance
```

### Error Handling
Scripts include comprehensive error handling with:
- Connection failure recovery
- Document-level error isolation
- Progress tracking continuation
- Detailed error logging

## Testing Strategy

Always test database operations in the following order:
1. **Dry-run on production** (read-only analysis)
2. **Test environment execution** (full operation)  
3. **Production execution** (after validation)

Use the database duplication script to create test environments that mirror production data structure and volume.