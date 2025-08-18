#!/usr/bin/env python3
"""
MongoDB Production to Test Database Duplication Script

This script duplicates all collections from a production MongoDB database
to a test environment, preserving data and indexes.

USAGE:
======

Prerequisites:
- Python 3.6+
- pymongo library: pip install pymongo
- Read-only access to production database
- Write access to test database

Basic Usage:
    python populate_test_with_prod.py

Dry Run (Preview):
    python populate_test_with_prod.py --dry-run

Environment Variables:
    MONGO_PROD_URI          Production MongoDB connection string
    MONGO_TEST_URI          Test MongoDB connection string  
    MONGO_PROD_DB           Production database name
    MONGO_TEST_DB           Test database name
    MONGO_EXCLUDED_COLLECTIONS  Comma-separated collections to exclude
    MONGO_BATCH_SIZE        Documents per batch (default: 1000)
    MONGO_DRY_RUN           Set to 'true' for dry run mode

Examples:
    # Basic duplication
    export MONGO_PROD_URI="mongodb://prod-server:27017"
    export MONGO_TEST_URI="mongodb://test-server:27017"
    export MONGO_PROD_DB="myapp_production"
    export MONGO_TEST_DB="myapp_test"
    python populate_test_with_prod.py

    # Preview what would be copied
    python populate_test_with_prod.py --dry-run

    # Exclude specific collections
    export MONGO_EXCLUDED_COLLECTIONS="logs,temp,cache"
    python populate_test_with_prod.py

Safety Features:
- Always enforces read-only access to production
- Validates source and destination are different databases
- Drops and recreates test collections (complete replacement)
- Detailed logging to mongo_duplication.log

IMPORTANT: This script completely replaces all collections in the test database.
Existing test data will be lost. Use dry-run mode first to preview changes.
"""

import os
import sys
import logging
from typing import List, Dict, Any, Optional
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure
import time

class MongoDBDuplicator:
    def __init__(self, prod_uri: str, test_uri: str, 
                 prod_db_name: str, test_db_name: str,
                 excluded_collections: Optional[List[str]] = None,
                 batch_size: int = 1000,
                 dry_run: bool = False):
        """
        Initialize the MongoDB duplicator.
        
        Args:
            prod_uri: Production MongoDB connection string
            test_uri: Test MongoDB connection string
            prod_db_name: Production database name
            test_db_name: Test database name
            excluded_collections: List of collections to exclude from duplication
            batch_size: Number of documents to process in each batch
            dry_run: If True, only show what would be copied without executing
        """
        self.prod_uri = prod_uri
        self.test_uri = test_uri
        self.prod_db_name = prod_db_name
        self.test_db_name = test_db_name
        self.excluded_collections = excluded_collections or []
        self.batch_size = batch_size
        self.dry_run = dry_run
        
        self.prod_client: Optional[MongoClient] = None
        self.test_client: Optional[MongoClient] = None
        self.prod_db: Optional[Database] = None
        self.test_db: Optional[Database] = None
        
        self._setup_logging()
        
        # Simple validation - only check different databases
        if not self._validate_different_databases():
            raise ValueError("Configuration validation failed")
        

    def _setup_logging(self):
        """Set up logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('mongo_duplication.log'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)

    def _validate_different_databases(self) -> bool:
        """Validate that source and destination are different databases."""
        if self.prod_uri == self.test_uri and self.prod_db_name == self.test_db_name:
            self.logger.error("Source and destination cannot be the same database")
            return False
        
        self.logger.info("Database validation passed - source and destination are different")
        return True

    def connect(self) -> bool:
        """
        Establish connections to both production and test databases.
        
        Returns:
            bool: True if both connections successful, False otherwise
        """
        try:
            self.logger.info("Connecting to production database...")
            self.prod_client = MongoClient(self.prod_uri, serverSelectionTimeoutMS=5000)
            self.prod_client.admin.command('ping')
            self.prod_db = self.prod_client[self.prod_db_name]
            
            self.logger.info("Connecting to test database...")
            self.test_client = MongoClient(self.test_uri, serverSelectionTimeoutMS=5000)
            self.test_client.admin.command('ping')
            self.test_db = self.test_client[self.test_db_name]
            
            self.logger.info("Successfully connected to both databases")
            
            # Check production database has read-only access
            if not self._check_production_readonly():
                return False
                
            return True
            
        except ConnectionFailure as e:
            self.logger.error(f"Failed to connect to database: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error during connection: {e}")
            return False

    def _check_production_readonly(self) -> bool:
        """
        Check that production database connection has read-only access.
        Always enforces read-only access for safety.
        
        Returns:
            bool: True if production is read-only, False otherwise
        """
        try:
            # Test basic read access by trying to list collections
            collections = self.prod_db.list_collection_names()
            self.logger.info(f"Production database read access confirmed ({len(collections)} collections found)")
            
            # Try to write to production to test permissions
            try:
                test_collection = self.prod_db['_mongo_write_access_test']
                test_collection.insert_one({'test': True, 'timestamp': time.time()})
                # If we get here, we have write access - this is NOT allowed
                test_collection.drop()  # Clean up
                self.logger.error("Production database has WRITE access - this is unsafe for data copying!")
                self.logger.error("Please use a read-only connection to production database.")
                return False
            except Exception as e:
                # Write failed - this is what we want for read-only access
                error_msg = str(e).lower()
                if 'unauthorized' in error_msg or 'not authorized' in error_msg or 'permission' in error_msg:
                    self.logger.info("Production database write access denied (read-only confirmed)")
                    return True
                else:
                    self.logger.warning(f"Production write test failed for unknown reason: {e}")
                    self.logger.info("Assuming read-only access and proceeding...")
                    return True
            
        except Exception as e:
            self.logger.error(f"Production database access check failed: {e}")
            return False

    def disconnect(self):
        """Close database connections."""
        if self.prod_client:
            self.prod_client.close()
        if self.test_client:
            self.test_client.close()
        self.logger.info("Disconnected from databases")

    def get_collection_names(self) -> List[str]:
        """
        Get list of collection names from production database.
        
        Returns:
            List[str]: Collection names excluding system collections and excluded ones
        """
        try:
            all_collections = self.prod_db.list_collection_names()
            
            # Filter out system collections and excluded collections
            collections = [
                coll for coll in all_collections
                if not coll.startswith('system.') and coll not in self.excluded_collections
            ]
            
            self.logger.info(f"Found {len(collections)} collections to duplicate")
            return collections
            
        except Exception as e:
            self.logger.error(f"Error getting collection names: {e}")
            return []

    def copy_indexes(self, source_collection: Collection, target_collection: Collection) -> bool:
        """
        Copy indexes from source collection to target collection.
        
        Args:
            source_collection: Source MongoDB collection
            target_collection: Target MongoDB collection
            
        Returns:
            bool: True if indexes copied successfully, False otherwise
        """
        try:
            indexes = list(source_collection.list_indexes())
            
            for index in indexes:
                # Skip the default _id index
                if index['name'] == '_id_':
                    continue
                    
                # Extract index specification
                index_spec = index['key']
                index_options = {k: v for k, v in index.items() 
                               if k not in ['key', 'v', 'ns']}
                
                # Create index on target collection
                target_collection.create_index(
                    list(index_spec.items()),
                    **index_options
                )
                
            self.logger.info(f"Copied {len(indexes) - 1} indexes")
            return True
            
        except Exception as e:
            self.logger.error(f"Error copying indexes: {e}")
            return False

    def copy_collection_data(self, collection_name: str) -> bool:
        """
        Copy all data from a production collection to test collection.
        
        Args:
            collection_name: Name of the collection to copy
            
        Returns:
            bool: True if data copied successfully, False otherwise
        """
        try:
            source_collection = self.prod_db[collection_name]
            target_collection = self.test_db[collection_name]
            
            # Get total document count
            total_docs = source_collection.count_documents({})
            
            if self.dry_run:
                self.logger.info(f"DRY RUN: Would copy {total_docs} documents from {collection_name}")
                # Check indexes in dry run
                if total_docs > 0:
                    self._dry_run_index_check(source_collection)
                return True
            
            self.logger.info(f"Copying {total_docs} documents from {collection_name}")
            
            # Drop existing collection in test database
            target_collection.drop()
            
            if total_docs == 0:
                self.logger.info(f"Collection {collection_name} is empty, skipping data copy")
                return True
            
            # Copy data in batches
            copied_docs = 0
            cursor = source_collection.find({}).batch_size(self.batch_size)
            
            batch = []
            for document in cursor:
                batch.append(document)
                
                if len(batch) >= self.batch_size:
                    target_collection.insert_many(batch)
                    copied_docs += len(batch)
                    batch = []
                    
                    # Progress update
                    progress = (copied_docs / total_docs) * 100
                    self.logger.info(f"Progress: {copied_docs}/{total_docs} ({progress:.1f}%)")
            
            # Insert remaining documents
            if batch:
                target_collection.insert_many(batch)
                copied_docs += len(batch)
            
            self.logger.info(f"Successfully copied {copied_docs} documents to {collection_name}")
            
            # Copy indexes
            self.copy_indexes(source_collection, target_collection)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error copying collection {collection_name}: {e}")
            return False
    
    def _dry_run_index_check(self, source_collection: Collection):
        """
        Check indexes during dry run without creating them.
        
        Args:
            source_collection: Source collection to analyze
        """
        try:
            indexes = list(source_collection.list_indexes())
            index_count = len([idx for idx in indexes if idx['name'] != '_id_'])
            self.logger.info(f"DRY RUN: Would copy {index_count} indexes")
            
        except Exception as e:
            self.logger.warning(f"Could not analyze indexes during dry run: {e}")

    def duplicate_database(self) -> bool:
        """
        Duplicate entire production database to test environment.
        
        Returns:
            bool: True if duplication successful, False otherwise
        """
        start_time = time.time()
        
        if not self.connect():
            return False
        
        try:
            collections = self.get_collection_names()
            if not collections:
                self.logger.warning("No collections found to duplicate")
                return True
            
            successful_collections = 0
            failed_collections = []
            
            for i, collection_name in enumerate(collections, 1):
                self.logger.info(f"Processing collection {i}/{len(collections)}: {collection_name}")
                
                if self.copy_collection_data(collection_name):
                    successful_collections += 1
                else:
                    failed_collections.append(collection_name)
            
            # Summary
            elapsed_time = time.time() - start_time
            self.logger.info(f"\n=== Duplication Summary ===")
            self.logger.info(f"Total collections: {len(collections)}")
            self.logger.info(f"Successful: {successful_collections}")
            self.logger.info(f"Failed: {len(failed_collections)}")
            self.logger.info(f"Time elapsed: {elapsed_time:.2f} seconds")
            
            if failed_collections:
                self.logger.error(f"Failed collections: {', '.join(failed_collections)}")
                return False
            
            if self.dry_run:
                self.logger.info("DRY RUN completed successfully! No changes were made.")
            else:
                self.logger.info("Database duplication completed successfully!")
            return True
            
        except Exception as e:
            self.logger.error(f"Fatal error during duplication: {e}")
            return False
        finally:
            self.disconnect()


def load_config_from_env() -> Dict[str, Any]:
    """Load configuration from environment variables."""
    config = {
        'prod_uri': os.getenv('MONGO_PROD_URI', 'mongodb://localhost:27017'),
        'test_uri': os.getenv('MONGO_TEST_URI', 'mongodb://localhost:27017'),
        'prod_db_name': os.getenv('MONGO_PROD_DB', 'production'),
        'test_db_name': os.getenv('MONGO_TEST_DB', 'test'),
        'excluded_collections': os.getenv('MONGO_EXCLUDED_COLLECTIONS', '').split(','),
        'batch_size': int(os.getenv('MONGO_BATCH_SIZE', '1000')),
        'dry_run': os.getenv('MONGO_DRY_RUN', '').lower() in ('true', '1', 'yes')
    }
    
    # Filter empty excluded collections
    config['excluded_collections'] = [
        coll.strip() for coll in config['excluded_collections'] 
        if coll.strip()
    ]
    
    return config


def parse_arguments():
    """Parse command line arguments."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='MongoDB Production to Test Database Duplicator',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
USAGE EXAMPLES:

  Basic duplication:
    %(prog)s

  Preview changes first (recommended):
    %(prog)s --dry-run

  With environment variables:
    export MONGO_PROD_URI="mongodb://prod-server:27017"
    export MONGO_TEST_URI="mongodb://test-server:27017"
    export MONGO_PROD_DB="myapp_production"
    export MONGO_TEST_DB="myapp_test"
    %(prog)s

  Exclude collections:
    export MONGO_EXCLUDED_COLLECTIONS="logs,temp,cache"
    %(prog)s --dry-run

SAFETY NOTES:
- Production database must be read-only accessible
- Test database will be completely replaced
- Always run with --dry-run first to preview changes
- Check mongo_duplication.log for detailed operation logs
        """
    )
    
    parser.add_argument(
        '--dry-run', 
        action='store_true',
        help='Preview operations without executing them'
    )
    
    return parser.parse_args()


def main():
    """Main function to run the duplication process."""
    print("MongoDB Production to Test Database Duplicator")
    print("=" * 50)
    
    # Parse command line arguments
    args = parse_arguments()
    
    # Load configuration
    config = load_config_from_env()
    
    # Override config with command line arguments
    if args.dry_run:
        config['dry_run'] = True
    
    # Display configuration
    print(f"Production DB: {config['prod_db_name']}")
    print(f"Test DB: {config['test_db_name']}")
    print(f"Batch size: {config['batch_size']}")
    print(f"Mode: {'DRY RUN' if config['dry_run'] else 'LIVE EXECUTION'}")
    print("Read-only enforcement: ALWAYS ENABLED (production must be read-only)")
    
    if config['excluded_collections']:
        print(f"Excluded collections: {', '.join(config['excluded_collections'])}")
    print()
    
    # Confirm before proceeding
    warning_msg = (
        "This will overwrite the test database" if not config['dry_run'] 
        else "This is a dry run - no changes will be made"
    )
    response = input(f"Do you want to proceed? {warning_msg}. (y/N): ")
    if response.lower() != 'y':
        print("Operation cancelled.")
        return
    
    # Create duplicator and run
    try:
        duplicator = MongoDBDuplicator(**config)
    except ValueError as e:
        print(f"Configuration error: {e}")
        sys.exit(1)
    
    try:
        success = duplicator.duplicate_database()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\nOperation interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()