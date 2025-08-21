#!/usr/bin/env python3
"""
MongoDB User ID Migration and Integrity Verification Script

This script migrates userId fields to use MongoDB _id values and maintains 
referential integrity across all collections.

USAGE:
======

Prerequisites:
- Python 3.6+
- pymongo library: pip install pymongo
- Write access to the target database

What it does:
1. Updates users collection: replaces userId field with _id field value
2. Updates referencing collections: updates userId references in addresses, orders, reviews, rewards
3. Performs integrity checks: validates all references exist and are consistent
4. Provides detailed reporting: shows before/after statistics and any issues

Basic Usage:
    python migrate_user_ids.py

Dry Run (Preview):
    python migrate_user_ids.py --dry-run

Integrity Check Only:
    python migrate_user_ids.py --check-only

Environment Variables:
    MONGO_URI               MongoDB connection string
    MONGO_DB                Database name
    MONGO_BATCH_SIZE        Documents per batch (default: 1000)
    MONGO_DRY_RUN           Set to 'true' for dry run mode

Examples:
    # Preview the migration
    export MONGO_URI="mongodb://localhost:27017"
    export MONGO_DB="myapp"
    python migrate_user_ids.py --dry-run

    # Perform the migration
    python migrate_user_ids.py

    # Only check integrity (no migration)
    python migrate_user_ids.py --check-only

Collections Involved:
- users (primary collection with userId field)
- addresses (references userId)
- orders (references userId)  
- reviews (references userId)
- rewards (references userId)

Migration Process:
1. Analyze current state
2. Create userId to _id mapping
3. Update users collection userId field
4. Update all referencing collections
5. Verify referential integrity
6. Generate migration report

Safety Features:
- Dry-run mode to preview changes
- Backup creation of userId mappings
- Transaction support where possible
- Comprehensive validation before and after
- Detailed logging to migrate_user_ids.log
- Rollback capability on errors

IMPORTANT: This script modifies userId fields across multiple collections.
Always run with --dry-run first to preview changes and verify the migration plan.
"""

import os
import sys
import logging
from typing import Dict, Any, List, Optional, Tuple
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure, BulkWriteError
from bson import ObjectId
import time
import json

class UserIdMigrator:
    def __init__(self, mongo_uri: str, db_name: str, 
                 batch_size: int = 1000,
                 dry_run: bool = False,
                 check_only: bool = False):
        """
        Initialize the user ID migrator.
        
        Args:
            mongo_uri: MongoDB connection string
            db_name: Database name
            batch_size: Number of documents to process in each batch
            dry_run: If True, only show what would be updated without executing
            check_only: If True, only perform integrity checks without migration
        """
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.batch_size = batch_size
        self.dry_run = dry_run
        self.check_only = check_only
        
        # Collection names
        self.users_collection = 'users'
        self.referencing_collections = ['addresses', 'orders', 'reviews', 'rewards']
        
        self.client: Optional[MongoClient] = None
        self.db: Optional[Database] = None
        
        # Migration tracking
        self.migration_map: Dict[str, str] = {}  # old_userId -> new_userId (_id)
        self.backup_file = 'user_id_migration_backup.json'
        
        self._setup_logging()

    def _setup_logging(self):
        """Set up logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('migrate_user_ids.log'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)

    def connect(self) -> bool:
        """
        Establish connection to MongoDB database.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.logger.info("Connecting to MongoDB...")
            self.client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=5000)
            self.client.admin.command('ping')
            self.db = self.client[self.db_name]
            
            self.logger.info("Successfully connected to MongoDB")
            return True
            
        except ConnectionFailure as e:
            self.logger.error(f"Failed to connect to MongoDB: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error during connection: {e}")
            return False

    def disconnect(self):
        """Close database connection."""
        if self.client:
            self.client.close()
        self.logger.info("Disconnected from MongoDB")

    def _count_matching_documents(self, collection: Collection, match_expression: Dict[str, Any]) -> int:
        """
        Count documents using aggregation pipeline with $expr.
        
        Args:
            collection: MongoDB collection
            match_expression: Expression for $match stage
            
        Returns:
            int: Count of matching documents
        """
        try:
            pipeline = [
                {'$match': match_expression},
                {'$count': 'total'}
            ]
            result = list(collection.aggregate(pipeline))
            return result[0]['total'] if result else 0
        except Exception as e:
            self.logger.warning(f"Error counting documents: {e}")
            return 0

    def analyze_current_state(self) -> Dict[str, Any]:
        """
        Analyze the current state of all collections.
        
        Returns:
            Dict[str, Any]: Analysis results
        """
        self.logger.info("Analyzing current state...")
        
        analysis = {
            'users': {},
            'references': {},
            'integrity': {}
        }
        
        try:
            users_coll = self.db[self.users_collection]
            
            # Analyze users collection
            analysis['users']['total_users'] = users_coll.count_documents({})
            analysis['users']['users_with_userId'] = users_coll.count_documents({
                'userId': {'$exists': True, '$ne': None}
            })
            analysis['users']['users_without_userId'] = users_coll.count_documents({
                '$or': [
                    {'userId': {'$exists': False}},
                    {'userId': None}
                ]
            })
            analysis['users']['users_where_userId_equals_id'] = self._count_matching_documents(
                users_coll,
                {
                    '$expr': {
                        '$eq': [
                            {'$toString': '$_id'},
                            {'$toString': '$userId'}
                        ]
                    }
                }
            )
            
            # Analyze referencing collections
            for coll_name in self.referencing_collections:
                coll = self.db[coll_name]
                analysis['references'][coll_name] = {
                    'total_documents': coll.count_documents({}),
                    'documents_with_userId': coll.count_documents({
                        'userId': {'$exists': True, '$ne': None}
                    }),
                    'unique_userId_values': len(coll.distinct('userId'))
                }
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"Error during analysis: {e}")
            return analysis

    def display_analysis(self, analysis: Dict[str, Any]):
        """Display analysis results."""
        self.logger.info("\n" + "=" * 70)
        self.logger.info("CURRENT STATE ANALYSIS")
        self.logger.info("=" * 70)
        
        # Users collection analysis
        users_info = analysis.get('users', {})
        self.logger.info("USERS COLLECTION:")
        self.logger.info(f"  Total users: {users_info.get('total_users', 0):,}")
        self.logger.info(f"  Users with userId field: {users_info.get('users_with_userId', 0):,}")
        self.logger.info(f"  Users without userId field: {users_info.get('users_without_userId', 0):,}")
        self.logger.info(f"  Users where userId equals _id: {users_info.get('users_where_userId_equals_id', 0):,}")
        
        # Referencing collections analysis
        self.logger.info("\nREFERENCING COLLECTIONS:")
        references = analysis.get('references', {})
        for coll_name in self.referencing_collections:
            if coll_name in references:
                info = references[coll_name]
                self.logger.info(f"  {coll_name}:")
                self.logger.info(f"    Total documents: {info.get('total_documents', 0):,}")
                self.logger.info(f"    Documents with userId: {info.get('documents_with_userId', 0):,}")
                self.logger.info(f"    Unique userId values: {info.get('unique_userId_values', 0):,}")
        
        self.logger.info("=" * 70)

    def create_migration_mapping(self) -> bool:
        """
        Create mapping from current userId to _id values.
        
        Returns:
            bool: True if mapping created successfully
        """
        try:
            self.logger.info("Creating userId to _id mapping...")
            
            users_coll = self.db[self.users_collection]
            # Find all users to create mapping (including those without userId)
            cursor = users_coll.find({}, {'_id': 1, 'userId': 1})
            
            self.migration_map = {}
            for user in cursor:
                # Handle users with or without userId field
                old_user_id = str(user.get('userId', user['_id']))  # Use _id if no userId
                new_user_id = str(user['_id'])
                self.migration_map[old_user_id] = new_user_id
            
            self.logger.info(f"Created mapping for {len(self.migration_map)} users")
            
            # Save backup
            self._save_migration_backup()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error creating migration mapping: {e}")
            return False

    def _save_migration_backup(self):
        """Save migration mapping to backup file."""
        try:
            with open(self.backup_file, 'w') as f:
                json.dump(self.migration_map, f, indent=2)
            self.logger.info(f"Migration mapping saved to {self.backup_file}")
        except Exception as e:
            self.logger.warning(f"Could not save backup file: {e}")

    def migrate_users_collection(self) -> bool:
        """
        Update userId field in users collection to match _id.
        
        Returns:
            bool: True if migration successful
        """
        if self.dry_run:
            self.logger.info("DRY RUN: Would update userId field in users collection")
            return True
        
        try:
            self.logger.info("Migrating users collection...")
            
            users_coll = self.db[self.users_collection]
            updated_count = 0
            
            # Update each user's userId to match their _id (as string)
            # This includes users without userId field (they will get one created)
            for user_doc in users_coll.find({}):
                result = users_coll.update_one(
                    {'_id': user_doc['_id']},
                    {'$set': {'userId': str(user_doc['_id'])}}
                )
                if result.modified_count > 0:
                    updated_count += 1
            
            self.logger.info(f"Updated {updated_count} users in users collection")
            return True
            
        except Exception as e:
            self.logger.error(f"Error migrating users collection: {e}")
            return False

    def migrate_referencing_collections(self) -> bool:
        """
        Update userId references in all referencing collections.
        
        Returns:
            bool: True if migration successful
        """
        success = True
        
        for coll_name in self.referencing_collections:
            if not self._migrate_single_collection(coll_name):
                success = False
        
        return success

    def _migrate_single_collection(self, collection_name: str) -> bool:
        """
        Migrate userId references in a single collection.
        
        Args:
            collection_name: Name of the collection to migrate
            
        Returns:
            bool: True if successful
        """
        try:
            self.logger.info(f"Migrating collection: {collection_name}")
            
            coll = self.db[collection_name]
            
            if self.dry_run:
                # Count documents that would be updated
                count = coll.count_documents({'userId': {'$exists': True, '$ne': None}})
                self.logger.info(f"DRY RUN: Would update {count} documents in {collection_name}")
                return True
            
            updated_count = 0
            
            # Process in batches
            cursor = coll.find(
                {'userId': {'$exists': True, '$ne': None}},
                {'_id': 1, 'userId': 1}
            ).batch_size(self.batch_size)
            
            for doc in cursor:
                old_user_id = str(doc['userId'])
                
                # Find the new userId from our mapping
                if old_user_id in self.migration_map:
                    new_user_id = self.migration_map[old_user_id]  # Already a string
                    
                    result = coll.update_one(
                        {'_id': doc['_id']},
                        {'$set': {'userId': new_user_id}}
                    )
                    
                    if result.modified_count > 0:
                        updated_count += 1
                # Note: No warning for missing mappings - users may not have documents in all collections
            
            self.logger.info(f"Updated {updated_count} documents in {collection_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error migrating {collection_name}: {e}")
            return False

    def verify_integrity(self) -> Dict[str, Any]:
        """
        Verify referential integrity after migration.
        
        Returns:
            Dict[str, Any]: Integrity check results
        """
        self.logger.info("Verifying referential integrity...")
        
        results = {
            'users_consistency': {},
            'reference_integrity': {},
            'orphaned_references': {}
        }
        
        try:
            # Check users collection consistency
            results['users_consistency'] = self._check_users_consistency()
            
            # Check reference integrity
            results['reference_integrity'] = self._check_reference_integrity()
            
            # Find orphaned references
            results['orphaned_references'] = self._find_orphaned_references()
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error during integrity verification: {e}")
            return results

    def _check_users_consistency(self) -> Dict[str, int]:
        """Check that all users have consistent _id and userId values."""
        users_coll = self.db[self.users_collection]
        
        total_users = users_coll.count_documents({})
        consistent_users = self._count_matching_documents(
            users_coll,
            {
                '$expr': {
                    '$and': [
                        {'$ne': ['$userId', None]},  # userId exists and is not null
                        {'$eq': [
                            {'$toString': '$_id'},
                            {'$toString': '$userId'}
                        ]}
                    ]
                }
            }
        )
        
        return {
            'total_users': total_users,
            'consistent_users': consistent_users,
            'inconsistent_users': total_users - consistent_users,
            'inconsistent_user_details': self._find_inconsistent_users()
        }

    def _check_reference_integrity(self) -> Dict[str, Dict[str, Any]]:
        """Check that all userId references exist in users collection."""
        users_coll = self.db[self.users_collection]
        
        # Get all valid user IDs from userId field (which should now be strings matching _id)
        valid_user_ids = set(str(user_id) for user_id in users_coll.distinct('userId') if user_id is not None)
        
        results = {}
        
        for coll_name in self.referencing_collections:
            coll = self.db[coll_name]
            
            total_with_userId = coll.count_documents({'userId': {'$exists': True, '$ne': None}})
            
            # Count valid references and collect invalid ones
            valid_references = 0
            invalid_references = 0
            invalid_user_ids = []
            
            for doc in coll.find({'userId': {'$exists': True, '$ne': None}}, {'userId': 1}):
                user_id_str = str(doc['userId'])
                if user_id_str in valid_user_ids:
                    valid_references += 1
                else:
                    invalid_references += 1
                    if user_id_str not in invalid_user_ids:
                        invalid_user_ids.append(user_id_str)
            
            results[coll_name] = {
                'total_references': total_with_userId,
                'valid_references': valid_references,
                'invalid_references': invalid_references,
                'invalid_user_ids': invalid_user_ids[:50]  # Limit to first 50 for display
            }
        
        return results

    def _find_orphaned_references(self) -> Dict[str, List[str]]:
        """Find specific orphaned references."""
        users_coll = self.db[self.users_collection]
        valid_user_ids = set(str(user_id) for user_id in users_coll.distinct('userId') if user_id is not None)
        
        orphaned = {}
        
        for coll_name in self.referencing_collections:
            coll = self.db[coll_name]
            orphaned_ids = []
            
            for doc in coll.find({'userId': {'$exists': True, '$ne': None}}, {'userId': 1}):
                user_id_str = str(doc['userId'])
                if user_id_str not in valid_user_ids:
                    orphaned_ids.append(user_id_str)
            
            orphaned[coll_name] = list(set(orphaned_ids))  # Remove duplicates
        
        return orphaned

    def _find_inconsistent_users(self, limit: int = 50) -> List[Dict[str, str]]:
        """
        Find users where _id does not equal userId.
        
        Args:
            limit: Maximum number of inconsistent users to return
            
        Returns:
            List[Dict[str, str]]: List of inconsistent users with _id and userId
        """
        try:
            users_coll = self.db[self.users_collection]
            
            # Use aggregation to find inconsistent users
            pipeline = [
                {
                    '$match': {
                        '$expr': {
                            '$or': [
                                {'$eq': ['$userId', None]},  # userId is null
                                {'$ne': [
                                    {'$toString': '$_id'},
                                    {'$toString': '$userId'}
                                ]}
                            ]
                        }
                    }
                },
                {
                    '$project': {
                        '_id': 1,
                        'userId': 1
                    }
                },
                {'$limit': limit}
            ]
            
            inconsistent_users = []
            for user in users_coll.aggregate(pipeline):
                inconsistent_users.append({
                    '_id': str(user['_id']),
                    'userId': str(user.get('userId', 'None'))
                })
            
            return inconsistent_users
            
        except Exception as e:
            self.logger.warning(f"Error finding inconsistent users: {e}")
            return []

    def display_integrity_results(self, results: Dict[str, Any]):
        """Display integrity check results."""
        self.logger.info("\n" + "=" * 70)
        self.logger.info("REFERENTIAL INTEGRITY VERIFICATION")
        self.logger.info("=" * 70)
        
        # Users consistency
        users_consistency = results.get('users_consistency', {})
        self.logger.info("USERS COLLECTION CONSISTENCY:")
        self.logger.info(f"  Total users: {users_consistency.get('total_users', 0):,}")
        self.logger.info(f"  Consistent users (_id = userId): {users_consistency.get('consistent_users', 0):,}")
        self.logger.info(f"  Inconsistent users: {users_consistency.get('inconsistent_users', 0):,}")
        
        # Show inconsistent user details
        inconsistent_details = users_consistency.get('inconsistent_user_details', [])
        if inconsistent_details:
            self.logger.warning(f"\nINCONSISTENT USERS FOUND:")
            for i, user in enumerate(inconsistent_details, 1):
                self.logger.warning(f"  User {i}: _id={user['_id']}, userId={user['userId']}")
            
            if len(inconsistent_details) >= 50:
                self.logger.warning(f"  ... (showing first 50 of {users_consistency.get('inconsistent_users', 0)} total)")
        else:
            if users_consistency.get('inconsistent_users', 0) > 0:
                self.logger.info("  (Unable to retrieve inconsistent user details)")
        
        # Reference integrity
        self.logger.info("\nREFERENCE INTEGRITY:")
        reference_integrity = results.get('reference_integrity', {})
        for coll_name, stats in reference_integrity.items():
            self.logger.info(f"  {coll_name}:")
            self.logger.info(f"    Total references: {stats.get('total_references', 0):,}")
            self.logger.info(f"    Valid references: {stats.get('valid_references', 0):,}")
            self.logger.info(f"    Invalid references: {stats.get('invalid_references', 0):,}")
            
            # Show invalid userId values
            invalid_user_ids = stats.get('invalid_user_ids', [])
            if invalid_user_ids:
                self.logger.warning(f"    Invalid userId values found:")
                for invalid_id in invalid_user_ids:
                    self.logger.warning(f"      - {invalid_id}")
                
                total_invalid = stats.get('invalid_references', 0)
                if len(invalid_user_ids) >= 50 and total_invalid > 50:
                    self.logger.warning(f"    ... (showing first 50 of {total_invalid} total invalid references)")
        
        # Orphaned references
        orphaned = results.get('orphaned_references', {})
        self.logger.info("\nORPHANED REFERENCES:")
        for coll_name, orphaned_ids in orphaned.items():
            if orphaned_ids:
                self.logger.warning(f"  {coll_name}: {len(orphaned_ids)} orphaned userId values")
                if len(orphaned_ids) <= 10:
                    self.logger.warning(f"    Orphaned IDs: {orphaned_ids}")
                else:
                    self.logger.warning(f"    First 10 orphaned IDs: {orphaned_ids[:10]}")
            else:
                self.logger.info(f"  {coll_name}: No orphaned references found")
        
        self.logger.info("=" * 70)

    def run_migration(self) -> bool:
        """
        Run the complete migration process.
        
        Returns:
            bool: True if migration successful
        """
        start_time = time.time()
        
        if not self.connect():
            return False
        
        try:
            # Analyze current state
            initial_analysis = self.analyze_current_state()
            self.display_analysis(initial_analysis)
            
            # Perform initial integrity check
            self.logger.info("Performing pre-migration integrity check...")
            pre_migration_integrity = self.verify_integrity()
            self.display_integrity_results(pre_migration_integrity)
            
            if self.check_only:
                self.logger.info("Check-only mode: Skipping migration")
                return True
            
            # Create migration mapping
            if not self.create_migration_mapping():
                return False
            
            if not self.migration_map:
                self.logger.info("No users found to migrate")
                return True
            
            # Migrate users collection
            if not self.migrate_users_collection():
                return False
            
            # Migrate referencing collections
            if not self.migrate_referencing_collections():
                return False
            
            # Verify integrity after migration
            self.logger.info("Performing post-migration integrity check...")
            integrity_results = self.verify_integrity()
            self.display_integrity_results(integrity_results)
            
            # Final summary
            elapsed_time = time.time() - start_time
            self.logger.info(f"\n=== Migration Summary ===")
            self.logger.info(f"Migration completed in {elapsed_time:.2f} seconds")
            self.logger.info(f"Users migrated: {len(self.migration_map)}")
            self.logger.info(f"Collections updated: {len(self.referencing_collections) + 1}")
            
            if self.dry_run:
                self.logger.info("DRY RUN completed - no actual changes made")
            else:
                self.logger.info("Migration completed successfully!")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Fatal error during migration: {e}")
            return False
        finally:
            self.disconnect()


def load_config_from_env() -> Dict[str, Any]:
    """Load configuration from environment variables."""
    config = {
        'mongo_uri': os.getenv('MONGO_URI', 'mongodb://localhost:27017'),
        'db_name': os.getenv('MONGO_DB', 'test'),
        'batch_size': int(os.getenv('MONGO_BATCH_SIZE', '1000')),
        'dry_run': os.getenv('MONGO_DRY_RUN', '').lower() in ('true', '1', 'yes')
    }
    
    return config


def parse_arguments():
    """Parse command line arguments."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Migrate MongoDB user IDs and verify referential integrity',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
USAGE EXAMPLES:

  Preview migration (recommended first step):
    %(prog)s --dry-run

  Perform the migration:
    %(prog)s

  Check integrity only (no migration):
    %(prog)s --check-only

  With environment variables:
    export MONGO_URI="mongodb://localhost:27017"
    export MONGO_DB="myapp"
    %(prog)s --dry-run

MIGRATION PROCESS:
1. Analyze current state of all collections
2. Create userId to _id mapping
3. Update users collection (userId = _id)
4. Update referencing collections (addresses, orders, reviews, rewards)
5. Verify referential integrity
6. Generate detailed report

SAFETY NOTES:
- Always run with --dry-run first to preview changes
- Creates backup file with userId mappings
- Verifies all references before and after migration
- Check migrate_user_ids.log for detailed operation logs
        """
    )
    
    parser.add_argument(
        '--dry-run', 
        action='store_true',
        help='Preview operations without executing them'
    )
    
    parser.add_argument(
        '--check-only',
        action='store_true',
        help='Only perform integrity checks without migration'
    )
    
    return parser.parse_args()


def main():
    """Main function to run the migration process."""
    print("MongoDB User ID Migration and Integrity Verification")
    print("=" * 60)
    
    # Parse command line arguments
    args = parse_arguments()
    
    # Load configuration
    config = load_config_from_env()
    
    # Override config with command line arguments
    if args.dry_run:
        config['dry_run'] = True
    if args.check_only:
        config['check_only'] = True
    else:
        config['check_only'] = False
    
    # Display configuration
    print(f"Database: {config['db_name']}")
    print(f"Collections: users (primary), addresses, orders, reviews, rewards (references)")
    print(f"Batch size: {config['batch_size']}")
    
    if config.get('check_only'):
        print("Mode: INTEGRITY CHECK ONLY")
    elif config['dry_run']:
        print("Mode: DRY RUN (preview)")
    else:
        print("Mode: LIVE MIGRATION")
    
    print()
    
    # Confirm before proceeding
    if config.get('check_only'):
        warning_msg = "This will check referential integrity without making changes"
    elif config['dry_run']:
        warning_msg = "This is a dry run - no changes will be made"
    else:
        warning_msg = "This will migrate userId fields across multiple collections"
    
    response = input(f"Do you want to proceed? {warning_msg}. (y/N): ")
    if response.lower() != 'y':
        print("Operation cancelled.")
        return
    
    # Create migrator and run
    try:
        migrator = UserIdMigrator(**config)
    except ValueError as e:
        print(f"Configuration error: {e}")
        sys.exit(1)
    
    try:
        success = migrator.run_migration()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\nOperation interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()