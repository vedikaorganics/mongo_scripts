#!/usr/bin/env python3
"""
MongoDB Users Phone Fields Copy Script

This script copies existing phone fields to new field names in the users collection:
- phone -> phoneNumber
- phoneVerification -> phoneNumberVerification

The original fields are left untouched.

USAGE:
======

Prerequisites:
- Python 3.6+
- pymongo library: pip install pymongo
- Write access to the target database

What it does:
- Copies 'phone' field value to new 'phoneNumber' field (exact copy)
- Copies 'phoneVerification' field value to new 'phoneNumberVerification' field (exact copy)
- Only processes documents that have the source fields
- Skips documents that already have the target fields (by default)
- Preserves original fields completely untouched

Basic Usage:
    python copy_phone_fields.py

Dry Run (Preview):
    python copy_phone_fields.py --dry-run

Environment Variables:
    MONGO_URI               MongoDB connection string
    MONGO_DB                Database name
    MONGO_BATCH_SIZE        Documents per batch (default: 1000)
    MONGO_DRY_RUN           Set to 'true' for dry run mode
    MONGO_SKIP_EXISTING     Set to 'false' to overwrite existing target fields

Examples:
    # Basic usage - copy phone fields in users collection
    export MONGO_URI="mongodb://localhost:27017"
    export MONGO_DB="myapp"
    python copy_phone_fields.py

    # Preview what would be copied
    python copy_phone_fields.py --dry-run

    # Overwrite existing target fields
    python copy_phone_fields.py --no-skip-existing

    # Larger batch size for better performance
    export MONGO_BATCH_SIZE="5000"
    python copy_phone_fields.py

Expected Document Transformation:
    Before: { "phone": "+1234567890", "phoneVerification": true }
    After:  { 
        "phone": "+1234567890", 
        "phoneVerification": true,
        "phoneNumber": "+1234567890",
        "phoneNumberVerification": true
    }

Field Mappings:
- phone → phoneNumber (exact copy, any data type)
- phoneVerification → phoneNumberVerification (exact copy, any data type)

Safety Features:
- Dry-run mode to preview changes
- Original fields are never modified or removed
- Exact value copying (no data transformation)
- Skips documents with existing target fields (by default)
- Detailed analysis before making changes
- Progress tracking and logging to phone_fields_copy.log

IMPORTANT: This script modifies the users collection by adding new fields.
Original phone and phoneVerification fields remain completely unchanged.
Use --dry-run first to preview changes.
"""

import os
import sys
import logging
from typing import Dict, Any, Optional
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure
import time

class PhoneFieldsCopier:
    def __init__(self, mongo_uri: str, db_name: str, 
                 batch_size: int = 1000,
                 dry_run: bool = False,
                 skip_existing: bool = True):
        """
        Initialize the phone fields copier.
        
        Args:
            mongo_uri: MongoDB connection string
            db_name: Database name
            batch_size: Number of documents to process in each batch
            dry_run: If True, only show what would be updated without executing
            skip_existing: If True, skip documents that already have the new fields
        """
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.collection_name = 'users'  # Hardcoded collection name
        self.batch_size = batch_size
        self.dry_run = dry_run
        self.skip_existing = skip_existing
        
        # Field mappings: old_field -> new_field
        self.field_mappings = {
            'phone': 'phoneNumber',
            'phoneVerification': 'phoneNumberVerification'
        }
        
        self.client: Optional[MongoClient] = None
        self.db: Optional[Database] = None
        self.collection: Optional[Collection] = None
        
        self._setup_logging()

    def _setup_logging(self):
        """Set up logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('phone_fields_copy.log'),
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
            self.collection = self.db[self.collection_name]
            
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

    def _build_query(self) -> Dict[str, Any]:
        """
        Build the query to find documents that need updating.
        
        Returns:
            Dict[str, Any]: MongoDB query
        """
        # Build query conditions
        conditions = []
        
        # Add conditions for documents that have the source fields
        for old_field, new_field in self.field_mappings.items():
            # Documents that have the old field
            has_old_field = {old_field: {'$exists': True}}
            
            if self.skip_existing:
                # Skip documents that already have the new field
                has_old_field[new_field] = {'$exists': False}
            
            conditions.append(has_old_field)
        
        # Use $or to find documents that meet any of the conditions
        if len(conditions) > 1:
            query = {'$or': conditions}
        elif len(conditions) == 1:
            query = conditions[0]
        else:
            query = {}
        
        return query

    def analyze_collection(self) -> Dict[str, int]:
        """
        Analyze the collection to understand the data structure.
        
        Returns:
            Dict[str, int]: Statistics about the collection
        """
        try:
            stats = {}
            
            # Total documents
            stats['total_documents'] = self.collection.count_documents({})
            
            # Analyze each field mapping
            for old_field, new_field in self.field_mappings.items():
                # Documents with source field
                stats[f'has_{old_field}'] = self.collection.count_documents({
                    old_field: {'$exists': True}
                })
                
                # Documents with target field
                stats[f'has_{new_field}'] = self.collection.count_documents({
                    new_field: {'$exists': True}
                })
                
                # Documents that would be updated for this field
                field_query = {old_field: {'$exists': True}}
                if self.skip_existing:
                    field_query[new_field] = {'$exists': False}
                
                stats[f'to_update_{old_field}'] = self.collection.count_documents(field_query)
            
            # Total documents that would be updated
            stats['total_to_update'] = self.collection.count_documents(self._build_query())
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Error analyzing collection: {e}")
            return {}

    def display_analysis(self, stats: Dict[str, int]):
        """
        Display analysis results.
        
        Args:
            stats: Collection statistics
        """
        self.logger.info("\n" + "=" * 60)
        self.logger.info("COLLECTION ANALYSIS")
        self.logger.info("=" * 60)
        self.logger.info(f"Total documents: {stats.get('total_documents', 0):,}")
        
        for old_field, new_field in self.field_mappings.items():
            self.logger.info(f"\nField mapping: {old_field} -> {new_field}")
            self.logger.info(f"  Documents with '{old_field}': {stats.get(f'has_{old_field}', 0):,}")
            self.logger.info(f"  Documents with '{new_field}': {stats.get(f'has_{new_field}', 0):,}")
            self.logger.info(f"  To be updated for this field: {stats.get(f'to_update_{old_field}', 0):,}")
        
        self.logger.info(f"\nTotal documents to be updated: {stats.get('total_to_update', 0):,}")
        
        if self.skip_existing:
            self.logger.info("Mode: SKIP documents with existing target fields")
        else:
            self.logger.info("Mode: UPDATE all documents (including those with existing target fields)")
        
        self.logger.info("=" * 60)

    def copy_phone_fields(self) -> bool:
        """
        Copy phone fields in the users collection.
        
        Returns:
            bool: True if copy successful, False otherwise
        """
        start_time = time.time()
        
        if not self.connect():
            return False
        
        try:
            # Analyze collection first
            stats = self.analyze_collection()
            self.display_analysis(stats)
            
            documents_to_update = stats.get('total_to_update', 0)
            
            if documents_to_update == 0:
                self.logger.info("No documents need updating.")
                return True
            
            if self.dry_run:
                self.logger.info(f"\nDRY RUN: Would update {documents_to_update:,} documents")
                self._preview_updates()
                return True
            
            # Perform the update
            updated_count = self._perform_updates(documents_to_update)
            
            # Summary
            elapsed_time = time.time() - start_time
            self.logger.info(f"\n=== Update Summary ===")
            self.logger.info(f"Documents processed: {updated_count:,}")
            self.logger.info(f"Time elapsed: {elapsed_time:.2f} seconds")
            
            if updated_count == documents_to_update:
                self.logger.info("All documents updated successfully!")
                return True
            else:
                self.logger.warning(f"Some documents failed to update: {documents_to_update - updated_count} remaining")
                return False
            
        except Exception as e:
            self.logger.error(f"Fatal error during update: {e}")
            return False
        finally:
            self.disconnect()

    def _preview_updates(self):
        """Preview what updates would be made in dry-run mode."""
        try:
            query = self._build_query()
            cursor = self.collection.find(query).limit(5)  # Show first 5 examples
            
            self.logger.info("\nPreview of updates (first 5 documents):")
            self.logger.info("-" * 50)
            
            for doc in cursor:
                self.logger.info(f"Document ID: {doc.get('_id')}")
                
                for old_field, new_field in self.field_mappings.items():
                    old_value = doc.get(old_field)
                    existing_new_value = doc.get(new_field)
                    
                    if old_value is not None:
                        if existing_new_value is None or not self.skip_existing:
                            self.logger.info(f"  {old_field}: {old_value} -> {new_field}: {old_value}")
                        else:
                            self.logger.info(f"  {old_field}: {old_value} (SKIP - {new_field} already exists: {existing_new_value})")
                    else:
                        self.logger.info(f"  {old_field}: Not present")
                
                self.logger.info("-" * 50)
                
        except Exception as e:
            self.logger.error(f"Error during preview: {e}")

    def _perform_updates(self, total_docs: int) -> int:
        """
        Perform the actual updates in batches.
        
        Args:
            total_docs: Total number of documents to update
            
        Returns:
            int: Number of documents successfully processed
        """
        processed_count = 0
        
        try:
            query = self._build_query()
            cursor = self.collection.find(query).batch_size(self.batch_size)
            
            for doc in cursor:
                try:
                    # Build update operations for this document
                    update_operations = {}
                    
                    for old_field, new_field in self.field_mappings.items():
                        old_value = doc.get(old_field)
                        existing_new_value = doc.get(new_field)
                        
                        # Only copy if source field exists and target field should be updated
                        if old_value is not None:
                            if existing_new_value is None or not self.skip_existing:
                                update_operations[new_field] = old_value
                    
                    # Only perform update if there are fields to update
                    if update_operations:
                        result = self.collection.update_one(
                            {'_id': doc['_id']},
                            {'$set': update_operations}
                        )
                        
                        if result.modified_count > 0:
                            fields_updated = ', '.join(update_operations.keys())
                            self.logger.debug(f"Updated document {doc['_id']} with fields: {fields_updated}")
                    
                    processed_count += 1
                    
                    # Progress update
                    if processed_count % self.batch_size == 0:
                        progress = (processed_count / total_docs) * 100
                        self.logger.info(f"Progress: {processed_count:,}/{total_docs:,} ({progress:.1f}%)")
                
                except Exception as e:
                    self.logger.error(f"Error updating document {doc.get('_id')}: {e}")
                    continue
            
            # Final progress update
            if processed_count % self.batch_size != 0:
                progress = (processed_count / total_docs) * 100
                self.logger.info(f"Progress: {processed_count:,}/{total_docs:,} ({progress:.1f}%)")
            
            return processed_count
            
        except Exception as e:
            self.logger.error(f"Error during batch updates: {e}")
            return processed_count


def load_config_from_env() -> Dict[str, Any]:
    """Load configuration from environment variables."""
    config = {
        'mongo_uri': os.getenv('MONGO_URI', 'mongodb://localhost:27017'),
        'db_name': os.getenv('MONGO_DB', 'test'),
        'batch_size': int(os.getenv('MONGO_BATCH_SIZE', '1000')),
        'dry_run': os.getenv('MONGO_DRY_RUN', '').lower() in ('true', '1', 'yes'),
        'skip_existing': os.getenv('MONGO_SKIP_EXISTING', 'true').lower() in ('true', '1', 'yes')
    }
    
    return config


def parse_arguments():
    """Parse command line arguments."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Copy phone fields in MongoDB users collection (phone -> phoneNumber, phoneVerification -> phoneNumberVerification)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
USAGE EXAMPLES:

  Preview changes (recommended first step):
    %(prog)s --dry-run

  Basic copy (skip existing target fields):
    %(prog)s

  Overwrite existing target fields:
    %(prog)s --no-skip-existing

  With environment variables:
    export MONGO_URI="mongodb://localhost:27017"
    export MONGO_DB="myapp"
    %(prog)s --dry-run

  Larger batch size for performance:
    export MONGO_BATCH_SIZE="5000"
    %(prog)s

FIELD MAPPINGS:
  phone → phoneNumber (exact copy)
  phoneVerification → phoneNumberVerification (exact copy)

DOCUMENT TRANSFORMATION:
  Before: { "phone": "+1234567890", "phoneVerification": true }
  After:  { 
    "phone": "+1234567890", 
    "phoneVerification": true,
    "phoneNumber": "+1234567890",
    "phoneNumberVerification": true
  }

SAFETY NOTES:
- Always run with --dry-run first to preview changes
- Original phone fields are never modified or removed
- Only documents with source fields are processed
- Check phone_fields_copy.log for detailed operation logs
        """
    )
    
    parser.add_argument(
        '--dry-run', 
        action='store_true',
        help='Preview operations without executing them'
    )
    
    parser.add_argument(
        '--skip-existing',
        action='store_true',
        default=True,
        help='Skip documents that already have target fields (default)'
    )
    
    parser.add_argument(
        '--no-skip-existing',
        dest='skip_existing',
        action='store_false',
        help='Update all documents, even those with existing target fields'
    )
    
    return parser.parse_args()


def main():
    """Main function to run the copy process."""
    print("MongoDB Users Phone Fields Copier")
    print("=" * 50)
    print("Field mappings:")
    print("  phone -> phoneNumber")
    print("  phoneVerification -> phoneNumberVerification")
    print()
    
    # Parse command line arguments
    args = parse_arguments()
    
    # Load configuration
    config = load_config_from_env()
    
    # Override config with command line arguments
    if args.dry_run:
        config['dry_run'] = True
    if hasattr(args, 'skip_existing'):
        config['skip_existing'] = args.skip_existing
    
    # Display configuration
    print(f"Database: {config['db_name']}")
    print(f"Collection: users (hardcoded)")
    print(f"Batch size: {config['batch_size']}")
    print(f"Mode: {'DRY RUN' if config['dry_run'] else 'LIVE EXECUTION'}")
    print(f"Skip existing target fields: {'YES' if config['skip_existing'] else 'NO'}")
    print()
    
    # Confirm before proceeding
    warning_msg = (
        "This will copy phone fields in the database" if not config['dry_run'] 
        else "This is a dry run - no changes will be made"
    )
    response = input(f"Do you want to proceed? {warning_msg}. (y/N): ")
    if response.lower() != 'y':
        print("Operation cancelled.")
        return
    
    # Create copier and run
    try:
        copier = PhoneFieldsCopier(**config)
    except ValueError as e:
        print(f"Configuration error: {e}")
        sys.exit(1)
    
    try:
        success = copier.copy_phone_fields()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\nOperation interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()