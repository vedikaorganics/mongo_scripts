#!/usr/bin/env python3
"""
MongoDB Users Collection Name Field Update Script

This script updates the users collection by adding a 'name' field 
that concatenates firstName and lastName fields.
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

class UsersNameUpdater:
    def __init__(self, mongo_uri: str, db_name: str, 
                 batch_size: int = 1000,
                 dry_run: bool = False,
                 skip_existing: bool = True):
        """
        Initialize the users name field updater.
        
        Args:
            mongo_uri: MongoDB connection string
            db_name: Database name
            batch_size: Number of documents to process in each batch
            dry_run: If True, only show what would be updated without executing
            skip_existing: If True, skip documents that already have a name field
        """
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.collection_name = 'users'  # Hardcoded collection name
        self.batch_size = batch_size
        self.dry_run = dry_run
        self.skip_existing = skip_existing
        
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
                logging.FileHandler('users_name_update.log'),
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
        # Base query: documents with both firstName and lastName
        query = {
            'firstName': {'$exists': True, '$ne': None, '$ne': ''},
            'lastName': {'$exists': True, '$ne': None, '$ne': ''}
        }
        
        # If skipping existing, add condition that name field doesn't exist
        if self.skip_existing:
            query['name'] = {'$exists': False}
        
        return query

    def _create_name_field(self, first_name: str, last_name: str) -> str:
        """
        Create the name field by concatenating firstName and lastName.
        
        Args:
            first_name: First name
            last_name: Last name
            
        Returns:
            str: Concatenated full name
        """
        # Clean up the names (strip whitespace)
        first_name = str(first_name).strip() if first_name else ''
        last_name = str(last_name).strip() if last_name else ''
        
        # Concatenate with a space
        return f"{first_name} {last_name}".strip()

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
            
            # Documents with firstName
            stats['has_firstName'] = self.collection.count_documents({
                'firstName': {'$exists': True, '$ne': None, '$ne': ''}
            })
            
            # Documents with lastName
            stats['has_lastName'] = self.collection.count_documents({
                'lastName': {'$exists': True, '$ne': None, '$ne': ''}
            })
            
            # Documents with both firstName and lastName
            stats['has_both_names'] = self.collection.count_documents({
                'firstName': {'$exists': True, '$ne': None, '$ne': ''},
                'lastName': {'$exists': True, '$ne': None, '$ne': ''}
            })
            
            # Documents that already have a name field
            stats['has_name_field'] = self.collection.count_documents({
                'name': {'$exists': True}
            })
            
            # Documents that would be updated
            stats['to_be_updated'] = self.collection.count_documents(self._build_query())
            
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
        self.logger.info(f"Documents with firstName: {stats.get('has_firstName', 0):,}")
        self.logger.info(f"Documents with lastName: {stats.get('has_lastName', 0):,}")
        self.logger.info(f"Documents with both names: {stats.get('has_both_names', 0):,}")
        self.logger.info(f"Documents with existing name field: {stats.get('has_name_field', 0):,}")
        self.logger.info(f"Documents to be updated: {stats.get('to_be_updated', 0):,}")
        
        if self.skip_existing:
            self.logger.info("Mode: SKIP documents with existing name field")
        else:
            self.logger.info("Mode: UPDATE all documents (including those with existing name field)")
        
        self.logger.info("=" * 60)

    def update_users_name_field(self) -> bool:
        """
        Update the users collection with name field.
        
        Returns:
            bool: True if update successful, False otherwise
        """
        start_time = time.time()
        
        if not self.connect():
            return False
        
        try:
            # Analyze collection first
            stats = self.analyze_collection()
            self.display_analysis(stats)
            
            documents_to_update = stats.get('to_be_updated', 0)
            
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
            self.logger.info(f"Documents updated: {updated_count:,}")
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
            self.logger.info("-" * 40)
            
            for doc in cursor:
                first_name = doc.get('firstName', '')
                last_name = doc.get('lastName', '')
                new_name = self._create_name_field(first_name, last_name)
                
                self.logger.info(f"ID: {doc.get('_id')}")
                self.logger.info(f"  firstName: '{first_name}'")
                self.logger.info(f"  lastName: '{last_name}'")
                self.logger.info(f"  NEW name: '{new_name}'")
                self.logger.info("-" * 40)
                
        except Exception as e:
            self.logger.error(f"Error during preview: {e}")

    def _perform_updates(self, total_docs: int) -> int:
        """
        Perform the actual updates in batches.
        
        Args:
            total_docs: Total number of documents to update
            
        Returns:
            int: Number of documents successfully updated
        """
        updated_count = 0
        processed_count = 0
        
        try:
            query = self._build_query()
            cursor = self.collection.find(query).batch_size(self.batch_size)
            
            for doc in cursor:
                try:
                    # Create the name field
                    first_name = doc.get('firstName', '')
                    last_name = doc.get('lastName', '')
                    new_name = self._create_name_field(first_name, last_name)
                    
                    # Update the document
                    result = self.collection.update_one(
                        {'_id': doc['_id']},
                        {'$set': {'name': new_name}}
                    )
                    
                    if result.modified_count > 0:
                        updated_count += 1
                    
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
            
            return updated_count
            
        except Exception as e:
            self.logger.error(f"Error during batch updates: {e}")
            return updated_count


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
        description='Update MongoDB users collection with name field (firstName + lastName)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  %(prog)s --dry-run                    # Preview what would be updated
  %(prog)s --no-skip-existing           # Update all documents, even those with existing name field
  MONGO_DRY_RUN=true %(prog)s           # Set dry run via environment variable
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
        help='Skip documents that already have a name field (default)'
    )
    
    parser.add_argument(
        '--no-skip-existing',
        dest='skip_existing',
        action='store_false',
        help='Update all documents, even those with existing name field'
    )
    
    return parser.parse_args()


def main():
    """Main function to run the update process."""
    print("MongoDB Users Name Field Updater")
    print("=" * 50)
    
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
    print(f"Skip existing name fields: {'YES' if config['skip_existing'] else 'NO'}")
    print()
    
    # Confirm before proceeding
    warning_msg = (
        "This will update documents in the database" if not config['dry_run'] 
        else "This is a dry run - no changes will be made"
    )
    response = input(f"Do you want to proceed? {warning_msg}. (y/N): ")
    if response.lower() != 'y':
        print("Operation cancelled.")
        return
    
    # Create updater and run
    try:
        updater = UsersNameUpdater(**config)
    except ValueError as e:
        print(f"Configuration error: {e}")
        sys.exit(1)
    
    try:
        success = updater.update_users_name_field()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\nOperation interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()