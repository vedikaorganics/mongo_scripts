#!/usr/bin/env python3
"""
MongoDB Orders Collection - Rename offers.id to offers.offerId Script

This script renames the 'id' field to 'offerId' within the offers array 
in the orders collection.

USAGE:
======

Prerequisites:
- Python 3.6+
- pymongo library: pip install pymongo
- Write access to the target database

What it does:
1. Finds all orders with offers array containing objects with 'id' field
2. Renames 'id' to 'offerId' within each offer object
3. Skips offers that don't have an 'id' field
4. Provides detailed reporting of the operation

Basic Usage:
    python rename_offers_id_field.py

Dry Run (Preview):
    python rename_offers_id_field.py --dry-run

Environment Variables:
    MONGO_URI               MongoDB connection string
    MONGO_DB                Database name
    MONGO_BATCH_SIZE        Documents per batch (default: 1000)
    MONGO_DRY_RUN           Set to 'true' for dry run mode

Examples:
    # Preview the operation
    export MONGO_URI="mongodb://localhost:27017"
    export MONGO_DB="myapp"
    python rename_offers_id_field.py --dry-run

    # Perform the operation
    python rename_offers_id_field.py

Safety Features:
- Dry-run mode to preview changes
- Only processes offers that have an 'id' field
- Batch processing for large collections
- Detailed logging to rename_offers_id_field.log
- Comprehensive validation and reporting

IMPORTANT: This script modifies the offers array structure in orders documents.
Always run with --dry-run first to preview changes and verify the operation plan.
"""

import os
import sys
import logging
from typing import Dict, Any, List, Optional
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure, BulkWriteError
import time

class OffersFieldRenamer:
    def __init__(self, mongo_uri: str, db_name: str, 
                 batch_size: int = 1000,
                 dry_run: bool = False):
        """
        Initialize the offers field renamer.
        
        Args:
            mongo_uri: MongoDB connection string
            db_name: Database name
            batch_size: Number of documents to process in each batch
            dry_run: If True, only show what would be updated without executing
        """
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.batch_size = batch_size
        self.dry_run = dry_run
        
        # Collection name
        self.orders_collection = 'orders'
        
        self.client: Optional[MongoClient] = None
        self.db: Optional[Database] = None
        
        self._setup_logging()

    def _setup_logging(self):
        """Set up logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('rename_offers_id_field.log'),
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

    def analyze_orders(self) -> Dict[str, Any]:
        """
        Analyze the orders collection to understand the current state.
        
        Returns:
            Dict[str, Any]: Analysis results
        """
        self.logger.info("Analyzing orders collection...")
        
        try:
            orders_coll = self.db[self.orders_collection]
            
            analysis = {
                'total_orders': orders_coll.count_documents({}),
                'orders_with_offers': orders_coll.count_documents({
                    'offers': {'$exists': True, '$ne': []}
                }),
                'orders_with_offers_having_id': orders_coll.count_documents({
                    'offers.id': {'$exists': True}
                }),
                'orders_with_offers_having_offerId': orders_coll.count_documents({
                    'offers.offerId': {'$exists': True}
                })
            }
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"Error during analysis: {e}")
            return {}

    def display_analysis(self, analysis: Dict[str, Any]):
        """Display analysis results."""
        self.logger.info("\n" + "=" * 70)
        self.logger.info("ORDERS COLLECTION ANALYSIS")
        self.logger.info("=" * 70)
        
        self.logger.info(f"Total orders: {analysis.get('total_orders', 0):,}")
        self.logger.info(f"Orders with offers array: {analysis.get('orders_with_offers', 0):,}")
        self.logger.info(f"Orders with offers having 'id' field: {analysis.get('orders_with_offers_having_id', 0):,}")
        self.logger.info(f"Orders with offers having 'offerId' field: {analysis.get('orders_with_offers_having_offerId', 0):,}")
        
        self.logger.info("=" * 70)

    def rename_offers_id_field(self) -> Dict[str, int]:
        """
        Rename 'id' field to 'offerId' in offers array.
        
        Returns:
            Dict[str, int]: Operation results
        """
        try:
            self.logger.info("Starting offers field renaming operation...")
            
            orders_coll = self.db[self.orders_collection]
            
            results = {
                'orders_processed': 0,
                'orders_updated': 0,
                'offers_updated': 0,
                'errors': 0
            }
            
            # Find orders that have offers with 'id' field
            query = {'offers.id': {'$exists': True}}
            
            if self.dry_run:
                count = orders_coll.count_documents(query)
                self.logger.info(f"DRY RUN: Would process {count} orders with offers having 'id' field")
                results['orders_processed'] = count
                return results
            
            # Process orders in batches
            cursor = orders_coll.find(query).batch_size(self.batch_size)
            
            for order_doc in cursor:
                try:
                    results['orders_processed'] += 1
                    
                    # Process offers array
                    updated_offers = []
                    offers_updated_in_doc = 0
                    
                    for offer in order_doc.get('offers', []):
                        if isinstance(offer, dict) and 'id' in offer:
                            # Rename 'id' to 'offerId'
                            updated_offer = offer.copy()
                            updated_offer['offerId'] = updated_offer.pop('id')
                            updated_offers.append(updated_offer)
                            offers_updated_in_doc += 1
                        else:
                            # Keep offer as is if no 'id' field
                            updated_offers.append(offer)
                    
                    # Update the document if any offers were modified
                    if offers_updated_in_doc > 0:
                        update_result = orders_coll.update_one(
                            {'_id': order_doc['_id']},
                            {'$set': {'offers': updated_offers}}
                        )
                        
                        if update_result.modified_count > 0:
                            results['orders_updated'] += 1
                            results['offers_updated'] += offers_updated_in_doc
                            
                            if results['orders_processed'] % 100 == 0:
                                self.logger.info(f"Processed {results['orders_processed']} orders...")
                
                except Exception as e:
                    self.logger.error(f"Error processing order {order_doc.get('_id', 'unknown')}: {e}")
                    results['errors'] += 1
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error during field renaming operation: {e}")
            return {'orders_processed': 0, 'orders_updated': 0, 'offers_updated': 0, 'errors': 1}

    def verify_operation(self) -> Dict[str, Any]:
        """
        Verify the field renaming operation was successful.
        
        Returns:
            Dict[str, Any]: Verification results
        """
        self.logger.info("Verifying operation results...")
        
        try:
            orders_coll = self.db[self.orders_collection]
            
            verification = {
                'orders_with_offers_having_id': orders_coll.count_documents({
                    'offers.id': {'$exists': True}
                }),
                'orders_with_offers_having_offerId': orders_coll.count_documents({
                    'offers.offerId': {'$exists': True}
                })
            }
            
            return verification
            
        except Exception as e:
            self.logger.error(f"Error during verification: {e}")
            return {}

    def display_results(self, results: Dict[str, int], verification: Dict[str, Any]):
        """Display operation results and verification."""
        self.logger.info("\n" + "=" * 70)
        self.logger.info("OPERATION RESULTS")
        self.logger.info("=" * 70)
        
        self.logger.info(f"Orders processed: {results.get('orders_processed', 0):,}")
        self.logger.info(f"Orders updated: {results.get('orders_updated', 0):,}")
        self.logger.info(f"Total offers updated: {results.get('offers_updated', 0):,}")
        self.logger.info(f"Errors encountered: {results.get('errors', 0):,}")
        
        if not self.dry_run:
            self.logger.info("\nVERIFICATION:")
            self.logger.info(f"Orders with offers having 'id' field (should be 0): {verification.get('orders_with_offers_having_id', 0):,}")
            self.logger.info(f"Orders with offers having 'offerId' field: {verification.get('orders_with_offers_having_offerId', 0):,}")
        
        self.logger.info("=" * 70)

    def run_operation(self) -> bool:
        """
        Run the complete field renaming operation.
        
        Returns:
            bool: True if operation successful
        """
        start_time = time.time()
        
        if not self.connect():
            return False
        
        try:
            # Analyze current state
            initial_analysis = self.analyze_orders()
            self.display_analysis(initial_analysis)
            
            # Check if there's anything to process
            orders_to_process = initial_analysis.get('orders_with_offers_having_id', 0)
            if orders_to_process == 0:
                self.logger.info("No orders found with offers having 'id' field. Nothing to process.")
                return True
            
            # Perform field renaming
            results = self.rename_offers_id_field()
            
            # Verify operation (only if not dry run)
            verification = {}
            if not self.dry_run:
                verification = self.verify_operation()
            
            # Display results
            self.display_results(results, verification)
            
            # Final summary
            elapsed_time = time.time() - start_time
            self.logger.info(f"\n=== Operation Summary ===")
            self.logger.info(f"Operation completed in {elapsed_time:.2f} seconds")
            self.logger.info(f"Orders processed: {results.get('orders_processed', 0)}")
            self.logger.info(f"Offers updated: {results.get('offers_updated', 0)}")
            
            if self.dry_run:
                self.logger.info("DRY RUN completed - no actual changes made")
            else:
                self.logger.info("Field renaming operation completed successfully!")
                
                # Check if all 'id' fields were successfully renamed
                remaining_id_fields = verification.get('orders_with_offers_having_id', 0)
                if remaining_id_fields == 0:
                    self.logger.info("✓ All 'id' fields successfully renamed to 'offerId'")
                else:
                    self.logger.warning(f"⚠ {remaining_id_fields} orders still have offers with 'id' field")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Fatal error during operation: {e}")
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
        description='Rename offers.id field to offers.offerId in orders collection',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
USAGE EXAMPLES:

  Preview operation (recommended first step):
    %(prog)s --dry-run

  Perform the operation:
    %(prog)s

  With environment variables:
    export MONGO_URI="mongodb://localhost:27017"
    export MONGO_DB="myapp"
    %(prog)s --dry-run

OPERATION PROCESS:
1. Analyze orders collection structure
2. Find orders with offers array containing 'id' field
3. Rename 'id' to 'offerId' in each offer object
4. Skip offers that don't have 'id' field
5. Verify operation success
6. Generate detailed report

SAFETY NOTES:
- Always run with --dry-run first to preview changes
- Only processes offers that have an 'id' field
- Preserves all other offer data unchanged
- Check rename_offers_id_field.log for detailed operation logs
        """
    )
    
    parser.add_argument(
        '--dry-run', 
        action='store_true',
        help='Preview operations without executing them'
    )
    
    return parser.parse_args()


def main():
    """Main function to run the field renaming operation."""
    print("MongoDB Orders Collection - Rename offers.id to offers.offerId")
    print("=" * 65)
    
    # Parse command line arguments
    args = parse_arguments()
    
    # Load configuration
    config = load_config_from_env()
    
    # Override config with command line arguments
    if args.dry_run:
        config['dry_run'] = True
    
    # Display configuration
    print(f"Database: {config['db_name']}")
    print(f"Collection: orders")
    print(f"Operation: Rename offers.id → offers.offerId")
    print(f"Batch size: {config['batch_size']}")
    
    if config['dry_run']:
        print("Mode: DRY RUN (preview)")
    else:
        print("Mode: LIVE OPERATION")
    
    print()
    
    # Confirm before proceeding
    if config['dry_run']:
        warning_msg = "This will preview the field renaming operation without making changes"
    else:
        warning_msg = "This will rename 'id' to 'offerId' in offers array within orders collection"
    
    response = input(f"Do you want to proceed? {warning_msg}. (y/N): ")
    if response.lower() != 'y':
        print("Operation cancelled.")
        return
    
    # Create renamer and run
    try:
        renamer = OffersFieldRenamer(**config)
    except ValueError as e:
        print(f"Configuration error: {e}")
        sys.exit(1)
    
    try:
        success = renamer.run_operation()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\nOperation interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()