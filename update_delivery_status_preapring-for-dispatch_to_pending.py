#!/usr/bin/env python3
"""
MongoDB Orders Collection - Update Delivery Status Script

This script updates orders where deliveryStatus is 'PREPARING_FOR_DISPATCH' 
to 'PREPARING' in the orders collection.

USAGE:
======

Prerequisites:
- Python 3.6+
- pymongo library: pip install pymongo
- Write access to the target database

What it does:
1. Finds all orders with deliveryStatus = 'PREPARING_FOR_DISPATCH'
2. Updates these orders to have deliveryStatus = 'PREPARING'
3. Provides detailed reporting of the operation
4. Batch processing for efficient updates

Basic Usage:
    python update_delivery_status_preapring-for-dispatch_to_pending.py

Dry Run (Preview):
    python update_delivery_status_preapring-for-dispatch_to_pending.py --dry-run

Environment Variables:
    MONGO_URI               MongoDB connection string
    MONGO_DB                Database name
    MONGO_BATCH_SIZE        Documents per batch (default: 1000)
    MONGO_DRY_RUN           Set to 'true' for dry run mode

Examples:
    # Preview the operation
    export MONGO_URI="mongodb://localhost:27017"
    export MONGO_DB="myapp"
    python update_delivery_status_preapring-for-dispatch_to_pending.py --dry-run

    # Perform the operation
    python update_delivery_status_preapring-for-dispatch_to_pending.py

    # Larger batch size for better performance
    export MONGO_BATCH_SIZE="5000"
    python update_delivery_status_preapring-for-dispatch_to_pending.py

Safety Features:
- Dry-run mode to preview changes
- Only processes orders with deliveryStatus = 'PREPARING_FOR_DISPATCH'
- Batch processing for large collections
- Detailed logging to delivery_status_update.log
- Comprehensive validation and reporting

IMPORTANT: This script modifies the deliveryStatus field in orders documents.
Always run with --dry-run first to preview changes and verify the operation plan.
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

class DeliveryStatusUpdater:
    def __init__(self, mongo_uri: str, db_name: str, 
                 batch_size: int = 1000,
                 dry_run: bool = False):
        """
        Initialize the delivery status updater.
        
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
        
        # Collection name and status values
        self.orders_collection = 'orders'
        self.old_status = 'PREPARING_FOR_DISPATCH'
        self.new_status = 'PREPARING'
        
        self.client: Optional[MongoClient] = None
        self.db: Optional[Database] = None
        
        self._setup_logging()

    def _setup_logging(self):
        """Set up logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('delivery_status_update.log'),
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
        Analyze the orders collection to understand the current delivery status distribution.
        
        Returns:
            Dict[str, Any]: Analysis results
        """
        self.logger.info("Analyzing orders collection...")
        
        try:
            orders_coll = self.db[self.orders_collection]
            
            analysis = {
                'total_orders': orders_coll.count_documents({}),
                'orders_with_preparing_for_dispatch': orders_coll.count_documents({
                    'deliveryStatus': self.old_status
                }),
                'orders_with_preparing': orders_coll.count_documents({
                    'deliveryStatus': self.new_status
                }),
                'orders_with_delivery_status': orders_coll.count_documents({
                    'deliveryStatus': {'$exists': True}
                })
            }
            
            # Get sample of other delivery statuses for reference
            pipeline = [
                {'$match': {'deliveryStatus': {'$exists': True}}},
                {'$group': {'_id': '$deliveryStatus', 'count': {'$sum': 1}}},
                {'$sort': {'count': -1}},
                {'$limit': 10}
            ]
            
            status_distribution = list(orders_coll.aggregate(pipeline))
            analysis['status_distribution'] = status_distribution
            
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
        self.logger.info(f"Orders with deliveryStatus field: {analysis.get('orders_with_delivery_status', 0):,}")
        self.logger.info(f"Orders with 'PREPARING_FOR_DISPATCH' status: {analysis.get('orders_with_preparing_for_dispatch', 0):,}")
        self.logger.info(f"Orders with 'PREPARING' status: {analysis.get('orders_with_preparing', 0):,}")
        
        # Display delivery status distribution
        if analysis.get('status_distribution'):
            self.logger.info("\nDelivery Status Distribution:")
            for status in analysis['status_distribution']:
                self.logger.info(f"  {status['_id']}: {status['count']:,} orders")
        
        self.logger.info("=" * 70)

    def update_delivery_status(self) -> Dict[str, int]:
        """
        Update deliveryStatus from 'PREPARING_FOR_DISPATCH' to 'PREPARING'.
        
        Returns:
            Dict[str, int]: Operation results
        """
        try:
            self.logger.info("Starting delivery status update operation...")
            
            orders_coll = self.db[self.orders_collection]
            
            results = {
                'orders_processed': 0,
                'orders_updated': 0,
                'errors': 0
            }
            
            # Query for orders with the old status
            query = {'deliveryStatus': self.old_status}
            
            if self.dry_run:
                count = orders_coll.count_documents(query)
                self.logger.info(f"DRY RUN: Would update {count} orders from '{self.old_status}' to '{self.new_status}'")
                results['orders_processed'] = count
                return results
            
            # Process orders in batches
            cursor = orders_coll.find(query, {'_id': 1}).batch_size(self.batch_size)
            
            batch = []
            for order_doc in cursor:
                batch.append(order_doc['_id'])
                results['orders_processed'] += 1
                
                # Process batch when it reaches the specified size
                if len(batch) >= self.batch_size:
                    updated_count = self._process_batch(orders_coll, batch)
                    results['orders_updated'] += updated_count
                    
                    if results['orders_processed'] % (self.batch_size * 5) == 0:
                        self.logger.info(f"Processed {results['orders_processed']} orders...")
                    
                    batch = []
            
            # Process remaining orders in the last batch
            if batch:
                updated_count = self._process_batch(orders_coll, batch)
                results['orders_updated'] += updated_count
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error during status update operation: {e}")
            return {'orders_processed': 0, 'orders_updated': 0, 'errors': 1}

    def _process_batch(self, collection: Collection, order_ids: list) -> int:
        """
        Process a batch of order IDs for status update.
        
        Args:
            collection: MongoDB collection object
            order_ids: List of order IDs to update
        
        Returns:
            int: Number of documents updated
        """
        try:
            update_result = collection.update_many(
                {'_id': {'$in': order_ids}},
                {'$set': {'deliveryStatus': self.new_status}}
            )
            
            return update_result.modified_count
            
        except Exception as e:
            self.logger.error(f"Error processing batch: {e}")
            return 0

    def verify_operation(self) -> Dict[str, Any]:
        """
        Verify the delivery status update operation was successful.
        
        Returns:
            Dict[str, Any]: Verification results
        """
        self.logger.info("Verifying operation results...")
        
        try:
            orders_coll = self.db[self.orders_collection]
            
            verification = {
                'orders_with_old_status': orders_coll.count_documents({
                    'deliveryStatus': self.old_status
                }),
                'orders_with_new_status': orders_coll.count_documents({
                    'deliveryStatus': self.new_status
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
        self.logger.info(f"Errors encountered: {results.get('errors', 0):,}")
        
        if not self.dry_run:
            self.logger.info("\nVERIFICATION:")
            self.logger.info(f"Orders with '{self.old_status}' status (should be 0): {verification.get('orders_with_old_status', 0):,}")
            self.logger.info(f"Orders with '{self.new_status}' status: {verification.get('orders_with_new_status', 0):,}")
        
        self.logger.info("=" * 70)

    def run_operation(self) -> bool:
        """
        Run the complete delivery status update operation.
        
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
            orders_to_process = initial_analysis.get('orders_with_preparing_for_dispatch', 0)
            if orders_to_process == 0:
                self.logger.info(f"No orders found with deliveryStatus '{self.old_status}'. Nothing to process.")
                return True
            
            # Perform status update
            results = self.update_delivery_status()
            
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
            self.logger.info(f"Orders updated: {results.get('orders_updated', 0)}")
            
            if self.dry_run:
                self.logger.info("DRY RUN completed - no actual changes made")
            else:
                self.logger.info("Delivery status update operation completed successfully!")
                
                # Check if all old statuses were successfully updated
                remaining_old_status = verification.get('orders_with_old_status', 0)
                if remaining_old_status == 0:
                    self.logger.info(f"✓ All '{self.old_status}' statuses successfully updated to '{self.new_status}'")
                else:
                    self.logger.warning(f"⚠ {remaining_old_status} orders still have '{self.old_status}' status")
            
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
        description='Update deliveryStatus from PREPARING_FOR_DISPATCH to PREPARING in orders collection',
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
1. Analyze orders collection delivery status distribution
2. Find orders with deliveryStatus = 'PREPARING_FOR_DISPATCH'
3. Update these orders to deliveryStatus = 'PREPARING'
4. Verify operation success
5. Generate detailed report

SAFETY NOTES:
- Always run with --dry-run first to preview changes
- Only processes orders with deliveryStatus = 'PREPARING_FOR_DISPATCH'
- Preserves all other order data unchanged
- Check delivery_status_update.log for detailed operation logs
        """
    )
    
    parser.add_argument(
        '--dry-run', 
        action='store_true',
        help='Preview operations without executing them'
    )
    
    return parser.parse_args()


def main():
    """Main function to run the delivery status update operation."""
    print("MongoDB Orders Collection - Update Delivery Status")
    print("=" * 50)
    
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
    print(f"Operation: Update deliveryStatus PREPARING_FOR_DISPATCH → PREPARING")
    print(f"Batch size: {config['batch_size']}")
    
    if config['dry_run']:
        print("Mode: DRY RUN (preview)")
    else:
        print("Mode: LIVE OPERATION")
    
    print()
    
    # Confirm before proceeding
    if config['dry_run']:
        warning_msg = "This will preview the delivery status update operation without making changes"
    else:
        warning_msg = "This will update deliveryStatus from 'PREPARING_FOR_DISPATCH' to 'PREPARING' in orders collection"
    
    response = input(f"Do you want to proceed? {warning_msg}. (y/N): ")
    if response.lower() != 'y':
        print("Operation cancelled.")
        return
    
    # Create updater and run
    try:
        updater = DeliveryStatusUpdater(**config)
    except ValueError as e:
        print(f"Configuration error: {e}")
        sys.exit(1)
    
    try:
        success = updater.run_operation()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\nOperation interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()