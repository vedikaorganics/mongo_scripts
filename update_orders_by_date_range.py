#!/usr/bin/env python3
"""
MongoDB Orders Collection - Date Range Status Update Script

This script updates orders based on date range, payment status, and delivery status criteria.
It changes deliveryStatus from 'PENDING', 'DISPATCHED', or 'PREPARING' to 'DELIVERED' for orders with:
- paymentStatus: 'CASH_ON_DELIVERY' or 'PAID' 
- createdAt: within the specified date range
- deliveryStatus: 'PENDING', 'DISPATCHED', or 'PREPARING'

USAGE:
======

Prerequisites:
- Python 3.6+
- pymongo library: pip install pymongo
- Write access to the target database

What it does:
1. Finds orders with createdAt between start_date and end_date (inclusive)
2. Filters for paymentStatus in ['CASH_ON_DELIVERY', 'PAID']
3. Filters for deliveryStatus in ['PENDING', 'DISPATCHED', 'PREPARING'] 
4. Updates these orders to set deliveryStatus = 'DELIVERED'
5. Provides detailed reporting of the operation

Basic Usage:
    python update_orders_by_date_range.py --start-date 2024-01-01 --end-date 2024-01-31

Dry Run (Preview):
    python update_orders_by_date_range.py --start-date 2024-01-01 --end-date 2024-01-31 --dry-run

REQUIRED ARGUMENTS:
    --start-date            Start date (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)
    --end-date              End date (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)

Environment Variables:
    MONGO_URI               MongoDB connection string
    MONGO_DB                Database name
    MONGO_BATCH_SIZE        Documents per batch (default: 1000)
    MONGO_DRY_RUN           Set to 'true' for dry run mode

Examples:
    # Preview orders from January 2024
    python update_orders_by_date_range.py --start-date 2024-01-01 --end-date 2024-01-31 --dry-run

    # Update orders from a specific week
    python update_orders_by_date_range.py --start-date 2024-01-15 --end-date 2024-01-21

    # With specific timestamps
    python update_orders_by_date_range.py --start-date "2024-01-01T00:00:00" --end-date "2024-01-31T23:59:59"

    # Larger batch size for better performance
    export MONGO_BATCH_SIZE="5000"
    python update_orders_by_date_range.py --start-date 2024-01-01 --end-date 2024-01-31

Date Formats Supported:
    YYYY-MM-DD              2024-01-01
    YYYY-MM-DDTHH:MM:SS     2024-01-01T14:30:00
    ISO 8601 with timezone  2024-01-01T14:30:00Z

Query Criteria:
    createdAt: {$gte: start_date, $lte: end_date}
    paymentStatus: {$in: ['CASH_ON_DELIVERY', 'PAID']}
    deliveryStatus: {$in: ['PENDING', 'DISPATCHED', 'PREPARING']}
    
Update Operation:
    Set deliveryStatus = 'DELIVERED' for matching orders

Safety Features:
- Dry-run mode to preview changes
- Required date arguments (no defaults to prevent accidents)
- Date range validation (start must be before end)
- Batch processing for large collections
- Detailed logging to orders_date_range_update.log
- Comprehensive validation and reporting

IMPORTANT: This script modifies orderStatus fields in orders documents.
Always run with --dry-run first to preview changes and verify the operation scope.
"""

import os
import sys
import logging
from typing import Dict, Any, Optional
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure
from datetime import datetime, timezone
import time

class OrderDateRangeUpdater:
    def __init__(self, mongo_uri: str, db_name: str, 
                 start_date: str, end_date: str,
                 batch_size: int = 1000,
                 dry_run: bool = False):
        """
        Initialize the order date range updater.
        
        Args:
            mongo_uri: MongoDB connection string
            db_name: Database name
            start_date: Start date string (YYYY-MM-DD or ISO format)
            end_date: End date string (YYYY-MM-DD or ISO format) 
            batch_size: Number of documents to process in each batch
            dry_run: If True, only show what would be updated without executing
        """
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.start_date_str = start_date
        self.end_date_str = end_date
        self.batch_size = batch_size
        self.dry_run = dry_run
        
        # Collection name and status values
        self.orders_collection = 'orders'
        self.target_payment_statuses = ['CASH_ON_DELIVERY', 'PAID']
        self.target_delivery_statuses = ['PENDING', 'DISPATCHED', 'PREPARING'] 
        self.new_delivery_status = 'DELIVERED'
        
        self.client: Optional[MongoClient] = None
        self.db: Optional[Database] = None
        
        # Parse and validate dates during initialization
        self.start_date = self.parse_date(start_date, is_end_date=False)
        self.end_date = self.parse_date(end_date, is_end_date=True)
        self.validate_dates()
        
        self._setup_logging()

    def parse_date(self, date_str: str, is_end_date: bool = False) -> datetime:
        """
        Parse date string into datetime object.
        
        Args:
            date_str: Date string in various formats
            is_end_date: If True, sets time to end of day (23:59:59.999999) for YYYY-MM-DD format
            
        Returns:
            datetime: Parsed datetime object in UTC
            
        Raises:
            ValueError: If date format is invalid
        """
        try:
            # Try YYYY-MM-DD format first
            if len(date_str) == 10 and date_str.count('-') == 2:
                dt = datetime.strptime(date_str, '%Y-%m-%d')
                if is_end_date:
                    # Set to end of day (23:59:59.999999) for inclusive end date
                    dt = dt.replace(hour=23, minute=59, second=59, microsecond=999999)
                return dt.replace(tzinfo=timezone.utc)
            
            # Try YYYY-MM-DDTHH:MM:SS format
            elif 'T' in date_str:
                if date_str.endswith('Z'):
                    return datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
                else:
                    return datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S').replace(tzinfo=timezone.utc)
            
            else:
                raise ValueError(f"Unsupported date format: {date_str}")
                
        except ValueError as e:
            raise ValueError(f"Invalid date format '{date_str}'. Supported formats: YYYY-MM-DD, YYYY-MM-DDTHH:MM:SS, YYYY-MM-DDTHH:MM:SSZ")

    def validate_dates(self):
        """Validate that start_date is before end_date."""
        if self.start_date >= self.end_date:
            raise ValueError(f"Start date ({self.start_date_str}) must be before end date ({self.end_date_str})")

    def _setup_logging(self):
        """Set up logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('orders_date_range_update.log'),
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
        Analyze the orders collection for the given date range and criteria.
        
        Returns:
            Dict[str, Any]: Analysis results
        """
        self.logger.info("Analyzing orders collection...")
        
        try:
            orders_coll = self.db[self.orders_collection]
            
            # Base query for date range
            date_query = {
                'createdAt': {
                    '$gte': self.start_date,
                    '$lte': self.end_date
                }
            }
            
            # Full matching query
            matching_query = {
                **date_query,
                'paymentStatus': {'$in': self.target_payment_statuses},
                'deliveryStatus': {'$in': self.target_delivery_statuses}
            }
            
            analysis = {
                'total_orders': orders_coll.count_documents({}),
                'orders_in_date_range': orders_coll.count_documents(date_query),
                'orders_matching_criteria': orders_coll.count_documents(matching_query),
                'date_range_start': self.start_date,
                'date_range_end': self.end_date
            }
            
            # Breakdown by payment status within matching criteria
            payment_breakdown = {}
            for payment_status in self.target_payment_statuses:
                payment_query = {
                    **date_query,
                    'paymentStatus': payment_status,
                    'deliveryStatus': {'$in': self.target_delivery_statuses}
                }
                payment_breakdown[payment_status] = orders_coll.count_documents(payment_query)
            
            analysis['payment_status_breakdown'] = payment_breakdown
            
            # Breakdown by delivery status within matching criteria
            delivery_status_breakdown = {}
            for delivery_status in self.target_delivery_statuses:
                delivery_query = {
                    **date_query,
                    'paymentStatus': {'$in': self.target_payment_statuses},
                    'deliveryStatus': delivery_status
                }
                delivery_status_breakdown[delivery_status] = orders_coll.count_documents(delivery_query)
            
            analysis['delivery_status_breakdown'] = delivery_status_breakdown
            
            # Orders already delivered in date range
            delivered_query = {
                **date_query,
                'paymentStatus': {'$in': self.target_payment_statuses},
                'deliveryStatus': 'DELIVERED'
            }
            analysis['already_delivered'] = orders_coll.count_documents(delivered_query)
            
            # DEBUG: Individual criteria breakdowns to identify the issue
            debug_info = {}
            
            # Orders in date range with any payment status (to see what payment statuses exist)
            debug_info['date_range_payment_status'] = list(orders_coll.aggregate([
                {'$match': date_query},
                {'$group': {'_id': '$paymentStatus', 'count': {'$sum': 1}}},
                {'$sort': {'count': -1}}
            ]))
            
            # Orders in date range with any delivery status (to see what delivery statuses exist)
            debug_info['date_range_delivery_status'] = list(orders_coll.aggregate([
                {'$match': date_query},
                {'$group': {'_id': '$deliveryStatus', 'count': {'$sum': 1}}},
                {'$sort': {'count': -1}}
            ]))
            
            # Orders in date range with target payment status (any delivery status)
            debug_info['date_range_with_target_payment'] = orders_coll.count_documents({
                **date_query,
                'paymentStatus': {'$in': self.target_payment_statuses}
            })
            
            # Orders in date range with target delivery status (any payment status)
            debug_info['date_range_with_target_delivery'] = orders_coll.count_documents({
                **date_query,
                'deliveryStatus': {'$in': self.target_delivery_statuses}
            })
            
            # Sample documents from date range
            sample_docs = list(orders_coll.find(date_query, {
                'orderId': 1,
                'paymentStatus': 1,
                'deliveryStatus': 1,
                'createdAt': 1
            }).limit(5))
            debug_info['sample_documents'] = sample_docs
            
            # Date range debugging
            debug_info['parsed_start_date'] = self.start_date
            debug_info['parsed_end_date'] = self.end_date
            
            analysis['debug_info'] = debug_info
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"Error during analysis: {e}")
            return {}

    def display_analysis(self, analysis: Dict[str, Any]):
        """Display analysis results."""
        self.logger.info("\n" + "=" * 80)
        self.logger.info("ORDERS COLLECTION ANALYSIS")
        self.logger.info("=" * 80)
        
        self.logger.info(f"Date Range: {self.start_date_str} to {self.end_date_str}")
        self.logger.info(f"Total orders in database: {analysis.get('total_orders', 0):,}")
        self.logger.info(f"Orders in date range: {analysis.get('orders_in_date_range', 0):,}")
        self.logger.info(f"Orders matching all criteria: {analysis.get('orders_matching_criteria', 0):,}")
        
        self.logger.info(f"\nTarget Criteria:")
        self.logger.info(f"  Payment Status: {', '.join(self.target_payment_statuses)}")
        self.logger.info(f"  Delivery Status: {', '.join(self.target_delivery_statuses)}")
        self.logger.info(f"  Will update to: {self.new_delivery_status}")
        
        # Payment status breakdown
        if analysis.get('payment_status_breakdown'):
            self.logger.info(f"\nBreakdown by Payment Status (matching orders):")
            for status, count in analysis['payment_status_breakdown'].items():
                self.logger.info(f"  {status}: {count:,} orders")
        
        # Delivery status breakdown
        if analysis.get('delivery_status_breakdown'):
            self.logger.info(f"\nBreakdown by Delivery Status (matching orders):")
            for status, count in analysis['delivery_status_breakdown'].items():
                self.logger.info(f"  {status}: {count:,} orders")
        
        self.logger.info(f"\nAlready DELIVERED in date range: {analysis.get('already_delivered', 0):,}")
        
        # DEBUG INFORMATION
        debug_info = analysis.get('debug_info', {})
        if debug_info:
            self.logger.info("\n" + "=" * 80)
            self.logger.info("DEBUG INFORMATION")
            self.logger.info("=" * 80)
            
            # Show parsed date range
            self.logger.info(f"Parsed Start Date: {debug_info.get('parsed_start_date')}")
            self.logger.info(f"Parsed End Date: {debug_info.get('parsed_end_date')}")
            
            # Show actual payment statuses in date range
            self.logger.info(f"\nActual Payment Statuses in Date Range:")
            payment_statuses = debug_info.get('date_range_payment_status', [])
            if payment_statuses:
                for status in payment_statuses:
                    self.logger.info(f"  {status['_id']}: {status['count']:,} orders")
            else:
                self.logger.info("  No payment statuses found")
                
            # Show actual delivery statuses in date range
            self.logger.info(f"\nActual Delivery Statuses in Date Range:")
            delivery_statuses = debug_info.get('date_range_delivery_status', [])
            if delivery_statuses:
                for status in delivery_statuses:
                    self.logger.info(f"  {status['_id']}: {status['count']:,} orders")
            else:
                self.logger.info("  No delivery statuses found")
                
            # Show partial matching results
            self.logger.info(f"\nPartial Matching Results:")
            self.logger.info(f"  Orders in date range with target payment status: {debug_info.get('date_range_with_target_payment', 0):,}")
            self.logger.info(f"  Orders in date range with target delivery status: {debug_info.get('date_range_with_target_delivery', 0):,}")
            
            # Show sample documents
            sample_docs = debug_info.get('sample_documents', [])
            if sample_docs:
                self.logger.info(f"\nSample Documents from Date Range:")
                for i, doc in enumerate(sample_docs, 1):
                    self.logger.info(f"  {i}. OrderID: {doc.get('orderId', 'N/A')}")
                    self.logger.info(f"     Payment Status: {doc.get('paymentStatus', 'N/A')}")
                    self.logger.info(f"     Delivery Status: {doc.get('deliveryStatus', 'N/A')}")
                    self.logger.info(f"     Created At: {doc.get('createdAt', 'N/A')}")
                    self.logger.info("")
            else:
                self.logger.info(f"\nNo sample documents found in date range")
        
        self.logger.info("=" * 80)

    def update_orders_in_range(self) -> Dict[str, int]:
        """
        Update orders matching the date range and criteria.
        
        Returns:
            Dict[str, int]: Operation results
        """
        try:
            self.logger.info("Starting order status update operation...")
            
            orders_coll = self.db[self.orders_collection]
            
            results = {
                'orders_processed': 0,
                'orders_updated': 0,
                'errors': 0
            }
            
            # Query for orders matching all criteria
            query = {
                'createdAt': {
                    '$gte': self.start_date,
                    '$lte': self.end_date
                },
                'paymentStatus': {'$in': self.target_payment_statuses},
                'deliveryStatus': {'$in': self.target_delivery_statuses}
            }
            
            if self.dry_run:
                count = orders_coll.count_documents(query)
                self.logger.info(f"DRY RUN: Would update {count} orders to deliveryStatus = '{self.new_delivery_status}'")
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
            self.logger.error(f"Error during order update operation: {e}")
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
                {'$set': {'deliveryStatus': self.new_delivery_status}}
            )
            
            return update_result.modified_count
            
        except Exception as e:
            self.logger.error(f"Error processing batch: {e}")
            return 0

    def verify_operation(self) -> Dict[str, Any]:
        """
        Verify the order status update operation was successful.
        
        Returns:
            Dict[str, Any]: Verification results
        """
        self.logger.info("Verifying operation results...")
        
        try:
            orders_coll = self.db[self.orders_collection]
            
            # Base date query
            date_query = {
                'createdAt': {
                    '$gte': self.start_date,
                    '$lte': self.end_date
                },
                'paymentStatus': {'$in': self.target_payment_statuses}
            }
            
            verification = {
                'orders_still_pending': orders_coll.count_documents({
                    **date_query,
                    'deliveryStatus': 'PENDING'
                }),
                'orders_still_dispatched': orders_coll.count_documents({
                    **date_query,
                    'deliveryStatus': 'DISPATCHED'
                }),
                'orders_still_preparing': orders_coll.count_documents({
                    **date_query,
                    'deliveryStatus': 'PREPARING'
                }),
                'orders_now_delivered': orders_coll.count_documents({
                    **date_query,
                    'deliveryStatus': 'DELIVERED'
                })
            }
            
            return verification
            
        except Exception as e:
            self.logger.error(f"Error during verification: {e}")
            return {}

    def display_results(self, results: Dict[str, int], verification: Dict[str, Any]):
        """Display operation results and verification."""
        self.logger.info("\n" + "=" * 80)
        self.logger.info("OPERATION RESULTS")
        self.logger.info("=" * 80)
        
        self.logger.info(f"Orders processed: {results.get('orders_processed', 0):,}")
        self.logger.info(f"Orders updated: {results.get('orders_updated', 0):,}")
        self.logger.info(f"Errors encountered: {results.get('errors', 0):,}")
        
        if not self.dry_run and verification:
            self.logger.info("\nVERIFICATION (within date range and payment criteria):")
            self.logger.info(f"Delivery Status still PENDING: {verification.get('orders_still_pending', 0):,}")
            self.logger.info(f"Delivery Status still DISPATCHED: {verification.get('orders_still_dispatched', 0):,}")
            self.logger.info(f"Delivery Status still PREPARING: {verification.get('orders_still_preparing', 0):,}")
            self.logger.info(f"Delivery Status now DELIVERED: {verification.get('orders_now_delivered', 0):,}")
        
        self.logger.info("=" * 80)

    def run_operation(self) -> bool:
        """
        Run the complete order update operation.
        
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
            orders_to_process = initial_analysis.get('orders_matching_criteria', 0)
            if orders_to_process == 0:
                self.logger.info("No orders found matching the criteria. Nothing to process.")
                return True
            
            # Perform order status update
            results = self.update_orders_in_range()
            
            # Verify operation (only if not dry run)
            verification = {}
            if not self.dry_run:
                verification = self.verify_operation()
            
            # Display results
            self.display_results(results, verification)
            
            # Final summary
            elapsed_time = time.time() - start_time
            self.logger.info(f"\n=== Operation Summary ===")
            self.logger.info(f"Date Range: {self.start_date_str} to {self.end_date_str}")
            self.logger.info(f"Operation completed in {elapsed_time:.2f} seconds")
            self.logger.info(f"Orders processed: {results.get('orders_processed', 0)}")
            self.logger.info(f"Orders updated: {results.get('orders_updated', 0)}")
            
            if self.dry_run:
                self.logger.info("DRY RUN completed - no actual changes made")
            else:
                self.logger.info("Delivery status update operation completed successfully!")
                
                # Check success rate
                total_still_not_delivered = (
                    verification.get('orders_still_pending', 0) + 
                    verification.get('orders_still_dispatched', 0) +
                    verification.get('orders_still_preparing', 0)
                )
                if total_still_not_delivered == 0:
                    self.logger.info(f"✓ All matching orders successfully updated to '{self.new_delivery_status}'")
                else:
                    self.logger.warning(f"⚠ {total_still_not_delivered} orders still need updating")
            
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
        description='Update delivery status to DELIVERED for orders in date range with specific payment/delivery status',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
USAGE EXAMPLES:

  Preview operation (recommended first step):
    %(prog)s --start-date 2024-01-01 --end-date 2024-01-31 --dry-run

  Perform the operation:
    %(prog)s --start-date 2024-01-01 --end-date 2024-01-31

  With specific timestamps:
    %(prog)s --start-date "2024-01-01T00:00:00" --end-date "2024-01-31T23:59:59"

OPERATION CRITERIA:
1. Orders with createdAt between start-date and end-date (inclusive)
2. paymentStatus is 'CASH_ON_DELIVERY' or 'PAID'
3. deliveryStatus is 'PENDING', 'DISPATCHED', or 'PREPARING'
4. Update these orders to deliveryStatus = 'DELIVERED'

SAFETY NOTES:
- Both --start-date and --end-date are REQUIRED
- Always run with --dry-run first to preview changes
- Check orders_date_range_update.log for detailed operation logs
- Date formats: YYYY-MM-DD, YYYY-MM-DDTHH:MM:SS, YYYY-MM-DDTHH:MM:SSZ
        """
    )
    
    parser.add_argument(
        '--start-date',
        required=True,
        help='Start date (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS format) - REQUIRED'
    )
    
    parser.add_argument(
        '--end-date', 
        required=True,
        help='End date (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS format) - REQUIRED'
    )
    
    parser.add_argument(
        '--dry-run', 
        action='store_true',
        help='Preview operations without executing them'
    )
    
    return parser.parse_args()


def main():
    """Main function to run the order update operation."""
    print("MongoDB Orders Collection - Date Range Status Update")
    print("=" * 55)
    
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
    print(f"Date Range: {args.start_date} to {args.end_date}")
    print(f"Operation: Update deliveryStatus (PENDING/DISPATCHED/PREPARING → DELIVERED)")
    print(f"Payment Filter: CASH_ON_DELIVERY, PAID")
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
        warning_msg = f"This will update delivery status from {args.start_date} to {args.end_date} to DELIVERED"
    
    response = input(f"Do you want to proceed? {warning_msg}. (y/N): ")
    if response.lower() != 'y':
        print("Operation cancelled.")
        return
    
    # Create updater and run
    try:
        updater = OrderDateRangeUpdater(
            mongo_uri=config['mongo_uri'],
            db_name=config['db_name'],
            start_date=args.start_date,
            end_date=args.end_date,
            batch_size=config['batch_size'],
            dry_run=config['dry_run']
        )
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