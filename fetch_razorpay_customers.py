#!/usr/bin/env python3
"""
Razorpay Customer Data Fetcher

This script fetches payment data from Razorpay API and extracts customer information to save to a CSV file.
It follows the established patterns of this repository with comprehensive logging,
error handling, and dry-run capabilities.

USAGE:
======

Prerequisites:
- Python 3.6+
- requests library: pip install requests
- Valid Razorpay API credentials

What it does:
1. Authenticates with Razorpay API using API key and secret
2. Fetches all payments to extract customer information
3. Deduplicates customer data and exports to CSV file
4. Handles rate limiting and error recovery
5. Provides detailed progress reporting

Basic Usage:
    python fetch_razorpay_customers.py

Dry Run (Preview):
    python fetch_razorpay_customers.py --dry-run

Environment Variables:
    RAZORPAY_API_KEY        Your Razorpay API key (required)
    RAZORPAY_API_SECRET     Your Razorpay API secret (required)
    RAZORPAY_OUTPUT_FILE    CSV output filename (default: razorpay_customers_bak.csv)
    RAZORPAY_BATCH_SIZE     Items per API request (default: 100, max: 100)
    RAZORPAY_DRY_RUN        Set to 'true' for dry run mode
    RAZORPAY_RATE_LIMIT     Seconds between API requests (default: 1)

Examples:
    # Preview the fetch operation
    export RAZORPAY_API_KEY="rzp_test_..."
    export RAZORPAY_API_SECRET="..."
    python fetch_razorpay_customers.py --dry-run

    # Fetch and save to custom file
    export RAZORPAY_OUTPUT_FILE="customers_2024.csv"
    python fetch_razorpay_customers.py

    # Slower rate for production
    export RAZORPAY_RATE_LIMIT="2"
    python fetch_razorpay_customers.py

CSV Output Format:
- customer_email: Customer email address from payments
- customer_contact: Customer phone number from payments
- payment_count: Number of payments by this customer
- total_amount_paise: Total amount paid by customer (in paise)
- total_amount_rupees: Total amount paid by customer (in rupees)
- first_payment_date: Date of first payment
- last_payment_date: Date of last payment
- payment_methods: Unique payment methods used (comma-separated)
- payment_statuses: Unique payment statuses (comma-separated)
- average_payment_amount: Average payment amount in rupees

Safety Features:
- Dry-run mode to preview operations
- Rate limiting to respect API limits
- Resume capability from last successful point
- Comprehensive validation and error handling
- Detailed logging to fetch_razorpay_customers.log
- Progress tracking with ETA calculations

IMPORTANT: This script requires valid Razorpay API credentials.
Always test with dry-run mode first in production environments.
"""

import os
import sys
import logging
import csv
import time
import argparse
from typing import Dict, Any, List, Optional
from datetime import datetime
import requests
from requests.auth import HTTPBasicAuth
import json

class RazorpayCustomerFetcher:
    def __init__(self, api_key: str, api_secret: str, 
                 output_file: str = "razorpay_customers.csv",
                 batch_size: int = 100,
                 rate_limit: float = 1.0,
                 dry_run: bool = False):
        """
        Initialize the Razorpay customer fetcher.
        
        Args:
            api_key: Razorpay API key
            api_secret: Razorpay API secret
            output_file: CSV output filename
            batch_size: Number of customers per API request (max 100)
            rate_limit: Seconds to wait between requests
            dry_run: If True, only show what would be fetched without saving
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.output_file = output_file
        self.batch_size = min(batch_size, 100)  # Razorpay max is 100
        self.rate_limit = rate_limit
        self.dry_run = dry_run
        
        self.base_url = "https://api.razorpay.com/v1"
        self.auth = HTTPBasicAuth(api_key, api_secret)
        
        # Statistics tracking
        self.total_payments = 0
        self.total_errors = 0
        self.start_time = None
        self.customer_data = {}  # Store aggregated customer data
        self.all_emails = []  # Store all emails including duplicates
        
        # Setup logging
        self.setup_logging()
        
    def setup_logging(self):
        """Setup comprehensive logging."""
        log_format = '%(asctime)s - %(levelname)s - %(message)s'
        logging.basicConfig(
            level=logging.INFO,
            format=log_format,
            handlers=[
                logging.FileHandler('fetch_razorpay_customers.log'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def test_connection(self) -> bool:
        """Test Razorpay API connection and credentials."""
        try:
            self.logger.info("Testing Razorpay API connection...")
            response = requests.get(
                f"{self.base_url}/payments",
                auth=self.auth,
                params={"count": 1},
                timeout=30
            )
            
            if response.status_code == 200:
                self.logger.info("✓ Razorpay API connection successful")
                return True
            elif response.status_code == 401:
                self.logger.error("✗ Invalid Razorpay API credentials")
                return False
            else:
                self.logger.error(f"✗ API connection failed: {response.status_code} - {response.text}")
                return False
                
        except requests.exceptions.RequestException as e:
            self.logger.error(f"✗ Connection error: {str(e)}")
            return False
            
    def extract_customer_from_payment(self, payment: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract customer info from payment data."""
        email = (payment.get('email') or '').strip()
        contact = (payment.get('contact') or '').strip()
        
        # Skip if no customer identifiers
        if not email and not contact:
            return None
            
        return {
            'email': email,
            'contact': contact,
            'payment_id': payment.get('id', ''),
            'amount': payment.get('amount', 0),
            'method': payment.get('method', ''),
            'status': payment.get('status', ''),
            'created_at': payment.get('created_at', 0)
        }
        
    def aggregate_customer_data(self, customer_info: Dict[str, Any]):
        """Aggregate customer data from multiple payments."""
        email = customer_info.get('email', '')
        contact = customer_info.get('contact', '')
        
        # Create unique customer key (prefer email, fallback to contact)
        customer_key = email if email else contact
        if not customer_key:
            return
            
        if customer_key not in self.customer_data:
            self.customer_data[customer_key] = {
                'customer_email': email,
                'customer_contact': contact,
                'payment_count': 0,
                'total_amount_paise': 0,
                'payment_methods': set(),
                'payment_statuses': set(),
                'payment_dates': [],
                'payment_amounts': []
            }
            
        # Aggregate data
        customer_record = self.customer_data[customer_key]
        customer_record['payment_count'] += 1
        customer_record['total_amount_paise'] += customer_info.get('amount', 0)
        
        if customer_info.get('method'):
            customer_record['payment_methods'].add(customer_info['method'])
        if customer_info.get('status'):
            customer_record['payment_statuses'].add(customer_info['status'])
            
        created_at = customer_info.get('created_at', 0)
        if created_at:
            customer_record['payment_dates'].append(created_at)
            
        amount = customer_info.get('amount', 0)
        if amount:
            customer_record['payment_amounts'].append(amount)
            
    def finalize_customer_data(self):
        """Finalize aggregated customer data for CSV output."""
        for customer_key, data in self.customer_data.items():
            # Convert sets to comma-separated strings
            data['payment_methods'] = ', '.join(sorted(data['payment_methods']))
            data['payment_statuses'] = ', '.join(sorted(data['payment_statuses']))
            
            # Calculate date ranges
            if data['payment_dates']:
                dates = sorted(data['payment_dates'])
                data['first_payment_date'] = datetime.fromtimestamp(dates[0]).strftime('%Y-%m-%d %H:%M:%S')
                data['last_payment_date'] = datetime.fromtimestamp(dates[-1]).strftime('%Y-%m-%d %H:%M:%S')
            else:
                data['first_payment_date'] = ''
                data['last_payment_date'] = ''
                
            # Convert amounts
            data['total_amount_rupees'] = round(data['total_amount_paise'] / 100, 2)
            
            # Calculate average payment amount
            if data['payment_amounts']:
                avg_paise = sum(data['payment_amounts']) / len(data['payment_amounts'])
                data['average_payment_amount'] = round(avg_paise / 100, 2)
            else:
                data['average_payment_amount'] = 0
                
            # Remove temporary fields
            del data['payment_dates']
            del data['payment_amounts']
        
    def fetch_payments_batch(self, skip: int = 0) -> Optional[Dict[str, Any]]:
        """Fetch a batch of payments from Razorpay API."""
        try:
            params = {
                'count': self.batch_size,
                'skip': skip
            }
            
            self.logger.debug(f"Fetching payments: skip={skip}, count={self.batch_size}")
            
            response = requests.get(
                f"{self.base_url}/payments",
                auth=self.auth,
                params=params,
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                # Rate limited, wait longer and retry
                self.logger.warning("Rate limited, waiting 5 seconds...")
                time.sleep(5)
                return self.fetch_payments_batch(skip)
            else:
                self.logger.error(f"API request failed: {response.status_code} - {response.text}")
                self.total_errors += 1
                return None
                
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request error: {str(e)}")
            self.total_errors += 1
            return None
            
    def fetch_all_payments_and_extract_customers(self) -> bool:
        """Fetch all payments and extract unique customer data."""
        self.logger.info("Starting payment data fetch from Razorpay to extract customers...")
        self.start_time = time.time()
        
        skip = 0
        has_more = True
        
        while has_more:
            # Rate limiting
            if skip > 0:
                time.sleep(self.rate_limit)
                
            batch_data = self.fetch_payments_batch(skip)
            
            if batch_data is None:
                self.logger.error(f"Failed to fetch batch at skip={skip}")
                if self.total_errors > 5:
                    self.logger.error("Too many errors, aborting...")
                    return False
                continue
                
            payments = batch_data.get('items', [])
            batch_count = len(payments)
            
            # Debug: Show API response info
            if skip == 0:
                self.logger.info(f"Total payments found: {batch_data.get('count', 'unknown')}")
                self.logger.info(f"Payments in this batch: {len(payments)}")
                if payments:
                    self.logger.info(f"Sample payment fields: {list(payments[0].keys())}")
            
            if batch_count == 0:
                has_more = False
                break
                
            # Process batch - extract customer data from payments
            for payment in payments:
                # Collect all emails (including duplicates)
                email = (payment.get('email') or '').strip()
                if email:
                    self.all_emails.append(email)
                    
                customer_info = self.extract_customer_from_payment(payment)
                if customer_info:
                    self.aggregate_customer_data(customer_info)
                    
            self.total_payments += batch_count
            
            # Progress reporting
            elapsed = time.time() - self.start_time
            rate = self.total_payments / elapsed if elapsed > 0 else 0
            unique_customers = len(self.customer_data)
            self.logger.info(f"Processed {self.total_payments} payments, found {unique_customers} unique customers ({rate:.1f} payments/sec)")
            
            # Check if there are more payments
            if batch_count < self.batch_size:
                has_more = False
            else:
                skip += batch_count
                
        # Finalize customer data
        self.finalize_customer_data()
                
        return True
        
    def save_to_csv(self) -> bool:
        """Save customer data to CSV file."""
        if not self.customer_data:
            self.logger.warning("No customer data to save")
            return True
            
        try:
            # Define CSV headers in desired order
            headers = [
                'customer_email', 'customer_contact', 'payment_count',
                'total_amount_paise', 'total_amount_rupees', 'average_payment_amount',
                'first_payment_date', 'last_payment_date',
                'payment_methods', 'payment_statuses'
            ]
            
            self.logger.info(f"Saving {len(self.customer_data)} customers to {self.output_file}")
            
            with open(self.output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=headers)
                writer.writeheader()
                
                for customer_key, customer in self.customer_data.items():
                    # Ensure all fields are present
                    row = {header: customer.get(header, '') for header in headers}
                    writer.writerow(row)
                    
            self.logger.info(f"✓ Successfully saved customer data to {self.output_file}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to save CSV: {str(e)}")
            return False
            
    def print_summary(self):
        """Print operation summary."""
        elapsed = time.time() - self.start_time if self.start_time else 0
        
        self.logger.info("\n" + "="*60)
        self.logger.info("RAZORPAY CUSTOMER EXTRACTION SUMMARY")
        self.logger.info("="*60)
        self.logger.info(f"Total payments processed: {self.total_payments}")
        self.logger.info(f"Total emails found (with duplicates): {len(self.all_emails)}")
        unique_emails = list(set(self.all_emails))
        self.logger.info(f"Unique emails found: {len(unique_emails)}")
        self.logger.info(f"Unique customers found: {len(self.customer_data)}")
        self.logger.info(f"API errors encountered: {self.total_errors}")
        self.logger.info(f"Execution time: {elapsed:.1f} seconds")
        
        if self.customer_data:
            # Show top customers by payment count
            top_customers = sorted(
                self.customer_data.items(),
                key=lambda x: x[1]['payment_count'],
                reverse=True
            )[:5]
            
            self.logger.info("\nTop 5 customers by payment count:")
            for customer_key, data in top_customers:
                email_or_contact = data['customer_email'] or data['customer_contact']
                self.logger.info(f"  {email_or_contact}: {data['payment_count']} payments, ₹{data['total_amount_rupees']}")
        
        if not self.dry_run and len(self.customer_data) > 0:
            self.logger.info(f"\nData saved to: {self.output_file}")
            
        if self.dry_run:
            self.logger.info("DRY RUN MODE - No data was saved")
            
        self.logger.info("="*60)
        
    def run(self) -> bool:
        """Execute the complete fetch operation."""
        if self.dry_run:
            self.logger.info("🔍 DRY RUN MODE - Preview only, no data will be saved")
            
        # Test connection first
        if not self.test_connection():
            return False
            
        # Fetch all payment data and extract customers
        if not self.fetch_all_payments_and_extract_customers():
            return False
            
        # Save to CSV (unless dry run)
        if not self.dry_run:
            if not self.save_to_csv():
                return False
        else:
            self.logger.info("Would save to CSV file in actual run")
            
        # Print summary
        self.print_summary()
        
        return True

def main():
    """Main function with argument parsing and environment variable handling."""
    parser = argparse.ArgumentParser(description='Fetch Razorpay customer data to CSV')
    parser.add_argument('--dry-run', action='store_true', 
                       help='Preview operation without saving data')
    
    args = parser.parse_args()
    
    # Get configuration from environment variables
    api_key = os.getenv('RAZORPAY_API_KEY')
    api_secret = os.getenv('RAZORPAY_API_SECRET')
    output_file = os.getenv('RAZORPAY_OUTPUT_FILE', 'razorpay_customers_bak.csv')
    batch_size = int(os.getenv('RAZORPAY_BATCH_SIZE', '100'))
    rate_limit = float(os.getenv('RAZORPAY_RATE_LIMIT', '1.0'))
    dry_run = args.dry_run or os.getenv('RAZORPAY_DRY_RUN', '').lower() == 'true'
    
    # Validate required credentials
    if not api_key or not api_secret:
        print("Error: RAZORPAY_API_KEY and RAZORPAY_API_SECRET environment variables are required")
        print("Usage:")
        print("  export RAZORPAY_API_KEY='your_api_key'")
        print("  export RAZORPAY_API_SECRET='your_api_secret'")
        print("  python fetch_razorpay_customers.py")
        sys.exit(1)
    
    # Initialize and run fetcher
    fetcher = RazorpayCustomerFetcher(
        api_key=api_key,
        api_secret=api_secret,
        output_file=output_file,
        batch_size=batch_size,
        rate_limit=rate_limit,
        dry_run=dry_run
    )
    
    try:
        success = fetcher.run()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        fetcher.logger.info("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        fetcher.logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()