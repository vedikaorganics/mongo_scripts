#!/usr/bin/env python3
"""
Create test customers in Razorpay for testing the fetch script.

Usage:
    export RAZORPAY_API_KEY="rzp_test_..."
    export RAZORPAY_API_SECRET="..."
    python create_test_customers.py
"""

import os
import requests
from requests.auth import HTTPBasicAuth

def create_test_customers():
    api_key = os.getenv('RAZORPAY_API_KEY')
    api_secret = os.getenv('RAZORPAY_API_SECRET')
    
    if not api_key or not api_secret:
        print("Error: Set RAZORPAY_API_KEY and RAZORPAY_API_SECRET")
        return
    
    auth = HTTPBasicAuth(api_key, api_secret)
    base_url = "https://api.razorpay.com/v1"
    
    test_customers = [
        {
            "name": "John Doe",
            "email": "john.doe@example.com",
            "contact": "+919876543210",
            "notes": {"source": "test_script", "priority": "high"}
        },
        {
            "name": "Jane Smith", 
            "email": "jane.smith@example.com",
            "contact": "+919876543211",
            "gstin": "12ABCDE3456F7GH",
            "notes": {"source": "test_script", "region": "north"}
        },
        {
            "name": "Bob Wilson",
            "email": "bob.wilson@example.com", 
            "contact": "+919876543212",
            "notes": {"source": "test_script", "type": "premium"}
        }
    ]
    
    created = 0
    for customer_data in test_customers:
        try:
            response = requests.post(
                f"{base_url}/customers",
                auth=auth,
                json=customer_data,
                timeout=30
            )
            
            if response.status_code == 200:
                customer = response.json()
                print(f"✓ Created customer: {customer['name']} (ID: {customer['id']})")
                created += 1
            else:
                print(f"✗ Failed to create {customer_data['name']}: {response.status_code} - {response.text}")
                
        except Exception as e:
            print(f"✗ Error creating {customer_data['name']}: {e}")
    
    print(f"\nCreated {created} test customers")
    print("Now you can run: python fetch_razorpay_customers.py")

if __name__ == "__main__":
    create_test_customers()