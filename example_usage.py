#!/usr/bin/env python3
"""
Example usage of the Amazon Ads Fetcher
This demonstrates how to use the script with your credentials
"""

import os

print("=" * 60)
print("Amazon Ads Data Fetcher - Setup Instructions")
print("=" * 60)
print()
print("To use this script, you need to set the following environment variables:")
print()
print("1. AMAZON_ADS_CLIENT_ID     - Your Amazon Ads API Client ID")
print("2. AMAZON_ADS_CLIENT_SECRET - Your Amazon Ads API Client Secret")
print("3. AMAZON_ADS_REFRESH_TOKEN - Your OAuth Refresh Token")
print("4. AMAZON_ADS_REGION        - API Region (NA/EU/FE) - defaults to NA")
print()
print("How to get these credentials:")
print("1. Go to https://advertising.amazon.com/API/")
print("2. Register for API access")
print("3. Create an OAuth application")
print("4. Note down your Client ID and Client Secret")
print("5. Complete OAuth flow to get a refresh token")
print()
print("=" * 60)
print()

if os.getenv("AMAZON_ADS_CLIENT_ID"):
    print("✓ Credentials detected! Running the fetcher...")
    print()
    import subprocess
    subprocess.run(["python", "amazon_ads_fetcher.py"])
else:
    print("⚠ No credentials found.")
    print()
    print("To set credentials in Replit:")
    print("1. Click the 'Secrets' tool (lock icon) in the left sidebar")
    print("2. Add each secret with the exact names above")
    print("3. Run this script again")
    print()
    print("Or run directly with credentials:")
    print()
    print("  AMAZON_ADS_CLIENT_ID=your_id \\")
    print("  AMAZON_ADS_CLIENT_SECRET=your_secret \\")
    print("  AMAZON_ADS_REFRESH_TOKEN=your_token \\")
    print("  python amazon_ads_fetcher.py")
