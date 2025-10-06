#!/usr/bin/env python3
"""
Amazon Ads Data Fetcher
Fetches campaign and performance data from Amazon Advertising API
"""

import os
import sys
import json
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional

class AmazonAdsFetcher:
    """Fetch Amazon Ads campaign and performance data"""
    
    REGION_URLS = {
        "NA": "https://advertising-api.amazon.com",
        "EU": "https://advertising-api-eu.amazon.com",
        "FE": "https://advertising-api-fe.amazon.com"
    }
    
    def __init__(self, client_id: str, client_secret: str, refresh_token: str, region: str = "NA"):
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.region = region
        self.base_url = self.REGION_URLS.get(region, self.REGION_URLS["NA"])
        self.access_token = None
        
    def refresh_access_token(self) -> str:
        """Refresh OAuth access token"""
        url = "https://api.amazon.com/auth/o2/token"
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }
        
        try:
            response = requests.post(url, data=payload)
            response.raise_for_status()
            data = response.json()
            self.access_token = data.get("access_token")
            return self.access_token
        except Exception as e:
            print(f"Error refreshing token: {e}", file=sys.stderr)
            raise
    
    def _get_headers(self, profile_id: Optional[str] = None) -> Dict[str, str]:
        """Get request headers"""
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Amazon-Advertising-API-ClientId": self.client_id,
            "Content-Type": "application/json"
        }
        if profile_id:
            headers["Amazon-Advertising-API-Scope"] = profile_id
        return headers
    
    def get_profiles(self) -> List[Dict]:
        """Fetch advertising profiles"""
        url = f"{self.base_url}/v2/profiles"
        params = {"profileTypeFilter": "seller,vendor"}
        
        try:
            response = requests.get(url, headers=self._get_headers(), params=params)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error fetching profiles: {e}", file=sys.stderr)
            return []
    
    def get_sponsored_display_campaigns(self, profile_id: str, state_filter: str = "enabled,paused,archived") -> List[Dict]:
        """Fetch Sponsored Display campaigns"""
        url = f"{self.base_url}/sd/campaigns"
        params = {
            "stateFilter": state_filter,
            "count": 100,
            "startIndex": 0
        }
        
        campaigns = []
        try:
            while True:
                response = requests.get(url, headers=self._get_headers(profile_id), params=params)
                response.raise_for_status()
                data = response.json()
                
                if not data:
                    break
                    
                campaigns.extend(data)
                
                if len(data) < 100:
                    break
                    
                params["startIndex"] += 100
            
            return campaigns
        except Exception as e:
            print(f"Error fetching Sponsored Display campaigns: {e}", file=sys.stderr)
            return []
    
    def get_sponsored_brands_campaigns(self, profile_id: str, state_filter: Optional[List[str]] = None) -> List[Dict]:
        """Fetch Sponsored Brands campaigns"""
        url = f"{self.base_url}/sb/v4/campaigns/list"
        
        if state_filter is None:
            state_filter = ["enabled", "paused", "archived"]
        
        headers = self._get_headers(profile_id)
        headers["Accept"] = "application/vnd.sbcampaignresource.v4+json"
        headers["Content-Type"] = "application/vnd.sbcampaignresource.v4+json"
        
        payload = {
            "stateFilter": {"include": ",".join(state_filter)},
            "maxResults": 100
        }
        
        campaigns = []
        try:
            while True:
                response = requests.post(url, headers=headers, json=payload)
                response.raise_for_status()
                data = response.json()
                
                if "campaigns" in data:
                    campaigns.extend(data["campaigns"])
                
                if "nextToken" not in data or not data["nextToken"]:
                    break
                    
                payload["nextToken"] = data["nextToken"]
            
            return campaigns
        except Exception as e:
            print(f"Error fetching Sponsored Brands campaigns: {e}", file=sys.stderr)
            return []
    
    def get_sponsored_products_campaigns(self, profile_id: str, state_filter: Optional[List[str]] = None) -> List[Dict]:
        """Fetch Sponsored Products campaigns"""
        url = f"{self.base_url}/sp/campaigns/list"
        
        if state_filter is None:
            state_filter = ["enabled", "paused", "archived"]
        
        headers = self._get_headers(profile_id)
        headers["Accept"] = "application/vnd.spCampaign.v3+json"
        headers["Content-Type"] = "application/vnd.spCampaign.v3+json"
        
        payload = {
            "stateFilter": {"include": ",".join(state_filter)},
            "maxResults": 100
        }
        
        campaigns = []
        try:
            while True:
                response = requests.post(url, headers=headers, json=payload)
                response.raise_for_status()
                data = response.json()
                
                if "campaigns" in data:
                    campaigns.extend(data["campaigns"])
                
                if "nextToken" not in data or not data["nextToken"]:
                    break
                    
                payload["nextToken"] = data["nextToken"]
            
            return campaigns
        except Exception as e:
            print(f"Error fetching Sponsored Products campaigns: {e}", file=sys.stderr)
            return []
    
    def get_attribution_performance_campaign(self, profile_id: str, timezone: str = "America/Los_Angeles") -> List[Dict]:
        """Fetch Attribution performance report by campaign"""
        url = f"{self.base_url}/attribution/report"
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=90)
        
        payload = {
            "reportType": "PERFORMANCE",
            "groupBy": "CAMPAIGN",
            "metrics": "Click-throughs,attributedDetailPageViewsClicks14d,attributedAddToCartClicks14d,attributedPurchases14d,unitsSold14d,attributedSales14d",
            "startDate": start_date.strftime("%Y%m%d"),
            "endDate": end_date.strftime("%Y%m%d"),
            "count": 300
        }
        
        reports = []
        try:
            while True:
                response = requests.post(url, headers=self._get_headers(profile_id), json=payload)
                
                if response.status_code == 400:
                    break
                
                response.raise_for_status()
                data = response.json()
                
                if "reports" in data:
                    reports.extend(data["reports"])
                
                if "cursorId" not in data or not data["cursorId"]:
                    break
                    
                payload["cursorId"] = data["cursorId"]
            
            return reports
        except Exception as e:
            print(f"Error fetching attribution report: {e}", file=sys.stderr)
            return []
    
    def fetch_all_data(self) -> Dict:
        """Fetch all campaign and performance data"""
        self.refresh_access_token()
        
        profiles = self.get_profiles()
        
        all_data = {
            "profiles": profiles,
            "campaigns": {},
            "performance": {}
        }
        
        for profile in profiles:
            profile_id = str(profile["profileId"])
            profile_name = profile.get("accountInfo", {}).get("name", profile_id)
            
            print(f"Fetching data for profile: {profile_name} ({profile_id})", file=sys.stderr)
            
            all_data["campaigns"][profile_id] = {
                "sponsored_display": self.get_sponsored_display_campaigns(profile_id),
                "sponsored_brands": self.get_sponsored_brands_campaigns(profile_id),
                "sponsored_products": self.get_sponsored_products_campaigns(profile_id)
            }
            
            timezone = profile.get("timezone", "America/Los_Angeles")
            all_data["performance"][profile_id] = {
                "attribution_campaign": self.get_attribution_performance_campaign(profile_id, timezone)
            }
        
        return all_data

def main():
    client_id = os.getenv("AMAZON_ADS_CLIENT_ID")
    client_secret = os.getenv("AMAZON_ADS_CLIENT_SECRET")
    refresh_token = os.getenv("AMAZON_ADS_REFRESH_TOKEN")
    region = os.getenv("AMAZON_ADS_REGION", "NA")
    
    if not all([client_id, client_secret, refresh_token]):
        print("Error: Missing required environment variables:", file=sys.stderr)
        print("  AMAZON_ADS_CLIENT_ID", file=sys.stderr)
        print("  AMAZON_ADS_CLIENT_SECRET", file=sys.stderr)
        print("  AMAZON_ADS_REFRESH_TOKEN", file=sys.stderr)
        print("  AMAZON_ADS_REGION (optional, defaults to 'NA')", file=sys.stderr)
        sys.exit(1)
    
    fetcher = AmazonAdsFetcher(
        client_id or "",
        client_secret or "",
        refresh_token or "",
        region
    )
    
    print("Fetching Amazon Ads data...", file=sys.stderr)
    data = fetcher.fetch_all_data()
    
    print(json.dumps(data, indent=2))

if __name__ == "__main__":
    main()
