#!/usr/bin/env python3
"""
Standalone Bing Ads Data Extractor
Fetches campaign and performance data from Bing Ads API without Airbyte CDK dependency.
"""

import json
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import requests


class BingAdsExtractor:
    """Extract campaign and performance data from Bing Ads API."""
    
    def __init__(
        self,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        developer_token: str
    ):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.developer_token = developer_token
        self.access_token: Optional[str] = None
        
        self.customer_management_base = "https://clientcenter.api.bingads.microsoft.com/CustomerManagement/v13"
        self.campaign_management_base = "https://campaign.api.bingads.microsoft.com/CampaignManagement/v13"
        self.reporting_base = "https://reporting.api.bingads.microsoft.com/Api/Advertiser/Reporting/v13"
    
    def get_access_token(self) -> str:
        """Get OAuth access token using refresh token."""
        if self.access_token:
            return self.access_token
        
        token_url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        
        data = {
            "grant_type": "refresh_token",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
            "scope": "https://ads.microsoft.com/msads.manage offline_access"
        }
        
        response = requests.post(token_url, data=data)
        response.raise_for_status()
        
        token_data = response.json()
        self.access_token = token_data["access_token"]
        return self.access_token
    
    def _get_auth_headers(self, customer_account_id: Optional[str] = None, customer_id: Optional[str] = None) -> Dict[str, str]:
        """Get authentication headers for API requests."""
        headers = {
            "Authorization": f"Bearer {self.get_access_token()}",
            "Content-Type": "application/json",
            "DeveloperToken": self.developer_token
        }
        
        if customer_account_id:
            headers["CustomerAccountId"] = str(customer_account_id)
        if customer_id:
            headers["CustomerId"] = str(customer_id)
        
        return headers
    
    def get_users(self) -> List[Dict]:
        """Fetch all users."""
        url = f"{self.customer_management_base}/User/Query"
        headers = self._get_auth_headers()
        
        payload = {"UserId": None}
        
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        
        data = response.json()
        return data.get("User", []) if isinstance(data.get("User"), list) else [data.get("User")] if data.get("User") else []
    
    def get_accounts(self, user_id: int) -> List[Dict]:
        """Fetch all accounts for a user."""
        url = f"{self.customer_management_base}/Accounts/Search"
        headers = self._get_auth_headers()
        
        payload = {
            "PageInfo": {
                "Index": 0,
                "Size": 1000
            },
            "Predicates": [
                {
                    "Field": "UserId",
                    "Operator": "Equals",
                    "Value": str(user_id)
                }
            ],
            "ReturnAdditionalFields": "TaxCertificate,AccountMode"
        }
        
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        
        data = response.json()
        return data.get("Accounts", [])
    
    def get_campaigns(self, account_id: int, customer_id: int) -> List[Dict]:
        """Fetch all campaigns for an account."""
        url = f"{self.campaign_management_base}/Campaigns/QueryByAccountId"
        headers = self._get_auth_headers(customer_account_id=str(account_id), customer_id=str(customer_id))
        
        payload = {
            "AccountId": str(account_id),
            "CampaignType": "Audience,DynamicSearchAds,Search,Shopping,PerformanceMax",
            "ReturnAdditionalFields": "AdScheduleUseSearcherTimeZone,BidStrategyId,CpvCpmBiddingScheme,DynamicDescriptionSetting,DynamicFeedSetting,MaxConversionValueBiddingScheme,MultimediaAdsBidAdjustment,TargetImpressionShareBiddingScheme,TargetSetting,VerifiedTrackingSetting"
        }
        
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        
        data = response.json()
        campaigns = data.get("Campaigns", [])
        
        for campaign in campaigns:
            campaign["AccountId"] = account_id
            campaign["CustomerId"] = customer_id
        
        return campaigns
    
    def submit_performance_report(
        self,
        account_id: int,
        report_type: str = "CampaignPerformanceReportRequest",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        aggregation: str = "Daily"
    ) -> str:
        """Submit a performance report request and return the report request ID."""
        url = f"{self.reporting_base}/ReportingService.svc/SubmitGenerateReport"
        headers = self._get_auth_headers(customer_account_id=str(account_id))
        
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        columns = {
            "CampaignPerformanceReportRequest": [
                "TimePeriod", "AccountId", "CampaignId", "CampaignName", "CampaignStatus",
                "Impressions", "Clicks", "Spend", "Conversions", "Revenue", "Ctr"
            ],
            "AdGroupPerformanceReportRequest": [
                "TimePeriod", "AccountId", "CampaignId", "AdGroupId", "AdGroupName",
                "Impressions", "Clicks", "Spend", "Conversions", "Ctr"
            ]
        }
        
        payload = {
            "ReportRequest": {
                "ExcludeColumnHeaders": False,
                "ExcludeReportFooter": True,
                "ExcludeReportHeader": True,
                "Format": "Csv",
                "FormatVersion": "2.0",
                "ReportName": f"{report_type}_{datetime.now().strftime('%Y%m%d')}",
                "ReturnOnlyCompleteData": False,
                "Type": report_type,
                "Aggregation": aggregation,
                "Columns": columns.get(report_type, columns["CampaignPerformanceReportRequest"]),
                "Scope": {
                    "AccountIds": [str(account_id)]
                },
                "Time": {
                    "CustomDateRangeStart": {
                        "Day": start_dt.day,
                        "Month": start_dt.month,
                        "Year": start_dt.year
                    },
                    "CustomDateRangeEnd": {
                        "Day": end_dt.day,
                        "Month": end_dt.month,
                        "Year": end_dt.year
                    },
                    "ReportTimeZone": "GreenwichMeanTimeDublinEdinburghLisbonLondon"
                }
            }
        }
        
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        
        data = response.json()
        return data.get("ReportRequestId")
    
    def poll_report_status(self, report_request_id: str) -> Dict:
        """Poll the status of a report request."""
        url = f"{self.reporting_base}/ReportingService.svc/PollGenerateReport"
        headers = self._get_auth_headers()
        
        payload = {
            "ReportRequestId": report_request_id
        }
        
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        
        return response.json()
    
    def download_report(self, download_url: str) -> List[Dict]:
        """Download and parse the report CSV."""
        import csv
        import gzip
        from io import StringIO
        
        response = requests.get(download_url)
        response.raise_for_status()
        
        try:
            decompressed = gzip.decompress(response.content)
            text_content = decompressed.decode("utf-8-sig")
        except (gzip.BadGzipFile, OSError):
            text_content = response.content.decode("utf-8-sig")
        
        csv_reader = csv.DictReader(StringIO(text_content))
        return list(csv_reader)
    
    def get_performance_report(
        self,
        account_id: int,
        report_type: str = "CampaignPerformanceReportRequest",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        max_wait_seconds: int = 300
    ) -> List[Dict]:
        """Get performance report data (submits, polls, and downloads)."""
        import time
        
        print(f"Submitting {report_type} for account {account_id}...", file=sys.stderr)
        report_id = self.submit_performance_report(account_id, report_type, start_date, end_date)
        
        print(f"Report ID: {report_id}", file=sys.stderr)
        print(f"Polling for report completion...", file=sys.stderr)
        
        start_time = time.time()
        while time.time() - start_time < max_wait_seconds:
            status_response = self.poll_report_status(report_id)
            status = status_response.get("ReportRequestStatus", {}).get("Status")
            
            print(f"Status: {status}", file=sys.stderr)
            
            if status == "Success":
                download_url = status_response.get("ReportRequestStatus", {}).get("ReportDownloadUrl")
                print(f"Downloading report from: {download_url}", file=sys.stderr)
                return self.download_report(download_url)
            elif status == "Error":
                raise Exception(f"Report generation failed: {status_response}")
            
            time.sleep(10)
        
        raise TimeoutError(f"Report generation timed out after {max_wait_seconds} seconds")
    
    def extract_all(
        self,
        include_performance_reports: bool = True,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Dict:
        """Extract all campaign and performance data."""
        result = {
            "extracted_at": datetime.now().isoformat(),
            "users": [],
            "accounts": [],
            "campaigns": [],
            "performance_reports": []
        }
        
        print("Fetching users...", file=sys.stderr)
        users = self.get_users()
        result["users"] = users
        print(f"Found {len(users)} user(s)", file=sys.stderr)
        
        for user in users:
            user_id = user.get("Id")
            if not user_id:
                continue
            print(f"\nFetching accounts for user {user_id}...", file=sys.stderr)
            
            accounts = self.get_accounts(int(user_id))
            result["accounts"].extend(accounts)
            print(f"Found {len(accounts)} account(s)", file=sys.stderr)
            
            for account in accounts:
                account_id = account.get("Id")
                customer_id = account.get("ParentCustomerId")
                account_name = account.get("Name")
                
                if not account_id or not customer_id:
                    continue
                
                print(f"\nFetching campaigns for account {account_id} ({account_name})...", file=sys.stderr)
                
                try:
                    campaigns = self.get_campaigns(int(account_id), int(customer_id))
                    result["campaigns"].extend(campaigns)
                    print(f"Found {len(campaigns)} campaign(s)", file=sys.stderr)
                except Exception as e:
                    print(f"Error fetching campaigns for account {account_id}: {e}", file=sys.stderr)
                
                if include_performance_reports:
                    print(f"\nFetching performance report for account {account_id}...", file=sys.stderr)
                    try:
                        report_data = self.get_performance_report(
                            int(account_id),
                            report_type="CampaignPerformanceReportRequest",
                            start_date=start_date,
                            end_date=end_date
                        )
                        result["performance_reports"].append({
                            "account_id": account_id,
                            "account_name": account_name,
                            "report_type": "campaign_performance",
                            "data": report_data
                        })
                        print(f"Retrieved {len(report_data)} performance record(s)", file=sys.stderr)
                    except Exception as e:
                        print(f"Error fetching performance report for account {account_id}: {e}", file=sys.stderr)
        
        return result


def main():
    """Main execution function."""
    tenant_id = os.environ.get("BING_ADS_TENANT_ID")
    client_id = os.environ.get("BING_ADS_CLIENT_ID")
    client_secret = os.environ.get("BING_ADS_CLIENT_SECRET")
    refresh_token = os.environ.get("BING_ADS_REFRESH_TOKEN")
    developer_token = os.environ.get("BING_ADS_DEVELOPER_TOKEN")
    
    if not all([tenant_id, client_id, client_secret, refresh_token, developer_token]):
        print("Error: Missing required environment variables:", file=sys.stderr)
        print("  - BING_ADS_TENANT_ID", file=sys.stderr)
        print("  - BING_ADS_CLIENT_ID", file=sys.stderr)
        print("  - BING_ADS_CLIENT_SECRET", file=sys.stderr)
        print("  - BING_ADS_REFRESH_TOKEN", file=sys.stderr)
        print("  - BING_ADS_DEVELOPER_TOKEN", file=sys.stderr)
        sys.exit(1)
    
    start_date = os.environ.get("START_DATE")
    end_date = os.environ.get("END_DATE")
    include_reports = os.environ.get("INCLUDE_PERFORMANCE_REPORTS", "true").lower() == "true"
    
    extractor = BingAdsExtractor(
        tenant_id=str(tenant_id),
        client_id=str(client_id),
        client_secret=str(client_secret),
        refresh_token=str(refresh_token),
        developer_token=str(developer_token)
    )
    
    try:
        data = extractor.extract_all(
            include_performance_reports=include_reports,
            start_date=start_date,
            end_date=end_date
        )
        
        print(json.dumps(data, indent=2))
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
