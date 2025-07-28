import requests
import json
import time
import random
from datetime import datetime
from google.cloud import storage
import os
from typing import Optional, Dict, List

# GCS Configuration
BUCKET_NAME = "group-1-landing-lets-talk"
STACKEXCHANGE_PREFIX = "stack_exchange/"

# Service account configuration
SERVICE_ACCOUNT_KEY = os.path.abspath("calm-depot-454710-p0-4cf918c71f69.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_KEY

# Stack Exchange API Configuration
STACK_EXCHANGE_SITES = [
    "stackoverflow",
    "softwareengineering",
    "superuser",
    "askubuntu",
    "gaming",
    "worldbuilding",
    "movies",
    "cooking",
    "travel",
    "philosophy"
]

class StackExchangeUploader:
    def __init__(self):
        self.api_url = "https://api.stackexchange.com/2.3/search/advanced"
        self.base_params = {
            "order": "desc",
            "sort": "votes",      # sort by votes to get the best content
            "pagesize": 100,      # max allowed per page
            "filter": "!9_bDDxJY5"  # detailed field set
        }
        
    def _get_timestamp(self) -> str:
        """Generate timestamp for file naming."""
        return datetime.now().strftime("%Y%m%d_%H%M%S")
    
    def _upload_to_gcs(self, data: Dict, timestamp: str) -> bool:
        """
        Upload data to Google Cloud Storage.
        
        Args:
            data: The data to upload
            timestamp: Timestamp string for file naming
            
        Returns:
            bool: True if upload successful, False otherwise
        """
        try:
            storage_client = storage.Client()
            bucket = storage_client.bucket(BUCKET_NAME)
            
            # Create blob with timestamp
            blob_name = f"{STACKEXCHANGE_PREFIX}stackexchange_raw_{timestamp}.json"
            blob = bucket.blob(blob_name)
            
            # Convert and upload
            json_data = json.dumps(data, ensure_ascii=False, indent=2)
            blob.upload_from_string(json_data, content_type='application/json')
            
            print(f"Successfully uploaded to gs://{BUCKET_NAME}/{blob_name}")
            return True
            
        except Exception as e:
            print(f"Error uploading to GCS: {str(e)}")
            return False

    def fetch_data(self, sites: Optional[List[str]] = None, 
                  max_pages: int = 25) -> List[Dict]:
        """
        Fetch data from Stack Exchange sites.
        
        Args:
            sites: List of Stack Exchange sites to fetch from. If None, uses default list.
            max_pages: Maximum number of pages to fetch per site (default 25)
            
        Returns:
            List of dictionaries containing the fetched data
        """
        if sites is None:
            sites = STACK_EXCHANGE_SITES
            
        raw_results = []
        
        for site in sites:
            print(f"\nFetching data for site: {site}")
            site_pages = []
            page = 1
            
            while page <= max_pages:
                params = self.base_params.copy()
                params["site"] = site
                params["page"] = page
                
                retries = 0
                success = False
                
                while retries < 5:
                    try:
                        response = requests.get(self.api_url, params=params)
                        response.raise_for_status()
                        data = response.json()
                        
                        if "items" in data:
                            print(f"  ✓ Page {page}: Retrieved {len(data['items'])} items")
                            if "backoff" in data:
                                backoff = data["backoff"]
                                print(f" API requesting backoff of {backoff} seconds...")
                                time.sleep(backoff + 1)  # Add 1 second buffer
                        
                        site_pages.append(data)
                        success = True
                        break
                        
                    except requests.exceptions.RequestException as e:
                        retries += 1
                        print(f" Error on page {page}: {str(e)}. Retry {retries}/5 in 60s...")
                        time.sleep(60)
                
                if not success:
                    print(f" Failed to fetch page {page} after 5 retries. Skipping remaining pages.")
                    break
                
                # Respect API rate limits with random delay
                time.sleep(random.uniform(2, 3))
                
                # Check if there aren't more pages
                if not data.get("has_more", False):
                    print(f" No more pages available for {site}")
                    break
                    
                page += 1
            
            raw_results.append({
                "site": site,
                "pages": site_pages,
                "collection_timestamp": datetime.now().isoformat(),
                "items_count": sum(len(page.get("items", [])) for page in site_pages)
            })
            
            print(f"Completed {site}: {raw_results[-1]['items_count']} total items")
            
            # Add a longer delay between sites to be extra careful with API limits
            time.sleep(random.uniform(3, 5))
        
        return raw_results

    def upload(self, max_pages: int = 25) -> bool:
        """
        Main method to fetch and upload Stack Exchange data.
        
        Args:
            max_pages: Maximum pages to fetch per site
            
        Returns:
            bool: True if successful, False otherwise
        """
        print("\nStarting Stack Exchange data collection (no time limit, collecting top voted posts)...")
        timestamp = self._get_timestamp()
        
        try:
            # Fetch the data
            raw_results = self.fetch_data(max_pages=max_pages)
            
            # Save local backup first
            local_filename = f"stackexchange_raw_{timestamp}.json"
            with open(local_filename, "w", encoding="utf-8") as f:
                json.dump(raw_results, f, ensure_ascii=False, indent=2)
            print(f"\nLocal backup saved: {local_filename}")
            
            # Upload to GCS
            total_items = sum(result["items_count"] for result in raw_results)
            print(f"\nSummary before upload:")
            print(f"  - Sites processed: {len(raw_results)}")
            print(f"  - Total items collected: {total_items}")
            
            if self._upload_to_gcs(raw_results, timestamp):
                print("\nData collection and upload completed successfully!")
                # Clean up local file
                os.remove(local_filename)
                print(f"🧹 Cleaned up local file: {local_filename}")
                return True
            else:
                print(f"\nUpload failed. Data preserved in: {local_filename}")
                return False
                
        except Exception as e:
            print(f"\nError during collection: {str(e)}")
            return False

def main():
    """Command line interface for the uploader."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Upload Stack Exchange data to GCS landing zone")
    parser.add_argument("--max-pages", type=int, default=25,
                       help="Maximum pages to fetch per site (default: 25)")
    
    args = parser.parse_args()
    
    uploader = StackExchangeUploader()
    success = uploader.upload(max_pages=args.max_pages)
    
    if not success:
        exit(1)

if __name__ == "__main__":
    main() 