import requests
import json
import time
import random

# StackExchange sites
cool_sites = [
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

api_url = "https://api.stackexchange.com/2.3/search/advanced"
base_params = {
    "order": "desc",
    "sort": "votes",      # sort by votes
    "pagesize": 100,      # this is the max allowed
    "filter": "!9_bDDxJY5" # filter that returns a detailed set of fields, from the API docs
}

max_pages = 10  # 10 pages per site
raw_results = []

for site in cool_sites:
    print(f"Fetching data for site: {site}")
    site_pages = []
    page = 1
    while page <= max_pages:
        params = base_params.copy()
        params["site"] = site
        params["q"] = ""  # empty for this stage (filter in P2)
        params["page"] = page

        retries = 0
        success = False
        # we did this as we hit the limit quite often
        # https://api.stackexchange.com/docs/throttle 
        while retries < 5:
            response = requests.get(api_url, params=params)
            if response.status_code == 200:
                data = response.json()
                site_pages.append(data)
                success = True
                break
            else:
                retries += 1
                print(f"Error fetching data for site {site} on page {page}: {response.status_code}. Retrying ({retries}/5) in 5 seconds.")
                time.sleep(60)  # 1m before retrying

        if not success:
            print(f"Failed to fetch data for site {site} on page {page} after 5 retries. Skipping further pages for this site.")
            break

        # sleep a random 1-2 seconds between requests
        time.sleep(random.uniform(1, 2))

        # if there aren't more pages, break
        if not data.get("has_more", False):
            break

        page += 1

    raw_results.append({
        "site": site,
        "pages": site_pages
    })

# save
with open("stackexchange_raw.json", "w", encoding="utf-8") as f:
    json.dump(raw_results, f, ensure_ascii=False, indent=2)

print("Raw JSON data exported to stackexchange_raw.json")
