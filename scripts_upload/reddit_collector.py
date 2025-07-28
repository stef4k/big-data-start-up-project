import requests
import json

client_id = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
client_secret = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
username = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
password = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
user_agent = 'XXXXXXXXXXXXXXXXXXXXXXXXXXX'

# OAuth2 access token needed as it allows for 100 requests per minute instead of 10
auth = requests.auth.HTTPBasicAuth(client_id, client_secret)
data = {
    'grant_type': 'password',
    'username': username,
    'password': password
}
headers = {'User-Agent': user_agent}

response = requests.post('https://www.reddit.com/api/v1/access_token',
                         auth=auth, data=data, headers=headers)
token = response.json().get('access_token')

if not token:
    raise Exception("Could not obtain access token. Check your credentials.")

headers['Authorization'] = f'bearer {token}'

# fetch raw JSON data (pagination)
subreddits = ['AskReddit', 'NoStupidQuestions', 'ExplainLikeImFive', 'questions', 'ask', 'DoesAnybodyElse', 'askscience', 'Ask_Politics', 'TrueAskReddit', 'AskScienceFiction', 'AskEngineers', 'AskHistorians', 'AskMen', 'AskWomen']
all_posts = {}
max_posts_per_subreddit = 1000

for sub in subreddits:
    subreddit_posts = []
    url = f'https://oauth.reddit.com/r/{sub}/top'
    params = {'limit': 100, 't': 'all'}
    after = None

    while len(subreddit_posts) < max_posts_per_subreddit:
        if after:
            params['after'] = after
        response = requests.get(url, headers=headers, params=params)
        print(f"Fetching posts from {sub}... (after: {after})")
        data = response.json()
        
        # posts are under data->children
        posts = data.get('data', {}).get('children', [])
        if not posts:
            break  # no more posts
        
        subreddit_posts.extend(posts)
        
        after = data.get('data', {}).get('after')
        if not after:
            break  # reached the last page
        
    all_posts[sub] = subreddit_posts

# save raw JSON data
with open("data/reddit_posts_raw.json", "w", encoding="utf-8") as f:
    json.dump(all_posts, f, ensure_ascii=False, indent=2)

print("Raw JSON data exported to reddit_posts_raw.json")