
import json
import requests
from time import sleep
from requests.exceptions import RequestException

# Credentials file
SERVICE_ACCOUNT_FILE = 'credentials.json'

# Load API Key from credentials.json
def load_api_key(file_path):
    try:
        with open(file_path, 'r') as file:
            credentials = json.load(file)
            api_key = credentials.get("MISSIVE_API_KEY")
            if not api_key:
                raise ValueError("MISSIVE_API_KEY not found in credentials.json")
            return api_key
    except FileNotFoundError:
        raise FileNotFoundError(f"Credentials file not found: {file_path}")
    except json.JSONDecodeError:
        raise ValueError(f"Invalid JSON format in: {file_path}")

MISSIVE_API_KEY = load_api_key(SERVICE_ACCOUNT_FILE)



# Function to fetch Missive users
def get_missive_users():
    url = 'https://public.missiveapp.com/v1/users'
    headers = {
        'Authorization': f'Bearer {MISSIVE_API_KEY}'
    }
    users = []
    limit = 200  # Max value allowed by Missive API
    offset = 0   # Start offset

    while True:
        try:
            params = {
                'limit': limit,
                'offset': offset
            }
            response = requests.get(url, headers=headers, params=params, timeout=10)
            print(f"Fetching users with offset {offset}, limit {limit}. Status Code: {response.status_code}")
            response.raise_for_status() 

            page_users = response.json().get('users', [])
            if not page_users:
                print("No more users found.")
                break
            
            users.extend(page_users)
            print(f"Fetched {len(page_users)} users. Total users so far: {len(users)}")
            
            offset += limit
            sleep(0.5)  # Optional delay to avoid rate limiting

        except RequestException as e:
            print(f"An error occurred: {e}")
            break

    return users

# Fetch and print Missive users
users = get_missive_users()
missive_user_set = {user['name'].strip() for user in users}

#  Print the names
print("Missive Users:")
for name in missive_user_set:
    print(name)
"""
for a_number, paralegal_name in data_dict.items():
    normalized_name = extract_main_name(paralegal_name)
 
    if normalized_name in missive_user_set:
        print(f"Paralegal '{paralegal_name}' found in Missive with name: {paralegal_name}")
    else:
        print(f"Paralegal '{paralegal_name.strip()}' not found in Missive.")

if users:
    print('Missive Users:')
    for user in users:
        print(f"ID: {user['id']}, Name: {user['name']}, Email: {user['email']}")
else:
    print('No users found.')

"""

"""3.step
"""
"""
if  users:
        print("\nAll Missive Users:")
        for user in users:
            print(f"ID: {user['id']}, Name: {user['name']}, Email: {user.get('email', 'No email')}")
else:
    print('No users found in Missive.')
"""