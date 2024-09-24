from google.oauth2 import service_account
from googleapiclient.discovery import build
import os
import regex as re
from time import sleep
from requests.exceptions import RequestException
# Replace with your spreadsheet ID
SPREADSHEET_ID = '1hBC_JXsHKSFSlHvlgE9p3ZYA0npKGP-IQrcTuzZU67I'  # e.g., '1hBC_JXsHKSFSlHvlgE9p3ZYA0npKGP-IQrcTuzZU67I'
SERVICE_ACCOUNT_FILE = 'credentials.json'

# Define the scope
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# Authenticate using the service account
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)

# Build the service
service = build('sheets', 'v4', credentials=credentials)

# Call the Sheets API
sheet = service.spreadsheets()

# Specify the range you want to read
RANGE_NAME = 'Anumbers'  # Adjust to match your sheet's name and desired range

def extract_main_name(paralegal_name):
    return paralegal_name.split(' - ')[0].strip()       
try:
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
    values = result.get('values', [])

    if not values:
        print('No data found in the database sheet.')
    else:
        # Assuming the first row is the header
        headers = values[0]
        data_rows = values[1:]

        # Dictionary to store A-numbers in their original format
        data_dict = {}
        for row in data_rows:
            if len(row) >= 2:
                a_number = row[0].strip()  # Keep the original A-number format
                paralegal_name = row[1].strip()
                # Use the original A-number as the key
                data_dict[a_number] = paralegal_name

        # Output the A-numbers in their original format
        print('Data dictionary:')
        for original_a_number, paralegal in data_dict.items():
            print(f'{original_a_number}: {paralegal}')
except Exception as e:
    print(f'An error occurred: {e}')



MISSIVE_API_KEY = "missive_pat-o2ylEV6WSMiMEr1NvFMWWIzYh9RGukzVn_rs4jMavbaFsn8ox7Sjfxufw1rWgpGxRuYqtw"

if not MISSIVE_API_KEY:
    
    raise ValueError("Missing MISSIVE_API_KEY environment variable.")
else: print(MISSIVE_API_KEY)

import requests

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
            
            # Print debugging information
            print(f"Fetching users with offset {offset}, limit {limit}. Status Code: {response.status_code}")
            response.raise_for_status()  # Raises an exception for HTTP errors

            # Extract users from response
            page_users = response.json().get('users', [])
            if not page_users:
                print("No more users found.")
                break
            
            users.extend(page_users)
            print(f"Fetched {len(page_users)} users. Total users so far: {len(users)}")
            
            # Increment the offset for the next request
            offset += limit
            sleep(0.5)  # Optional delay between requests to avoid rate limiting

        except RequestException as e:
            print(f"An error occurred: {e}")
            break

    return users        
# Fetch and print Missive users
users = get_missive_users()
# Using a set to avoid duplicate names
missive_user_set = {user['name'].strip() for user in users}

#print("Missive deki isimler:")
#for name in missive_user_set:
 #   print(f"Missive deki isimler: {name}")

for a_number, paralegal_name in data_dict.items():
    normalized_name = extract_main_name(paralegal_name)
    
    if normalized_name in missive_user_set:
        print(f"Paralegal '{paralegal_name}' found in Missive with name: {paralegal_name}")
    else:
        print(f"Paralegal '{paralegal_name.strip()}' not found in Missive.")
"""
if users:
    print('Missive Users:')
    for user in users:
        print(f"ID: {user['id']}, Name: {user['name']}, Email: {user['email']}")
else:
    print('No users found.')

"""

"""3.step
"""
def get_missive_users():
    url = 'https://public.missiveapp.com/v1/users'
    headers = {
        'Authorization': f'Bearer {MISSIVE_API_KEY}'
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json().get('users', [])
    else:
        print(f'Failed to retrieve users: {response.status_code} - {response.text}')
        return []

if  users:
        print("\nAll Missive Users:")
        for user in users:
            print(f"ID: {user['id']}, Name: {user['name']}, Email: {user.get('email', 'No email')}")
else:
    print('No users found in Missive.')
