import test
import requests
import re
import pandas as pd
import time
from datetime import datetime, timedelta
from fuzzywuzzy import fuzz, process
import Levenshtein as lev
from bs4 import BeautifulSoup
from dateutil import parser

# Constants
EOIR_TEAM_ID = 'e3aa36e4-d631-488d-8002-35f8e85bb824'
CSV_FILE_PATH = 'cases.csv'
TIME_WINDOW_MINUTES = 5  # Time window to consider emails as related family members
def normalize_name(name):
    if not isinstance(name, str):
        name = str(name) if name is not None else ''
    
    turkish_char_map = {
        'ç': 'c', 'Ç': 'C',
        'ğ': 'g', 'Ğ': 'G',
        'ı': 'i', 'İ': 'I',
        'ö': 'o', 'Ö': 'O',
        'ş': 's', 'Ş': 'S',
        'ü': 'u', 'Ü': 'U'
    }
    name_variants = {
        'MUHAMMED': ['MUHAMMET', 'MOHAMMED', 'MOHAMET'],
        'MUHAMMET': ['MUHAMMED', 'MOHAMMED', 'MOHAMET'],
        'MOHAMMED': ['MUHAMMED', 'MUHAMMET', 'MOHAMET'],
        'MOHAMET': ['MUHAMMED', 'MUHAMMET', 'MOHAMMED'],
       
    }
    normalized_name = ''.join(turkish_char_map.get(char, char) for char in name)
    for key, variants in name_variants.items():
        if normalized_name in variants or normalized_name == key:
            normalized_name = key
            break
    return normalized_name.upper()
def parse_created_at(created_at):
    if isinstance(created_at, str):
        try:
            return parser.isoparse(created_at)
        except ValueError:
            print(f"Unrecognized date format: {created_at}")
            return None
    elif isinstance(created_at, (int, float)):
            # Incorrectly indented
            return datetime.fromtimestamp(created_at)
    else:
        print(f"Unrecognized type for created_at: {type(created_at)}")
        return None
# Load the CSV data and process to extract clients
def process_csv_clients(df):
    clients = []
    for _, row in df.iterrows():
        case_name = row['Case/Matter Name']
        lead_attorney = row['Lead Attorney']

        # Extract client name from case_name
        client_name_part = case_name.split('-')[0].strip() if '-' in case_name else case_name.strip()
        client_name_cleaned = re.sub(r'\s+ve\s+Ailesi', '', client_name_part, flags=re.IGNORECASE)

        # Split the name into parts
        name_parts = client_name_cleaned.split()
        first_name = ' '.join(name_parts[:-1]) if len(name_parts) > 1 else name_parts[0]
        last_name = name_parts[-1] if len(name_parts) > 1 else ''
       
        # Store the client information
        clients.append({
            'first_name': first_name.upper(),
            'last_name': last_name.upper(),
            'lead_attorney': lead_attorney
        })
    
    return clients

# Load and process CSV data
df = pd.read_csv(CSV_FILE_PATH)
csv_clients = process_csv_clients(df)
processed_clients_df = pd.DataFrame(csv_clients)

# Function to fetch team conversations
def get_team_conversations(team_id):
    url = 'https://public.missiveapp.com/v1/conversations'
    headers = {
        'Authorization': f'Bearer {test.MISSIVE_API_KEY}'
    }
    params = {
        'team_all': team_id,
        'limit': 50,
        'page': 15
    }
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        response_json = response.json()
        if 'error' in response_json:
            print(f"API Error: {response_json['error']}")
            return []
        conversations = response_json.get('conversations', [])
        unassigned_conversations = [convo for convo in conversations if not convo.get('assignees')]
        print(f"Total conversations retrieved: {len(unassigned_conversations)}")
        return unassigned_conversations
    else:
        print(f'Failed to retrieve conversations: {response.text}')
        return []

# Function to fetch messages from a conversation
def get_conversation_messages(conversation_id):
    url = f'https://public.missiveapp.com/v1/conversations/{conversation_id}/messages'
    headers = {
        'Authorization': f'Bearer {test.MISSIVE_API_KEY}'
    }
    params = {
        'limit': 10
    }
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        return response.json().get('messages', [])
    else:
        print(f'Failed to retrieve messages for conversation {conversation_id}: {response.text}')
        return []

# Function to fetch the full message content
def get_full_message(message_id):
    url = f'https://public.missiveapp.com/v1/messages/{message_id}'
    headers = {
        'Authorization': f'Bearer {test.MISSIVE_API_KEY}'
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json().get('messages', {})
    else:
        print(f'Failed to retrieve full message for ID {message_id}: {response.text}')
        return {}

# Function to extract client details such as full name and timestamp from the message body
def extract_client_details(body):
    # Parse the HTML content and extract plain text
    soup = BeautifulSoup(body, 'html.parser')
    text = soup.get_text(separator=' ')

    client_name_match = re.search(r'Noncitizen Name:\s*([^,]+),\s*(.+)', text)
    if client_name_match:
        surname = client_name_match.group(1).strip()
        first_name = client_name_match.group(2).strip()
    else:
        surname = None
        first_name = None

    return {
        'surname': surname,
        'first_name': first_name,
    }





# Function to group related emails by surname and a time window
def group_related_emails(emails):
    grouped_emails = {}
    for email in emails:
        details = extract_client_details(email['body'])
        details['conversation_id'] = email.get('conversation_id')
        created_at = email.get('created_at')
        
        print(f"created_at value: {created_at}, type: {type(created_at)}")  # Debugging
        
        if created_at:
            if isinstance(created_at, (int, float)):
                # Check if the timestamp is in milliseconds
                if created_at > 1e12:
                    # Timestamp is in milliseconds
                    details['uploaded_on'] = datetime.utcfromtimestamp(created_at / 1000)
                else:
                    # Timestamp is in seconds
                    details['uploaded_on'] = datetime.utcfromtimestamp(created_at)
            elif isinstance(created_at, str):
                try:
                    details['uploaded_on'] = datetime.strptime(
                        created_at, "%Y-%m-%dT%H:%M:%S.%fZ"
                    )
                except ValueError:
                    try:
                        details['uploaded_on'] = datetime.strptime(
                            created_at, "%Y-%m-%dT%H:%M:%SZ"
                        )
                    except ValueError:
                        print(f"Unrecognized date format: {created_at}")
                        details['uploaded_on'] = None
            else:
                print(f"Unrecognized type for created_at: {type(created_at)}")
                details['uploaded_on'] = None
        else:
            details['uploaded_on'] = None

        if details['surname'] and details['uploaded_on']:
            time_window_start = details['uploaded_on'] - timedelta(minutes=TIME_WINDOW_MINUTES)
            time_window_end = details['uploaded_on'] + timedelta(minutes=TIME_WINDOW_MINUTES)

            # Look for an existing group that matches the surname and is within the time window
            matched_group = None
            for (surname, timestamp), group in grouped_emails.items():
                if surname == details['surname'] and time_window_start <= timestamp <= time_window_end:
                    matched_group = (surname, timestamp)
                    break

            # If a matching group is found, add to it; otherwise, create a new group
            if matched_group:
                grouped_emails[matched_group].append(details)
            else:
                grouped_emails[(details['surname'], details['uploaded_on'])] = [details]

    return grouped_emails



# Function to assign grouped emails to the correct paralegal
def assign_group_to_paralegal(group_emails):
    if group_emails:
        # Assume all emails in the group have the same first and last name
        first_name = group_emails[0]['first_name'].upper() if group_emails[0]['first_name'] else ''
        last_name = group_emails[0]['surname'].upper() if group_emails[0]['surname'] else ''
        paralegal_name = match_client_to_paralegal(first_name, last_name)
        if paralegal_name:
            print(f"Assigning all family members with name '{first_name} {last_name}' to paralegal: {paralegal_name}")
            for family_email in group_emails:
                convo_id = family_email.get('conversation_id')
                if convo_id:
                    # Explicitly override assignment if paralegal_name is "Arda Mert Geldi"
                    if paralegal_name.strip().lower() == "arda mert geldi".lower():
                        print(f"Overriding assignment of conversation {convo_id} from 'Arda Mert Geldi' to 'Ismail Dislik'")
                        assign_conversation_to_paralegal(convo_id, "Ismail Dislik")
                    else:
                        print(f"Assigning conversation {convo_id} to {paralegal_name}")
                        assign_conversation_to_paralegal(convo_id, paralegal_name)
            return
        else:
            print(f"No matching client found for name '{first_name} {last_name}'. Please verify manually.")
            return
    else:
        print("No paralegal found for the grouped family emails; please review manually.")



# Function to match client to paralegal based on first and last names and lead attorney
def match_client_to_paralegal(full_name):
    normalized_full_name = normalize_name(full_name)
    for client in csv_clients:
        client_full_name = f"{client.get('first_name', '')} {client.get('last_name', '')}"
        normalized_client_full_name = normalize_name(client_full_name)
        if normalized_full_name == normalized_client_full_name:
            return client['lead_attorney']
    #burada çift isimli olup sistemde tek isimle girilmiş olanları bulmaya çalışıyoruz, çalışmazsa kaldır:
    name_parts = full_name.split()
    if len(name_parts) > 1:
        first_name = name_parts[0]
        last_name = name_parts[-1]
        combined_name = f"{first_name} {last_name}".upper()
        
        for client in csv_clients:
            client_combined_name = f"{client.get('first_name', '')} {client.get('last_name', '')}".upper()
            if combined_name == client_combined_name:
                return client['lead_attorney']
            if normalize_name(first_name) in normalize_name(client.get('first_name', '')) and last_name == client.get('last_name', '').upper():
                return client['lead_attorney']
    return None





def assign_conversation_to_paralegal(conversation_id, paralegal_name):
    paralegal_user = next((user for user in test.users if user['name'].strip() == paralegal_name.strip()), None)
    if not paralegal_user:
        print(f"Paralegal '{paralegal_name}' not found in Missive users.")
        return False

    user_id = paralegal_user['id']

    url = 'https://public.missiveapp.com/v1/posts'
    headers = {
        'Authorization': f'Bearer {test.MISSIVE_API_KEY}',
        'Content-Type': 'application/json'
    }
    data = {
        "posts": {
            "conversation": conversation_id,
            "add_assignees": [user_id],
            "organization": 'f50f2ccf-e588-4b56-bb15-672a515e0e1e',
            "text": "This conversation has been assigned to the paralegal.",
            "notification": {
                "title": "Assignment Notification",
                "body": f"Conversation {conversation_id} has been assigned to {paralegal_name}."
            }
        }
    }

    try:
        response = requests.post(url, json=data, headers=headers)
        response.raise_for_status()
        print(f"Conversation {conversation_id} assigned to {paralegal_name}.")
        return True
    except requests.exceptions.HTTPError as e:
        print(f"Failed to assign conversation {conversation_id}: {e.response.status_code} - {e.response.text}")
        return False


# Main workflow function
def run_assignment_process():
    conversations = get_team_conversations(EOIR_TEAM_ID)
    if not conversations:
        print('No conversations found.')
        return

    assigned_paralegals = {}  # Key: last name, Value: {'paralegal': name, 'timestamp': datetime}

    for convo in conversations:
        convo_id = convo['id']
        messages = get_conversation_messages(convo_id)
        if not messages:
            print(f"No messages found in conversation {convo_id}.")
            continue

        for message in messages:
            msg_id = message['id']
            full_message = get_full_message(msg_id)
            body = full_message.get('body', '')
            created_at = full_message.get('created_at')
            email_details = extract_client_details(body)
            if not email_details['surname']:
                continue  # Skip if surname not found

            # Parse the created_at timestamp
            if created_at:
                uploaded_on = parse_created_at(created_at)
                if not uploaded_on:
                    continue
            else:
                continue

            full_name = f"{email_details['first_name']} {email_details['surname']}".upper()

            # Attempt to find a direct match in CSV
            paralegal_name = match_client_to_paralegal(full_name)

            if paralegal_name:
    # Direct match found, assign conversation
                if paralegal_name.strip().lower() == "arda mert geldi".lower():
                    print(f"Overriding assignment of conversation {convo_id} from 'Arda Mert Geldi' to 'Ismail Dislik'")
                    paralegal_name = "Ismail Dislik"

                assign_conversation_to_paralegal(convo_id, paralegal_name)
                # Record the assigned paralegal for this last name and time window
                assigned_paralegals[email_details['surname']] = {
                    'paralegal': paralegal_name,
                    'timestamp': uploaded_on
    }
            else:
                # Check if the last name has an assigned paralegal within the time window
                assigned_info = assigned_paralegals.get(email_details['surname'])
                if assigned_info:
                    time_diff = abs((uploaded_on - assigned_info['timestamp']).total_seconds()) / 60
                    if time_diff <= TIME_WINDOW_MINUTES:
                        # Assign to the same paralegal
                        assign_conversation_to_paralegal(convo_id, assigned_info['paralegal'])
                    else:
                        print(f"No direct match and no recent assignment for surname '{email_details['first_name'] + ' ' + email_details['surname']}'. Please review manually.")
                else:
                    print(f"No direct match and no assignment found for surname '{email_details['first_name'] + ' ' + email_details['surname']}'. Please review manually.")





# Loop to run the process every minute
def main():

    while(True):
        run_assignment_process()
        time.sleep(60)
       

# Run the main function
if __name__ == "__main__":
   # main()
   while(True):
        run_assignment_process()
        time.sleep(60)

        #düzeldi gibi