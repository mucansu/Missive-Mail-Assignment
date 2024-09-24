import test
import requests
import re
import pandas as pd
import time
from datetime import datetime, timedelta
from fuzzywuzzy import fuzz, process
import Levenshtein as lev
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
    normalized_name = ''.join(turkish_char_map.get(char, char) for char in name)
    return normalized_name.upper()
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
        if len(name_parts) >= 2:
            first_name = ' '.join(name_parts[:-1])
            last_name = name_parts[-1]
        else:
            first_name = name_parts[0] if name_parts else ''
            last_name = ''

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
        'limit': 30,
        'page': 3
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
    client_name_match = re.search(r'Noncitizen Name:\s*([^,]+),\s*(\w+)', body)
    lead_attorney_match = re.search(r'Lead Attorney:\s*([\w\s]+)', body)
    uploaded_on_match = re.search(r'Uploaded On:\s*(\d{2}/\d{2}/\d{4})', body)

    return {
        'surname': client_name_match.group(1).strip() if client_name_match else None,
        'full_name': f"{client_name_match.group(1).strip()}, {client_name_match.group(2).strip()}" if client_name_match else None,
        'lead_attorney': lead_attorney_match.group(1).strip() if lead_attorney_match else None,
        'uploaded_on': datetime.strptime(uploaded_on_match.group(1), "%m/%d/%Y") if uploaded_on_match else None
    }

# Function to group related emails by surname and a time window
def group_related_emails(emails):
    grouped_emails = {}
    for email in emails:
        details = extract_client_details(email['body'])
        details['conversation_id'] = email.get('conversation_id')
        
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
    # Try to find a matched paralegal from one of the emails in the group
    for email in group_emails:
        first_name = email['full_name'].split(', ')[1].upper() if email['full_name'] else ''
        last_name = email['surname'].upper() if email['surname'] else ''
        lead_attorney = email.get('lead_attorney', '')

        # Safeguard: ensure lead_attorney is a string before stripping
        if lead_attorney:
            lead_attorney = lead_attorney.strip()

        paralegal_name = match_client_to_paralegal(first_name, last_name, lead_attorney)
        if paralegal_name:
            print(f"Assigning all family members with surname '{last_name}' to paralegal: {paralegal_name}")
            for family_email in group_emails:
                convo_id = family_email.get('conversation_id')
                if convo_id:
                    assign_conversation_to_paralegal(convo_id, paralegal_name)
            return

    print("No paralegal found for the grouped family emails; please review manually.")

# Function to match client to paralegal based on first and last names and lead attorney
def match_client_to_paralegal(first_name, last_name, lead_attorney):
    normalized_first_name = normalize_name(first_name)
    normalized_last_name = normalize_name(last_name)
    normalized_lead_attorney = normalize_name(lead_attorney) if lead_attorney else ''

    # Levenshtein distance thresholds
    name_distance_threshold = 3
    attorney_distance_threshold = 5

    exact_matches = []
    possible_matches = []

    for client in csv_clients:
        client_first_name = normalize_name(client.get('first_name', ''))
        client_last_name = normalize_name(client.get('last_name', ''))
        client_lead_attorney = normalize_name(client.get('lead_attorney', ''))

        # Exact match check
        if (client_first_name == normalized_first_name and
            client_last_name == normalized_last_name and
            (client_lead_attorney == normalized_lead_attorney or not lead_attorney)):
            exact_matches.append(client)
            continue  # Skip adding to possible matches if already an exact match

        # Check possible matches based on Levenshtein distance
        first_name_distance = lev.distance(normalized_first_name, client_first_name)
        last_name_distance = lev.distance(normalized_last_name, client_last_name)
        
        if first_name_distance <= name_distance_threshold and last_name_distance <= name_distance_threshold:
            # Handle case when lead attorney is missing or different
            if (not lead_attorney) or (client_lead_attorney == normalized_lead_attorney):
                possible_matches.append(client)

    # Automatically select the best match if exactly one perfect match found
    if len(exact_matches) == 1:
        return exact_matches[0]['lead_attorney']
    elif len(exact_matches) > 1:
        print("Multiple exact matches found; please verify manually:")
        for match in exact_matches:
            print(f"Exact match: {match['first_name']} {match['last_name']} with lead attorney {match['lead_attorney']}")
        return None

    # Select from possible matches if no exact match found
    if len(possible_matches) == 1:
        return possible_matches[0]['lead_attorney']
    elif len(possible_matches) > 1:
        print(f"Multiple clients found with the name '{first_name} {last_name}' and different lead attorneys. Please verify manually.")
        for match in possible_matches:
            print(f"Potential match: {match['first_name']} {match['last_name']} with lead attorney {match['lead_attorney']}")
        return None
    else:
        print(f"No matching client found for '{first_name} {last_name}' with lead attorney '{lead_attorney or 'None'}'. Please verify manually.")
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

    all_emails = []
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
            email_details = extract_client_details(body)
            if email_details['full_name']:
                all_emails.append({'body': body, 'conversation_id': convo_id})

    # Group related emails based on surname and time window
    grouped_emails = group_related_emails(all_emails)

    # Assign each group of emails to the appropriate paralegal
    for group_key, group in grouped_emails.items():
        print(f"\nGroup: Surname '{group_key[0]}' with emails uploaded on {group_key[1]}")
        assign_group_to_paralegal(group)

# Loop to run the process every minute
""" def main():
 while True:
        print("\nStarting the assignment process...")
        run_assignment_process()
        print("Waiting for 60 seconds before the next check...\n")
        time.sleep(60)
"""
# Run the main function
if __name__ == "__main__":
   # main()
   run_assignment_process()
