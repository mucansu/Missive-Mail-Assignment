import test
import requests
import re
import pandas as pd
from datetime import datetime, timedelta

# Constants
EOIR_TEAM_ID = 'e3aa36e4-d631-488d-8002-35f8e85bb824'
CSV_FILE_PATH = 'cases.csv'

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
        conversations = response.json().get('conversations', [])
        unassigned_conversations = [
            convo for convo in conversations if not convo.get('assignees')
        ]
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

# Function to extract details such as client name and timestamp
def extract_email_details(body):
    client_name_match = re.search(r'Noncitizen Name:\s*([^,]+),\s*(\w+)', body)
    uploaded_on_match = re.search(r'Uploaded On:\s*(\d{2}/\d{2}/\d{4})', body)

    return {
        'surname': client_name_match.group(1).strip() if client_name_match else None,
        'full_name': f"{client_name_match.group(1).strip()}, {client_name_match.group(2).strip()}" if client_name_match else None,
        'uploaded_on': datetime.strptime(uploaded_on_match.group(1), "%m/%d/%Y") if uploaded_on_match else None
    }

# Function to group related emails by surname and a 5-minute time window
def group_related_emails(emails):
    grouped_emails = {}
    for email in emails:
        details = extract_email_details(email['body'])
        details['conversation_id'] = email.get('conversation_id')
        
        if details['surname'] and details['uploaded_on']:
            time_window_start = details['uploaded_on'] - timedelta(minutes=5)
            time_window_end = details['uploaded_on'] + timedelta(minutes=5)

            # Look for an existing group that matches the surname and is within the 5-minute window
            matched_group = None
            for (surname, timestamp), group in grouped_emails.items():
                if surname == details['surname'] and time_window_start <= timestamp <= time_window_end:
                    matched_group = (surname, timestamp)
                    break
            
            if matched_group:
                grouped_emails[matched_group].append(details)
            else:
                grouped_emails[(details['surname'], details['uploaded_on'])] = [details]
    
    return grouped_emails

# Function to assign conversation to the correct paralegal
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
            "text": "Bu mail ilgili paralegale aktarılmıştır ",  
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

# Function to match client with paralegal based on names
def match_client_to_paralegal(first_name, last_name, lead_attorney):
    # Check if the lead attorney is Arda Mert Geldi, then assign Ismail Dislik as well
    if lead_attorney == "Arda Mert Geldi":
        return "Ismail Dislik"
    
    # Find client in the processed clients list
    matched_client = next(
        (client for client in csv_clients if client['first_name'] == first_name and client['last_name'] == last_name), 
        None
    )
    if matched_client:
        paralegal_name = matched_client.get('lead_attorney')
        return paralegal_name
    else:
        print(f"Client '{first_name} {last_name}' not found in processed clients.")
        return None

# Main function to assign grouped emails to the correct paralegal
# Main function to assign grouped emails to the correct paralegal
def assign_group_to_paralegal(group_emails):
    # Ensure there's at least one valid email body in the group
    first_email = next((email for email in group_emails if 'body' in email and email['body']), None)
    
    if not first_email:
        print("No valid email body found in this group; skipping assignment.")
        return

    # Extract details from the first valid email
    details = extract_email_details(first_email.get('body', ''))
    
    # Check if full_name is available to avoid further errors
    if not details.get('full_name'):
        print("Missing client full name in the email details; skipping assignment.")
        return
    
    name_parts = details['full_name'].split(',')
    first_name = name_parts[1].strip().upper() if len(name_parts) > 1 else ""
    last_name = name_parts[0].strip().upper() if name_parts else ""

    # Match the client to the appropriate paralegal
    paralegal_name = match_client_to_paralegal(first_name, last_name, details.get('lead_attorney', ''))
    if not paralegal_name:
        print(f"Paralegal for '{first_name} {last_name}' could not be identified.")
        return

    print(f"Assigning all emails in the group to paralegal: {paralegal_name}")

    for email in group_emails:
        convo_id = email.get('conversation_id')
        # Ensure body exists before assigning
        if convo_id and 'body' in email and email['body']:
            assign_conversation_to_paralegal(convo_id, paralegal_name)
        else:
            print("Missing conversation ID or email body; cannot assign.")


# Main workflow
def main():
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
            email_details = extract_email_details(body)
            if email_details['full_name']:
                all_emails.append({'body': body, 'conversation_id': convo_id})

    grouped_emails = group_related_emails(all_emails)

    for group_key, group in grouped_emails.items():
        print(f"\nGroup: Surname '{group_key[0]}' with emails uploaded on {group_key[1]}")
        assign_group_to_paralegal(group)
    

# Run the main function
if __name__ == "__main__":
    main()
