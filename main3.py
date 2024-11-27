import test
import requests
import re
import pandas as pd
import time
from datetime import datetime, timedelta, timezone
from bs4 import BeautifulSoup
from dateutil import parser
from dataclasses import dataclass
from typing import List
import logging
import math  
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

EOIR_TEAM_ID = 'e3aa36e4-d631-488d-8002-35f8e85bb824'
CSV_FILE_PATH = 'cases.csv'
TIME_WINDOW_MINUTES = 30  # aile üye mailleri için offset time, aynı soyisimde unassigned-assigned mailler varsa aynı paralegale assign et.

@dataclass
class Client:
    first_name: str
    last_name: str
    lead_attorney: str
    originating_attorney: str

@dataclass
class Message:
    id: str
    body: str
    created_at: datetime
    conversation_id: str

@dataclass
class Conversation:
    id: str
    messages: List[Message]


def load_client_data(csv_file_path):
    df = pd.read_csv(csv_file_path)
    clients = process_client_data(df)
    return clients


# Fetch data from csv file, burayı optimize etmek lazım, database : case_name - lead_attorney,originating_attorney
def process_client_data(df):
    clients = []
    for _, row in df.iterrows():
        case_name = row['Case/Matter Name']
        lead_attorney = row['Lead Attorney']
        originating_attorney = row['Originating Attorney'] 
        
        if pd.isnull(case_name) or (pd.isnull(lead_attorney) and pd.isnull(originating_attorney)):
            continue
        #if pd.isnull(lead_attorney):
         #   lead_attorney = ''
        #else:
         #   lead_attorney = str(lead_attorney).strip()

        #if pd.isnull(originating_attorney):
          #  originating_attorney = ''
        #else:
         #   originating_attorney = str(originating_attorney).strip()

        # Extract client name from case_name
        #case_name = re.sub(r'^\[.*?\]\s*', '', case_name).strip()
        client_name_part = case_name.split('-')[0].strip() if '-' in case_name else case_name.strip()
        client_name_cleaned = re.sub(r'\s+ve\s+(Ailesi|eşi)', '', client_name_part, flags=re.IGNORECASE)
        
        # Split the name into parts
        name_parts = client_name_cleaned.split()
        first_name = ' '.join(name_parts[:-1]) if len(name_parts) > 1 else name_parts[0]
        last_name = name_parts[-1] if len(name_parts) > 1 else ''

        clients.append(Client(
            first_name=first_name.upper(),
            last_name=last_name.upper(),
            lead_attorney=lead_attorney,
            originating_attorney=originating_attorney 
        ))
    return clients



clients = load_client_data(CSV_FILE_PATH)

# API interaction
def fetch_unassigned_conversations(team_id, start_date=None, end_date=None):
    conversations = []
    url = 'https://public.missiveapp.com/v1/conversations'
    headers = {
        'Authorization': f'Bearer {test.MISSIVE_API_KEY}'
    }
    params = {
        'team_all': team_id,
        'limit': 50,
    }

    while True:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            logger.error(f'Failed to retrieve conversations: {response.text}')
            break

        response_json = response.json()
        if 'error' in response_json:
            logger.error(f"API Error: {response_json['error']}")
            break

        convos = response_json.get('conversations', [])
        if not convos:
            break

        for convo in convos:
            last_activity = parse_created_at(convo.get('last_activity_at'))
            if start_date and last_activity < start_date:
                return conversations  # Daha eski konuşmalara gerek yok
            if end_date and last_activity > end_date:
                continue  # Daha yeni konuşmaları atla
            if not convo.get('assignees'):
                conversations.append(Conversation(id=convo['id'], messages=[]))

        # Pagination için until parametresini ayarla
        oldest_last_activity = convos[-1].get('last_activity_at')
        params['until'] = oldest_last_activity

        # Eğer gelen konuşma sayısı limitten azsa veya son sayfaya geldiysek döngüyü bitir
        if len(convos) < params['limit']:
            break

    logger.info(f"Total conversations retrieved: {len(conversations)}")
    return conversations


def fetch_conversation_messages(conversation_id):
    url = f'https://public.missiveapp.com/v1/conversations/{conversation_id}/messages'
    headers = {
        'Authorization': f'Bearer {test.MISSIVE_API_KEY}'
    }
    params = {
        'limit': 10
    }
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        messages_data = response.json().get('messages', [])
        if not messages_data:

            return []

        messages = []
        for msg in messages_data:
            message_id = msg.get('id')
            messages.append(Message(
                id=message_id,
                body='',  # We'll fetch the body separately
                created_at=None,  # We'll fetch created_at separately
                conversation_id=conversation_id
            ))
        return messages
    else:
        logger.error(f'Failed to retrieve messages for conversation {conversation_id}: {response.text}')
        return []

def get_full_message(message_id):
    url = f'https://public.missiveapp.com/v1/messages/{message_id}'
    headers = {
        'Authorization': f'Bearer {test.MISSIVE_API_KEY}'
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        response_json = response.json()
        msg = response_json.get('messages', {})
        if not msg:
            logger.error(f"No 'messages' field found in response for message {message_id}")
            return None, None

        # Since 'messages' might be a list, we need to check and adjust accordingly
        if isinstance(msg, list):
            msg = msg[0] if msg else {}

        body = msg.get('body', '')
        created_at = msg.get('createdAt', msg.get('created_at'))

        return body, created_at
    else:
        logger.error(f'Failed to retrieve full message for ID {message_id}: {response.text}')
        return None, None

# Client Processing Module
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
        'MEHMET': ['MEHMED','MEMET'],
        'MEHMED': ['MEHMET','MEMET'],
        'MEMET': ['MEHMET','MEHMED'],
    }
    normalized_name = ''.join(turkish_char_map.get(char, char) for char in name)
    for key, variants in name_variants.items():
        if normalized_name in variants or normalized_name == key:
            normalized_name = key
            break
    return normalized_name.upper()

def parse_created_at(timestamp):
    if isinstance(timestamp, (int, float)):
        # Unix timestamp ini datetime objesine dönüştür
        return datetime.fromtimestamp(timestamp, timezone.utc)
    elif isinstance(timestamp, str):
        try:
            return parser.isoparse(timestamp)
        except ValueError:
            logger.error(f"Unrecognized date format: {timestamp}")
            return None
    else:
        logger.error(f"Unrecognized type for timestamp: {type(timestamp)}")
        return None


def extract_client_details_from_body(body):
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
        'first_name': first_name,
        'surname': surname,
    }

# Assignment Module
def apply_assignment_rules(paralegal_name):
    if paralegal_name is None or (isinstance(paralegal_name, float) and math.isnan(paralegal_name)):
        return None
    if not isinstance(paralegal_name, str):
        paralegal_name = str(paralegal_name)
    if paralegal_name.strip().lower() == "arda mert geldi":
        paralegal_name = "Ismail Dislik"
    return paralegal_name

def match_client_to_paralegal(client_details):
    full_name = f"{client_details.get('first_name', '')} {client_details.get('surname', '')}".strip().upper()
    normalized_full_name = normalize_name(full_name)
    surname = normalize_name(client_details.get('surname', ''))
    #logger.info(full_name)
    matched_clients = []

    # Tam isim ve soyisim eşleşmesi
    for client in clients:
        client_full_name = f"{client.first_name} {client.last_name}".strip().upper()
        normalized_client_full_name = normalize_name(client_full_name)

        if normalized_full_name == normalized_client_full_name:
            matched_clients.append(client)
            break  # Tam eşleşme bulundu

    if not matched_clients:
        # İsim ve soyisim tam eşleşmesi yoksa, soyisimle eşleşenleri bulalım
        surname_matches = [client for client in clients if normalize_name(client.last_name) == surname]

        if len(surname_matches) == 1:
            # Mycase üzerindeki kişilerde tek bir soyisim eşleşmesi varsa assign et geç
            client = surname_matches[0]
            if client.lead_attorney:
                return client.lead_attorney
            elif client.originating_attorney:
                logger.info(f"Lead Attorney bulunamadı, Originating Attorney atanıyor: {client.originating_attorney}")
                return client.originating_attorney
        else:
            # Birden fazla veya hiç eşleşme yoksa
            return None
    else:
        # Tam eşleşme bulundu
        client = matched_clients[0]
        if client.lead_attorney:
            return client.lead_attorney
        elif client.originating_attorney:
            logger.info(f"Lead Attorney bulunamadı, Originating Attorney atanıyor: {client.originating_attorney}")
            return client.originating_attorney

    return None



def assign_conversation_to_paralegal(conversation_id, paralegal_name):
    if paralegal_name is None:
        logger.error(f"No paralegal name provided for conversation {conversation_id}.")
        return False
    paralegal_user = next((user for user in test.users if user['name'].strip() == paralegal_name.strip()), None)
    if not paralegal_user:
        logger.error(f"Paralegal '{paralegal_name}' not found in Missive users.")
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
            "text": f"{paralegal_name}",
            "notification": {
                "title": "Assignment Notification",
                "body": f"{paralegal_name}"
            }
        }
    }

    try:
        response = requests.post(url, json=data, headers=headers)
        response.raise_for_status()
        
        logger.info(f"Conversation {conversation_id} assigned to {paralegal_name}.")
        return True
    except requests.exceptions.HTTPError as e:
        logger.error(f"Failed to assign conversation {conversation_id}: {e.response.status_code} - {e.response.text}")
        return False

# main module
def run_assignment_process():
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=3)
    conversations = fetch_unassigned_conversations(EOIR_TEAM_ID, start_date=start_date, end_date=end_date)
    if not conversations:
        return

    all_messages = []

    for conversation in conversations:
        try:
            messages = fetch_conversation_messages(conversation.id)
            if messages:
                for message in messages:
                    body, created_at_str = get_full_message(message.id)
                    if not body:
                        continue
                    message.body = body
                    created_at = parse_created_at(created_at_str)
                    if not created_at:
                        continue
                    message.created_at = created_at
                    client_details = extract_client_details_from_body(body)
                    if not client_details['surname']:
                        continue
                    message.client_details = client_details
                    message.conversation_id = conversation.id
                    all_messages.append(message)
        except Exception as e:
            logger.exception(f"An error occurred while processing conversation {conversation.id}")

    # Now group messages by surname and time window
    group_and_assign_messages(all_messages)


def process_conversation(conversation, assigned_paralegals):
    messages = fetch_conversation_messages(conversation.id)
    if not messages:

        return

    for message in messages:
        try:
            process_message(message, assigned_paralegals)
        except Exception as e:
            logger.exception(f"An error occurred while processing message {message.id}")
def group_and_assign_messages(messages):
    # Group messages by surname
    messages_by_surname = {}
    for message in messages:
        surname = normalize_name(message.client_details['surname'])
        if surname not in messages_by_surname:
            messages_by_surname[surname] = []
        messages_by_surname[surname].append(message)

    # For each surname, group messages by time window and assign
    for surname, messages_list in messages_by_surname.items():
        # Sort messages by created_at
        messages_list.sort(key=lambda m: m.created_at)

        # Group messages into time windows
        groups = []
        current_group = [messages_list[0]]
        for i in range(1, len(messages_list)):
            time_diff = (messages_list[i].created_at - messages_list[i-1].created_at).total_seconds() / 60
            if time_diff <= TIME_WINDOW_MINUTES:
                current_group.append(messages_list[i])
            else:
                groups.append(current_group)
                current_group = [messages_list[i]]
        groups.append(current_group)  # Add the last group
        
        # Aile olan müvekkillerin sadece başvuranın bilgileri Mycase'te oluyor, fakat bütün aile üyeleriyle ilgili
        # mail gelebiliyor, bu durumda belli bir timeframe içerisinde gelen mailleri gruplayıp soyadı aynı olan bütün müvekkilleri tek bir paralegale assign edebiliriz.
        
        # For each group, attempt to match to a paralegal and assign
        for group in groups:
            paralegal_name = None
            # Try to find a paralegal for any message in the group
            for message in group:
                paralegal_name = match_client_to_paralegal(message.client_details)
                if paralegal_name:
                    paralegal_name = apply_assignment_rules(paralegal_name)
                    break  # Found a paralegal, no need to check other messages
            if paralegal_name:
                # Assign all messages in the group to the paralegal
                for message in group:
                    assign_conversation_to_paralegal(message.conversation_id, paralegal_name)
            else:
                # No paralegal found, log for manual review
                for message in group:
                    full_name = f"{message.client_details.get('first_name', '')} {message.client_details['surname']}".strip()
                    logger.info(f"No paralegal found for '{full_name}'. Please review manually.")


def process_message(message, assigned_paralegals):
    body, created_at_str = get_full_message(message.id)
    if not body:
        return

    client_details = extract_client_details_from_body(body)
    if not client_details['surname']:
        return

    # Parse the created_at timestamp
    created_at = parse_created_at(created_at_str)
    if not created_at:
        return
    message.created_at = created_at
    message.body = body

    surname = client_details['surname']
    full_name = f"{client_details.get('first_name', '')} {surname}".strip()
    paralegal_name = match_client_to_paralegal(client_details)
    paralegal_name = apply_assignment_rules(paralegal_name)

    # Update assigned_paralegals regardless of whether a paralegal was found
    assigned_info = assigned_paralegals.get(surname)
    if assigned_info:
        time_diff = abs((created_at - assigned_info['timestamp']).total_seconds()) / 60
        if time_diff <= TIME_WINDOW_MINUTES:
            if paralegal_name is None:
                # Use the paralegal from the assigned_paralegals
                paralegal_name = assigned_info['paralegal']
        else:
            # Update assigned_paralegals with the latest info
            assigned_paralegals[surname] = {
                'paralegal': paralegal_name,
                'timestamp': created_at
            }
    else:
        # Add the surname to assigned_paralegals
        assigned_paralegals[surname] = {
            'paralegal': paralegal_name,
            'timestamp': created_at
        }

    if paralegal_name:
        assign_conversation_to_paralegal(message.conversation_id, paralegal_name)
    else:
        logger.info(f"No paralegal found for '{full_name}'. Please review manually.")
# Main Loop
def main():
    while True:
        try:
            run_assignment_process()
        except Exception as e:
            logger.exception("An error occurred during the assignment process.")
        time.sleep(30)
        #shutdown()
        

def shutdown():
    zaman_farki = datetime.now() - start_time
    if(zaman_farki >= timedelta(hours=16)):
        os.system('shutdown /s /t 1')


if __name__ == "__main__":
    start_time = datetime.now()
    main()
