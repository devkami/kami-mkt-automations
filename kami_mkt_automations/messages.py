import json
import logging
from dataclasses import dataclass, field
from os import getenv
from typing import List, Dict

from jinja2 import Environment, FileSystemLoader
from kami_logging import benchmark_with, logging_with
from kami_messenger.botconversa import Botconversa
from kami_messenger.email_messenger import EmailMessenger
from kami_messenger.messenger import Message
from database import get_dataframe_from_sql
from constant import MESSENGER_TYPES

messages_looger = logging.getLogger('messages_generator')
template_loader = FileSystemLoader('messages/templates')
template_env = Environment(loader=template_loader)


@dataclass(order=True)
class Contact:
    sort_index: int = field(init=False, repr=False)
    id: int = 0
    name: str = ''
    email: str = ''
    phone: str = ''
    groups: list = field(default_factory=list)

    def __post_init__(self):
        self.sort_index = self.id

@logging_with(messages_looger)
@benchmark_with(messages_looger)
def get_contacts_from_json(json_file) -> List[Contact]:
    contacts = []
    with open(json_file) as contacts_file:
        contacts_dict = json.load(contacts_file)

    for contact_dict in contacts_dict:
        contact = Contact(**contact_dict)
        contacts.append(contact)

    return contacts

class MissingIdError(Exception):
    def __init__(self, id):
        self.id = id
        super().__init__(f"There is no contact for the given id! given id = {id}")

def binary_search(contact_list, target_id, left, right):
    while left <= right:
        mid = (left + right) // 2
        if contact_list[mid].sort_index == target_id:
            return contact_list[mid]
        elif contact_list[mid].sort_index < target_id:
            left = mid + 1
        else:
            right = mid - 1
    raise MissingIdError(target_id)

@logging_with(messages_looger)
@benchmark_with(messages_looger)
def get_contact_by_id(target_id: int, contact_list: List[Contact]) -> Contact | None:    
    try:
        return binary_search(contact_list, target_id, 0, len(contact_list) - 1)
    except MissingIdError as e:
        messages_looger.exception(e)
    except Exception as e:
        messages_looger.exception("An unknow error occurred:", e)        
    return None


@logging_with(messages_looger)
def filter_contact_by_group(contacts:List[Contact], group: str) -> List[Contact]:
    return [contact for contact in contacts if group in contact.groups]

@logging_with(messages_looger)
@benchmark_with(messages_looger)
def get_sellers_contacts_from_database():
    script = 'data/in/contact_sellers.sql'
    contact_sellers = get_dataframe_from_sql(script)
    contact_sellers['groups'] = 'seller'
    contacts = contact_sellers.apply(lambda row: Contact(**row), axis=1)

    for contact in contacts:
        contact.groups = [contact.groups]

    return contacts

@logging_with(messages_looger)
@benchmark_with(messages_looger)
def generate_message_by_template(template_name: str, contact: Contact, message_dict: Dict) -> Message | None:
    message_template = template_env.get_template(f'{template_name}_message.md')
    message_dict['contact_name'] = contact.name
    message_body = message_template.render(message_dict)
    return Message(
        sender='',
        recipients=[],
        body=message_body,
        subject=message_dict['subject']
    )

@logging_with(messages_looger)
def send_email(message):
    email_messenger_str = {
        'name': 'Email - kamico.com.br',
        'messages': [message],
        'credentials': {
            'login': str(getenv('EMAIL_USER')),
            'password': str(getenv('EMAIL_PASSWORD')),
        },
        'engine': '',
    }
    email_messenger = EmailMessenger(**email_messenger_str)
    email_messenger.sendMessage()

@logging_with(messages_looger)
def send_whatsapp_message(message):
    botconversa_data = {
        'name': 'Botconversa',
        'messages': [message],
        'credentials': {'api-key': str(getenv('BOTCONVERSA_API_KEY'))},
        'engine': '',
    }
    botconversa = Botconversa(**botconversa_data)
    botconversa.sendMessage()

@logging_with(messages_looger)
def send_message_by_messenger(messenger: str, message: Message, contact: Contact):
    if messenger not in MESSENGER_TYPES:
        messages_looger.error(f'Mesenger Type:{messenger}, does not exit\'s')

    if messenger == 'whatsapp':
        message.recipients = [contact.phone]
        send_whatsapp_message(message=message)

    if messenger == 'email':
        message.recipients = [contact.email]
        send_email(message=message)

    messages_looger.error('Unable to send message')

@logging_with(messages_looger)
def send_message_by_all_messengers(message: Message, contact: Contact):
    for messenger in MESSENGER_TYPES:
        send_message_by_messenger(messenger, message, contact)

@logging_with(messages_looger)
@benchmark_with(messages_looger)
def send_message_by_group(template_name: str, group: str, message_dict: Dict, contacts: List[Contact]):
    filtered_contacts = filter_contact_by_group(contacts, group)    
    for contact in filtered_contacts:
        message = generate_message_by_template(template_name, contact, message_dict)
        send_message_by_all_messengers(message, contact)

@logging_with(messages_looger)
@benchmark_with(messages_looger)
def send_message_by_seller_id(contact_id: int, template_name: str, message_dict: str):
    contacts = get_sellers_contacts_from_database()
    sellers_contacts = filter_contact_by_group(contacts, 'seller')    
    contact = get_contact_by_id(contact_id, sellers_contacts)
    message = generate_message_by_template(template_name, contact, message_dict)
    send_message_by_all_messengers(message, contact)