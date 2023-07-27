import asyncio
import base64
import json
import pickle
import warnings
warnings.filterwarnings("ignore")
from core.client import PetalsClient
from core.utils import TCPMessage

client = PetalsClient('127.0.0.1', 8888)


query_condition = {
    'condition': 'and',
    'rules': [
        {
            'field': 'account_status',
            'value': 'Inactive'
        },
        {
            'field': 'account_type',
            'value': 'Savings'
        },
        {
            'field': 'loan_status',
            'value': 'Current'
        }
    ]
}

store = "store_name"  # replace with your actual store name
query_message = {'store': store, 'query': query_condition}
matching_files = asyncio.run(client.send_search_query(query_message))


for file_path in matching_files:
    print(file_path)

# Creating a message with text format
message = TCPMessage("message", "text", "Hello server!")
asyncio.run(client.send_message(message))



