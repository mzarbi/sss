import os
import pickle
from pathlib import Path
from typing import Dict

try:
    from boto.roboto.awsqueryservice import NoCredentialsError
    from dask.bytes.tests.test_s3 import boto3
except:pass

from core.server import KVServer
from core.trie import Trie
from core.utils import ensure_json_output, TCPMessage, get_filter_classes
from abc import ABC, abstractmethod


def create_filter(data):
    filter_classes = get_filter_classes()
    filter_type = data['type']
    if filter_type not in filter_classes:
        raise ValueError(f'Unknown filter type: {filter_type}')

    return filter_classes[filter_type](data)


class AbstractPetalsServer(KVServer, ABC):
    data = Trie()

    def __init__(self, host, port):
        super().__init__(host, port)
        self.load_data()

    @abstractmethod
    def load_data(self):
        pass

    @abstractmethod
    def load_raw_data(self, keys):
        pass

    def load_column_data(self, keys):
        data = self.load_raw_data(keys)
        return create_filter(data)

    def process_condition(self, condition: Dict, store: str) -> set:
        if 'condition' in condition and 'rules' in condition:
            # This is a composite condition
            sets = [self.process_condition(rule, store) for rule in condition['rules']]
            if condition['condition'] == 'and':
                return set.intersection(*sets)
            else:  # condition['condition'] == 'or'
                return set.union(*sets)
        else:
            # This is a single condition
            field = condition['field']
            value = condition['value']
            relevant_files = set()
            for keys in self.data.keys():
                store_name, file_name, column_file = keys.split('/')
                if store_name == store and column_file.startswith(field):
                    filter = self.data.search([store, file_name, column_file])
                    if filter is None:
                        filter = self.load_column_data([store, file_name, column_file])
                        self.data.insert([store, file_name, column_file], filter)
                    if filter.test(value):
                        relevant_files.add(file_name)
            return relevant_files

    async def init_handlers(self):
        super().init_handlers()

        @self.message_handler('query')
        @ensure_json_output
        async def query_handler(message: TCPMessage):
            store = message.payload['store']
            query = message.payload['query']
            relevant_files = self.process_condition(query, store)
            return list(relevant_files)


class PetalsServer(AbstractPetalsServer):
    def __init__(self, host, port, stores_dir):
        self.stores_dir = stores_dir
        super().__init__(host, port)

    def load_data(self):
        for root, _, files in os.walk(self.stores_dir):
            for file in files:
                if file.endswith('.pickle'):
                    path = Path(root) / file
                    store = path.parts[-3]
                    filename = path.parts[-2]
                    column = path.stem
                    self.data.insert([store, filename, column], None)

    def load_raw_data(self, keys):
        store, filename, column = keys
        path = Path(self.stores_dir) / store / filename / f"{column}.pickle"
        with open(path, 'rb') as f:
            data = pickle.load(f)
        return data


class S3PetalsServer(AbstractPetalsServer):
    def __init__(self, host, port, s3_bucket):
        self.s3_bucket = s3_bucket
        self.s3_client = boto3.client('s3')
        super().__init__(host, port)

    def load_data(self):
        try:
            response = self.s3_client.list_objects(Bucket=self.s3_bucket)
            for file in response['Contents']:
                if file['Key'].endswith('.pickle'):
                    path = Path(file['Key'])
                    store = path.parts[-3]
                    filename = path.parts[-2]
                    column = path.stem
                    self.data.insert([store, filename, column], None)
        except NoCredentialsError:
            print("No AWS credentials were found.")

    def load_raw_data(self, keys):
        store, filename, column = keys
        path = f'{store}/{filename}/{column}.pickle'
        try:
            s3_object = self.s3_client.get_object(Bucket=self.s3_bucket, Key=path)
            data = pickle.loads(s3_object['Body'].read())
            return data
        except NoCredentialsError:
            print("No AWS credentials were found.")
