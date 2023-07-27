import json
import os
import pickle
from abc import abstractmethod, ABC
from pathlib import Path
from pyarrow import parquet as pq
import pandas as pd

from core.utils import get_filter_classes


class FilterSelector:
    def __init__(self, all_chunks, column):
        self.all_chunks = all_chunks
        self.column = column

    def select_filter_strategy(self, bloom_threshold: int, set_threshold: int):
        dtype = None
        unique_values = set()

        for chunk in self.all_chunks:
            series = chunk[self.column]  # Get the Series from the DataFrame
            if dtype is None:
                dtype = series.dtypes if pd.notnull(series.iloc[0]) else None
            unique_values.update(series.dropna().unique())

        unique_count = len(unique_values)

        if unique_count < bloom_threshold:
            return "bloom"
        elif unique_count < set_threshold:
            return "set"
        elif dtype in ["int64", "float64"]:
            return "range"
        elif dtype == "datetime64[ns]":
            return "daterange"
        elif dtype == "datetime.date":
            return "date"
        elif dtype == "datetime.time":
            return "time"
        elif dtype == "datetime.datetime":
            return "datetimetz"
        elif dtype == "bool":
            return "set"
        elif dtype.name == "category":
            return "set" if len(dtype.categories) <= set_threshold else "bloom"
        elif dtype == "object":
            return "bloom"
        else:
            raise ValueError(f"Cannot handle column with dtype {dtype}")


class AbstractFilterGenerator(ABC):

    DEFAULT_CHUNK_SIZE = 10000
    BLOOM_THRESHOLD = 10000
    SET_THRESHOLD = 1000

    def __init__(self, data_dir, store_name, filter_dir, config_file=None, included_columns=None):
        self.data_dir = data_dir
        self.store_name = store_name
        self.filter_dir = filter_dir
        self.filter_classes = get_filter_classes()
        self.config = {}
        self.included_columns = set(included_columns or [])

        # If a configuration file is provided, load it into the config dictionary
        if config_file is not None:
            with open(config_file, 'r') as f:
                self.config = json.load(f)

    def generate_filters(self):
        metadata = {}  # Store metadata about the filters

        for root, _, files in os.walk(self.data_dir):
            for file in self.get_files(root):
                path = Path(root) / file

                # Load data
                reader_for_whole_df = self.load_data(path, chunksize=10)
                df = next(reader_for_whole_df)

                # Skip if data is empty
                if df.empty:
                    continue

                columns_to_filter = self.included_columns if self.included_columns else df.columns

                for column in columns_to_filter:
                    new_file_dir = Path(self.filter_dir) / self.store_name / path.stem
                    new_file_dir.mkdir(parents=True, exist_ok=True)

                    # Prepare filter parameters
                    filter_params = self.prepare_filter_params(column, path)
                    FilterClass = self.filter_classes.get(filter_params["strategy"])

                    if FilterClass is None:
                        raise ValueError(f"No filter class for strategy '{filter_params['strategy']}'")

                    # Instantiate the filter
                    filter_instance = FilterClass.create(reader=filter_params["reader"], **filter_params["params"])

                    # Save the filter to disk
                    filter_path = f"{new_file_dir}/{column}.pickle"
                    with open(filter_path, 'wb') as f:
                        pickle.dump(filter_instance, f)

                    # Update the metadata
                    metadata[column] = {
                        'filter_type': filter_params["strategy"],
                        'relative_path': os.path.relpath(filter_path, self.filter_dir)
                    }

        # Write the metadata to a JSON file
        os.makedirs(os.path.join(self.filter_dir, 'stores_metadata'), exist_ok=True)
        with open(os.path.join(self.filter_dir, 'stores_metadata', f'{self.store_name}.json'), 'w') as f:
            json.dump(metadata, f)

    @abstractmethod
    def get_files(self, root):
        pass

    @abstractmethod
    def load_data(self, path, columns=None, chunksize=None):
        pass

    def prepare_filter_params(self, column, path):
        filter_params = {}
        reader_for_selector = self.load_data(path, columns=[column], chunksize=self.DEFAULT_CHUNK_SIZE)

        if column in self.config:
            filter_info = self.config[column]
            filter_strategy = filter_info["strategy"]
            if "params" in filter_info:
                filter_params = filter_info["params"]
        else:
            selector = FilterSelector(reader_for_selector, column)
            filter_strategy = selector.select_filter_strategy(self.BLOOM_THRESHOLD, self.SET_THRESHOLD)

        reader_for_filter = self.load_data(path, columns=[column], chunksize=self.DEFAULT_CHUNK_SIZE)
        return {"strategy": filter_strategy, "params": filter_params, "reader": reader_for_filter}

    def override_filter_strategy(self, column, filter_strategy, params=None):
        """Overrides the filter strategy for a specified column"""
        if filter_strategy not in self.filter_classes:
            raise ValueError(f"Invalid filter strategy '{filter_strategy}'")
        self.config[column] = {"strategy": filter_strategy, "params": params or {}}


class ParquetFilterGenerator(AbstractFilterGenerator):

    def get_files(self, root):
        return [file for file in os.listdir(root) if file.endswith('.parquet')]

    def load_data(self, path, columns=None, chunksize=None):
        # Create a generator to read chunks from the file
        if chunksize:
            return self.read_parquet_in_chunks(path, chunksize, columns)

        # Otherwise, return a DataFrame
        parquet_file = pq.ParquetFile(path)
        table = parquet_file.read(columns=columns)  # Read all data if no chunksize
        return table.to_pandas()

    @staticmethod
    def read_parquet_in_chunks(file_path, chunk_size=10000, columns=None):
        parquet_file = pq.ParquetFile(file_path)

        # Get the number of rows in the file
        num_row_groups = parquet_file.num_row_groups

        for i in range(num_row_groups):
            yield parquet_file.read_row_group(i, columns=columns).to_pandas()


class CSVFilterGenerator(AbstractFilterGenerator):

    def get_files(self, root):
        return [file for file in os.listdir(root) if file.endswith('.csv')]

    def load_data(self, path, columns=None, chunksize=None):
        # If chunksize is not None, return a generator
        if chunksize:
            return pd.read_csv(path, usecols=columns, chunksize=chunksize)

        # Otherwise, return a DataFrame
        try:
            reader = pd.read_csv(path, usecols=columns, chunksize=10)
            return next(reader)
        except StopIteration:
            return pd.DataFrame()
