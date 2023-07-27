from abc import abstractmethod, ABC
from datetime import datetime
import jellyfish
from bitarray import bitarray
from intervaltree import IntervalTree
from scipy.spatial import KDTree
from pybloom_live import BloomFilter as bf
import pandas as pd


class Filter(ABC):
    name = None

    @abstractmethod
    def test(self, value):
        pass


class BloomFilter(Filter):
    name = 'bloom'

    def __init__(self, filter_data):
        self.filter = filter_data['filter']
        self.data_type = filter_data.get('data_type', 'str')

    @staticmethod
    def get_valid_data(chunk, data_type):
        """Helper method to get valid data from chunk, ignoring None and casting to the specified type"""
        valid_data = chunk.dropna().unique().astype(data_type)
        return valid_data

    @classmethod
    def create(cls, reader=None, error_rate=0.1, data_type='str'):
        # First, calculate total length based on unique values from all chunks
        unique_data = set()
        for chunk in reader:
            unique_data.update(cls.get_valid_data(chunk.squeeze(), data_type))

        total_length = len(unique_data)

        # Instantiate a bloom filter with the calculated total length
        bloom = bf(capacity=total_length, error_rate=error_rate)

        # Now, add all unique values to the bloom filter
        for item in unique_data:
            bloom.add(item)

        data = {'filter': bloom, 'data_type': data_type}
        return cls(data)

    def update(self, chunk):
        for item in self.get_valid_data(chunk, self.data_type):
            self.filter.add(item)

    def test(self, value):
        return value in self.filter


class RangeFilter(Filter):
    name = 'range'

    def __init__(self, data):
        self.min = data['min']
        self.max = data['max']

    @classmethod
    def create(cls, reader=None):
        # Calculate global min and max from all chunks
        min_val = min(chunk.min() for chunk in reader)
        max_val = max(chunk.max() for chunk in reader)

        data = {'min': min_val, 'max': max_val}
        return cls(data)

    def update(self, chunk):
        min_val = chunk.min()
        max_val = chunk.max()
        self.min = min(min_val, self.min)
        self.max = max(max_val, self.max)

    def test(self, value):
        return self.min <= value <= self.max


class SetMembershipFilter(Filter):
    name = 'set_membership'

    def __init__(self, data):
        self.allowed_values = set(data['allowed_values'])

    @classmethod
    def create(cls, reader=None):
        # Store unique values from all chunks in a set
        allowed_values = set(item for chunk in reader for item in chunk.unique())
        data = {'allowed_values': allowed_values}
        return cls(data)

    def update(self, chunk):
        self.allowed_values.update(chunk.unique())

    def test(self, value):
        return value in self.allowed_values


class FuzzyStringFilter(SetMembershipFilter):
    name = 'fuzzy_string'

    @classmethod
    def create(cls, reader=None, min_similarity=0.8):
        # Get a representative sample of unique strings
        allowed_values = set(item for chunk in reader for item in chunk.unique())
        data = {'allowed_values': allowed_values, 'min_similarity': min_similarity}
        return cls(data)

    def test(self, value):
        # Check for fuzzy matches against the target
        for allowed_value in self.allowed_values:
            if jellyfish.jaro_similarity(self.target, allowed_value) >= self.min_similarity:
                return True
        return False


class DateFilter(Filter):
    name = 'date'

    @classmethod
    def create(cls, reader=None, date_format="%Y-%m-%d"):
        # Determine min and max dates from all chunks
        min_date = max_date = None
        for chunk in reader:
            # Drop missing values and check if the remaining values are already date objects
            non_null_chunk = chunk.dropna()
            if not non_null_chunk.empty and isinstance(non_null_chunk.iloc[0], datetime.date):
                dates = non_null_chunk
            else:
                dates = pd.to_datetime(non_null_chunk).dt.date
            if not dates.empty:
                if min_date is None:
                    min_date = dates.min()
                    max_date = dates.max()
                else:
                    min_date = min(min_date, dates.min())
                    max_date = max(max_date, dates.max())

        data = {'min': min_date.strftime(date_format), 'max': max_date.strftime(date_format),
                'date_format': date_format}
        return cls(data)

    def __init__(self, data):
        self.date_format = data['date_format']
        self.min = self._to_date(data['min'])
        self.max = self._to_date(data['max'])

    def test(self, value):
        value_date = self._to_date(value, self.date_format)
        return self.min <= value_date <= self.max

    @staticmethod
    def _to_date(value, date_format):
        if isinstance(value, (str)):
            return datetime.strptime(value, date_format).date()
        elif isinstance(value, (datetime.datetime, pd.Timestamp)):
            return value.date()
        elif isinstance(value, datetime.date):
            return value
        else:
            raise TypeError("Unsupported date type")


class IntervalTreeFilter(Filter):
    name = 'intervaltree'

    def __init__(self, data):
        self.tree = IntervalTree()
        for interval in data:
            self.tree.addi(*interval)

    @classmethod
    def create(cls, reader=None):
        data = []
        for chunk in reader:
            data.extend(chunk.values)
        return cls(data)

    def test(self, point):
        return bool(self.tree[point])


class KDTreeFilter(Filter):
    name = 'kdtree'

    def __init__(self, data, radius):
        self.radius = radius
        self.tree = KDTree(data)

    @classmethod
    def create(cls, reader=None):
        radius = 0  # Change this to a proper method to calculate radius
        data = []
        for chunk in reader:
            data.extend(chunk.values)
        return cls(data, radius)

    def test(self, point):
        distance, _ = self.tree.query([point])
        return distance <= self.radius


class BitVectorFilter(Filter):
    name = 'bitvector'

    def __init__(self, data):
        self.vector = bitarray(len(data))
        self.vector.setall(0)
        for bit in data:
            self.vector[bit] = 1

    @classmethod
    def create(cls, reader=None):
        data = []
        for chunk in reader:
            data.extend(chunk.values)
        return cls(data)

    def test(self, bit):
        return self.vector[bit]
