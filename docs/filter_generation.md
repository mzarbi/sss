# Filter Generator Tool Documentation
The Filter Generator tool is a powerful utility for generating filters on datasets stored in Parquet or CSV format. It allows you to create and apply various filter strategies based on the characteristics of your data. It also provides flexibility by allowing you to override default filter strategies for specific columns.

## Installation
Filter Generator is a Python module and can be integrated into any Python environment.

### Basic Usage
```python
from core.metadata import ParquetFilterGenerator

# Instantiate the generator
generator = ParquetFilterGenerator(
    data_dir='path/to/your/data',  # Path to the directory with Parquet files
    store_name='my_store',  # Name of your data store
    filter_dir='path/to/save/filters'  # Where you want to save the filter objects
)
```
### Generate filters
```python
generator.generate_filters()
```
This will iterate over all Parquet files in data_dir, create a filter for each column in each file, and save these filters as pickle files in filter_dir. It will automatically choose a filter strategy based on the FilterSelector.select_filter_strategy method.

Overriding Filter Strategy for Specific Columns
If you want to override the filter strategy for a particular column, you can do so before generating the filters:

```python
# Override filter strategy for column 'my_column'
generator.override_filter_strategy('my_column', 'custom_strategy')

# Now generate filters
generator.generate_filters()
```
This code will use the 'custom_strategy' for the column 'my_column' instead of selecting one automatically. Ensure that 'custom_strategy' is a valid filter strategy and that a corresponding filter class has been defined.

Loading Configuration from a JSON File
```python
# Instantiate the generator with a config file
generator = ParquetFilterGenerator(
    data_dir='path/to/your/data',
    store_name='my_store',
    filter_dir='path/to/save/filters',
    config_file='path/to/config.json'
)

# Generate filters

generator.generate_filters()
```
The configuration file should be a JSON file where each key is a column name and the value is an object with the filter strategy and optional parameters for the strategy.

```json
{
  "column1": {
    "strategy": "custom_strategy",
    "params": {
      "param1": "value1",
      "param2": "value2"
    }
  },
  "column2": {
    "strategy": "another_strategy"
  }
}
```
This way, the generator will use the specified strategies and parameters for the given columns when generating filters.

Selecting Specific Columns to Generate Filters
You can specify a list of column names to only generate filters for those columns:

python
Copy code
### Instantiate the generator with specific columns
```python
generator = ParquetFilterGenerator(
    data_dir='path/to/your/data',
    store_name='my_store',
    filter_dir='path/to/save/filters',
    included_columns=['column1', 'column2']
)
# Generate filters
generator.generate_filters()
```
#### Working with CSV Files
You can use the CSVFilterGenerator in the same way as the ParquetFilterGenerator:

```python
from core.metadata import CSVFilterGenerator

# Instantiate the generator for CSV files
generator = CSVFilterGenerator(
    data_dir='path/to/your/data',
    store_name='my_store',
    filter_dir='path/to/save/filters'
)

# Generate filters
generator.generate_filters()
```
### Extending the Filter Generator
The Filter Generator tool is designed to be extensible. You can create new filter classes and strategies by extending the base Filter class and adding your custom filter class to the filter classes dictionary. Make sure your filter class implements a create class method to instantiate the filter from a Pandas DataFrame.