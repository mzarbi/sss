import warnings
warnings.filterwarnings("ignore")
# Instantiate the generator
from tqdm import trange

from core.metadata import ParquetFilterGenerator

generator = ParquetFilterGenerator(
    data_dir=r'C:\Users\medzi\Desktop\bnp\petals-framework\draft\data',  # Path to the directory with Parquet files
    store_name='store',  # Name of your data store
    filter_dir=r'C:\Users\medzi\Desktop\bnp\petals-framework\draft\stores'  # Where you want to save the filter objects
)

generator.generate_filters()