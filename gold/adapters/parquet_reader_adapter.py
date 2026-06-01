import os
import pandas as pd
from gold.ports.output_ports import CleanDataReaderPort


class ParquetCleanReaderAdapter(CleanDataReaderPort):
    def __init__(self, file_path: str = "data/silver/selic_cleaned.parquet"):
        self.file_path = file_path

    def read_clean_data(self) -> pd.DataFrame:
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"Clean data file not found at: {self.file_path}")
        return pd.read_parquet(self.file_path)
