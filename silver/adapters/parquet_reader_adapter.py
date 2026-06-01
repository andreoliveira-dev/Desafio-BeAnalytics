import os
import pandas as pd
from silver.ports.output_ports import RawDataReaderPort


class ParquetRawReaderAdapter(RawDataReaderPort):
    def __init__(self, file_path: str = "data/bronze/selic_raw.parquet"):
        self.file_path = file_path

    def read_raw_data(self) -> pd.DataFrame:
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"Raw data file not found at: {self.file_path}")
        return pd.read_parquet(self.file_path)
