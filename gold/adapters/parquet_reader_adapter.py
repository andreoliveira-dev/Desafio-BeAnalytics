import os
import polars as pl
from gold.ports.output_ports import CleanDataReaderPort
from bronze.adapters.s3_storage_adapter import get_s3_storage_options


class ParquetCleanReaderAdapter(CleanDataReaderPort):
    def __init__(self, file_path: str = "data/silver/selic_cleaned.parquet"):
        self.file_path = file_path

    def read_clean_data(self) -> pl.LazyFrame:
        if self.file_path.startswith("s3://"):
            return pl.scan_parquet(self.file_path, storage_options=get_s3_storage_options())

        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"Clean data file not found at: {self.file_path}")
        return pl.scan_parquet(self.file_path)

