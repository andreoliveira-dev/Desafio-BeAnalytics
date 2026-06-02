import os
import polars as pl
from silver.ports.output_ports import RawDataReaderPort
from bronze.adapters.s3_storage_adapter import get_s3_storage_options


class ParquetRawReaderAdapter(RawDataReaderPort):
    def __init__(self, file_path: str = "data/bronze/selic_raw.parquet"):
        self.file_path = file_path

    def read_raw_data(self) -> pl.LazyFrame:
        if self.file_path.startswith("s3://"):
            return pl.scan_parquet(self.file_path, storage_options=get_s3_storage_options())

        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"Raw data file not found at: {self.file_path}")
        return pl.scan_parquet(self.file_path)
