import os
import pandas as pd
from silver.ports.output_ports import CleanDataWriterPort


class ParquetCleanWriterAdapter(CleanDataWriterPort):
    def __init__(self, output_dir: str = "data/silver"):
        self.output_dir = output_dir

    def write_clean_data(self, df: pd.DataFrame) -> str:
        os.makedirs(self.output_dir, exist_ok=True)
        file_path = os.path.join(self.output_dir, "selic_clean.parquet")
        df.to_parquet(file_path, index=False)
        return file_path
