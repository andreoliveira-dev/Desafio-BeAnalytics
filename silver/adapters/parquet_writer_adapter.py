import os
import pandas as pd
from silver.ports.output_ports import CleanDataWriterPort


class ParquetCleanWriterAdapter(CleanDataWriterPort):
    def __init__(self, file_path: str = None, output_dir: str = "data/silver"):
        if file_path is not None:
            self.file_path = file_path
        else:
            self.file_path = os.path.join(output_dir, "selic_clean.parquet")

    def write_clean_data(self, df: pd.DataFrame) -> str:
        output_dir = os.path.dirname(self.file_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
        df.to_parquet(self.file_path, index=False)
        return self.file_path
