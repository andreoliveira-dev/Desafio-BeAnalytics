import os
from typing import List
import pandas as pd
from bronze.ports.output_ports import RawStoragePort
from bronze.domain.models import SelicRawRecord


class LocalParquetStorageAdapter(RawStoragePort):
    def __init__(self, output_dir: str = "data/bronze"):
        self.output_dir = output_dir

    def save_data(self, records: List[SelicRawRecord]) -> str:
        os.makedirs(self.output_dir, exist_ok=True)
        file_path = os.path.join(self.output_dir, "selic_raw.parquet")

        # Convert list of dataclasses to DataFrame
        data = [{"data": r.data, "valor": r.valor} for r in records]
        df = pd.DataFrame(data)

        # Save to Parquet
        df.to_parquet(file_path, index=False)
        return file_path
