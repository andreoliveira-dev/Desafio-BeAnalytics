import os
from typing import List
import pandas as pd
from bronze.ports.output_ports import RawStoragePort
from bronze.domain.models import SelicRawRecord


class LocalParquetStorageAdapter(RawStoragePort):
    def __init__(self, file_path: str = None, output_dir: str = "data/bronze"):
        if file_path is not None:
            self.file_path = file_path
        else:
            self.file_path = os.path.join(output_dir, "selic_raw.parquet")

    def save_data(self, records: List[SelicRawRecord]) -> str:
        output_dir = os.path.dirname(self.file_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

        # Convert list of dataclasses to DataFrame
        data = [{"data": r.data, "valor": r.valor} for r in records]
        df = pd.DataFrame(data)

        # Save to Parquet
        df.to_parquet(self.file_path, index=False)
        return self.file_path
