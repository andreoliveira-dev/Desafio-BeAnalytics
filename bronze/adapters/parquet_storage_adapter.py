import os
from typing import List
import polars as pl
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

        data = [{"data": r.data, "valor": r.valor} for r in records]
        df = pl.DataFrame(data)

        df.write_parquet(self.file_path)

        import json
        json_path = self.file_path.replace(".parquet", ".json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        return self.file_path
