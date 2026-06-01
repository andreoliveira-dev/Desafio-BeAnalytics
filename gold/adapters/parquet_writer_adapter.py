import os
import pandas as pd
from gold.ports.output_ports import MetricsWriterPort


class ParquetMetricsWriterAdapter(MetricsWriterPort):
    def __init__(self, file_path: str = None, output_dir: str = "data/gold"):
        if file_path is not None:
            self.file_path = file_path
        else:
            self.file_path = os.path.join(output_dir, "selic_metrics.parquet")

    def write_metrics(self, df: pd.DataFrame) -> str:
        output_dir = os.path.dirname(self.file_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

        df.to_parquet(self.file_path, index=False)
        return self.file_path
