import os
from typing import Tuple
import pandas as pd
from gold.ports.output_ports import MetricsWriterPort


class ParquetMetricsWriterAdapter(MetricsWriterPort):
    def __init__(self, output_dir: str = "data/gold"):
        self.output_dir = output_dir

    def write_metrics(self, monthly_df: pd.DataFrame, annual_df: pd.DataFrame) -> Tuple[str, str]:
        os.makedirs(self.output_dir, exist_ok=True)
        monthly_path = os.path.join(self.output_dir, "selic_mensal.parquet")
        annual_path = os.path.join(self.output_dir, "selic_anual.parquet")

        monthly_df.to_parquet(monthly_path, index=False)
        annual_df.to_parquet(annual_path, index=False)

        return monthly_path, annual_path
