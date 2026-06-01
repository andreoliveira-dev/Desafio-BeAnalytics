from abc import ABC, abstractmethod
from typing import Tuple
import pandas as pd


class CleanDataReaderPort(ABC):
    @abstractmethod
    def read_clean_data(self) -> pd.DataFrame:
        """
        Reads clean Selic data from Silver storage.
        """
        pass


class MetricsWriterPort(ABC):
    @abstractmethod
    def write_metrics(self, monthly_df: pd.DataFrame, annual_df: pd.DataFrame) -> Tuple[str, str]:
        """
        Writes aggregated monthly and annual DataFrames to Gold storage.
        Returns a tuple of (monthly_saved_path, annual_saved_path).
        """
        pass
