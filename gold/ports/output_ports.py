from abc import ABC, abstractmethod
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
    def write_metrics(self, df: pd.DataFrame) -> str:
        """
        Writes aggregated metrics DataFrame to Gold storage.
        Returns the saved path.
        """
        pass
