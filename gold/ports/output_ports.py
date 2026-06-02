from abc import ABC, abstractmethod
import polars as pl


class CleanDataReaderPort(ABC):
    @abstractmethod
    def read_clean_data(self) -> pl.LazyFrame:
        """
        Reads clean Selic data from Silver storage and returns a polars LazyFrame.
        """
        pass


class MetricsWriterPort(ABC):
    @abstractmethod
    def write_metrics(self, df: pl.DataFrame) -> str:
        """
        Writes aggregated metrics DataFrame to Gold storage.
        Returns the saved path.
        """
        pass

