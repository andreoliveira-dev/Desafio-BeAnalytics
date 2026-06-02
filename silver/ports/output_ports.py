from abc import ABC, abstractmethod
import polars as pl


class RawDataReaderPort(ABC):
    @abstractmethod
    def read_raw_data(self) -> pl.LazyFrame:
        """
        Reads the raw data from Bronze storage and returns a polars LazyFrame.
        """
        pass


class CleanDataWriterPort(ABC):
    @abstractmethod
    def write_clean_data(self, df: pl.DataFrame) -> str:
        """
        Writes the clean DataFrame to Silver storage.
        Returns the saved file path.
        """
        pass

