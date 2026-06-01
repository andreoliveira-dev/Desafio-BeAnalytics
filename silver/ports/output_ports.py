from abc import ABC, abstractmethod
import pandas as pd


class RawDataReaderPort(ABC):
    @abstractmethod
    def read_raw_data(self) -> pd.DataFrame:
        """
        Reads the raw data from Bronze storage and returns a pandas DataFrame.
        """
        pass


class CleanDataWriterPort(ABC):
    @abstractmethod
    def write_clean_data(self, df: pd.DataFrame) -> str:
        """
        Writes the clean DataFrame to Silver storage.
        Returns the saved file path.
        """
        pass
