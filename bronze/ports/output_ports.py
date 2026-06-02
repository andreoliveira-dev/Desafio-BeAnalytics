from abc import ABC, abstractmethod
from typing import List
from bronze.domain.models import SelicRawRecord


class SelicSourcePort(ABC):
    @abstractmethod
    def fetch_data(self, start_date: str, end_date: str) -> List[SelicRawRecord]:
        """
        Fetches raw Selic data from the source API.
        """
        pass


class RawStoragePort(ABC):
    @abstractmethod
    def save_data(self, records: List[SelicRawRecord]) -> str:
        """
        Saves raw records to Parquet file format.
        Returns the path where the data was saved.
        """
        pass


class CircuitBreakerStatePort(ABC):
    @abstractmethod
    def get_state(self, name: str) -> dict:
        """
        Retrieves the state for a given circuit breaker name.
        Expected return format: {"state": str, "failure_count": int, "last_failure_time": float}
        """
        pass

    @abstractmethod
    def update_state(self, name: str, state: str, failure_count: int, last_failure_time: float) -> None:
        """
        Updates the state for a given circuit breaker name.
        """
        pass
