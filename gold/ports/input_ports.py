from abc import ABC, abstractmethod
from typing import Tuple


class AggregateUseCase(ABC):
    @abstractmethod
    def execute(self) -> Tuple[str, str]:
        """
        Executes monthly and annual aggregations on clean Selic data.
        Returns a tuple of (monthly_metrics_path, annual_metrics_path).
        """
        pass
