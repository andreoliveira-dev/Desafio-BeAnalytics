from abc import ABC, abstractmethod


class AggregateUseCase(ABC):
    @abstractmethod
    def execute(self) -> str:
        """
        Executes monthly and annual aggregations on clean Selic data.
        Returns the path where metrics were saved.
        """
        pass
