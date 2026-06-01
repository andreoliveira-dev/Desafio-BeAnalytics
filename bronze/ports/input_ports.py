from abc import ABC, abstractmethod


class IngestUseCase(ABC):
    @abstractmethod
    def execute(self, start_date: str, end_date: str) -> str:
        """
        Executes the ingestion of Selic data from external source and saves it.
        Returns the output path of the saved raw data.
        """
        pass
