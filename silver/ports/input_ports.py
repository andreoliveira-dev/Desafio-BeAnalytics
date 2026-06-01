from abc import ABC, abstractmethod


class TransformUseCase(ABC):
    @abstractmethod
    def execute(self) -> str:
        """
        Executes the cleaning and transformation process on raw Selic data.
        Returns the output path of the cleaned data.
        """
        pass
