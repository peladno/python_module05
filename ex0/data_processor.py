from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self):
        self._storage: list[tuple[int, str]] = []
        self._rank: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        print()


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)) and not isinstance(data, bool):
            return True
        elif isinstance(data, list):
            return True
        else:
            return False

    def ingest(self, data: int | float | list[int | float]):
        if not self.validate(data):
            raise ValueError("Incorrect numeric data")


class TextProcessor(DataProcessor):
    def validate(self, data: list[str]):
        isinstance()

    def ingest(self, data: list[str]):
        if not self.validate(data):
            raise ValueError("Incorrect text data")


class LogProcessor(DataProcessor):
    def ingest(self, data: dict[str:str] | list[dict[str:str]]):
        if not self.validate(data):
            raise ValueError("Incorrect input data")
