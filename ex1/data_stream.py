from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._storage: list[tuple[int, str]] = []
        self._rank: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def add_storage(self, data: str) -> None:
        self._rank += 1
        self._storage.append((self._rank, data))

    def output(self) -> tuple[int, str]:
        return self._storage.pop(0)

    def data_status(self) -> tuple[int, int]:
        return (len(self._storage), self._rank)


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)) and not isinstance(data, bool):
            return True
        if isinstance(data, list):
            if not data:
                return False
            return all(isinstance(item, (int, float))
                       and not isinstance(item, bool)for item in data)
        return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        if not self.validate(data):
            raise ValueError("Incorrect numeric data")
        if isinstance(data, (int, float)):
            self.add_storage(str(data))
        else:
            for item in data:
                self.add_storage(str(item))


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            if not data:
                return False
            return all(isinstance(item, str) for item in data)
        return False

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise ValueError("Incorrect text data")
        if isinstance(data, str):
            self.add_storage(data)
        else:
            for item in data:
                self.add_storage(item)


class LogProcessor(DataProcessor):
    def _format_log(self, d: dict[str, str]) -> str:
        level = d["log_level"]
        msg = d["log_message"]
        return f"{level}: {msg}"

    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            return all(isinstance(k, str) and isinstance(v, str)
                       for k, v in data.items())

        if isinstance(data, list):
            if not data:
                return False
            return all(isinstance(d, dict) and all(
                isinstance(k, str) and isinstance(v, str)
                for k, v in d.items()
            )for d in data)
        return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if not self.validate(data):
            raise ValueError("Incorrect input data")

        if isinstance(data, dict):
            self.add_storage(self._format_log(data))
        else:
            for d in data:
                self.add_storage(self._format_log(d))


class DataStream():
    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []
        self._status: dict[DataProcessor, int] = {}

    def register_processor(self, proc: DataProcessor) -> None:
        if isinstance(proc, DataProcessor):
            self._processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for item in stream:
            for proc in self._processors:
                if proc.validate(item):
                    proc.ingest(item)
                else:
                    print("DataStream error - "
                          "Can't process element in stream: ", item)

    def print_processors_stats(self) -> None:
        if len(self._processors) == 0:
            print("No processor found, no data")
            return
        for proc in self._processors:
            len_store, rank = proc.data_status()
            print(f"{proc.__class__.__name__}: total {rank} items processed, "
                  f"remaining {len_store} on processor")


def data_stream() -> None:
    test = ['Hello world',
            [3.14, -1, 2.71],
            [{'log_level': 'WARNING', 'log_message': 'Telnet access!'},
             {'log_level': 'INFO', 'log_message': 'User wil isconnected'}],
            42,
            ['Hi', 'five']]
    print("=== Code Nexus - Data Stream ===", end="\n\n")

    numeric_test = NumericProcessor()
    data_s = DataStream()
    data_s.print_processors_stats()
    data_s.register_processor(numeric_test)
    data_s.process_stream(test)
    data_s.print_processors_stats()


if __name__ == "__main__":
    data_stream()
