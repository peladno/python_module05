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
            if not data:
                return False
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


def data_processor() -> None:

    print("== Code Nexus - Data processor")
    print()

    print("Testing numeric processor")
    numeric = NumericProcessor()

    test1 = 42
    print(f" Trying to validate '{42}': {numeric.validate(test1)}")
    test2 = "Hola"
    print(f" Trying to validate input '{test2}': {numeric.validate(test2)}")
    test3 = True
    print(f" Trying to validate input '{test3}': {numeric.validate(test3)}")

    nums = [3, 4, 9, 4.3, 10]
    for n in nums:
        numeric.ingest(n)
    print(" Processing data: ", nums)
    print("Extracting 3 values")

    for _ in range(3):
        rank, value = numeric.output()
        print(f" Extracting value {rank}: {value}")

    print()
    print("== Continuing tests ==")

    print("Testing numeric processor (valid & invalid ingest)")
    numeric2 = NumericProcessor()

    numeric2.ingest(100)
    numeric2.ingest([1, 2, 3])

    try:
        numeric2.ingest("hola")
    except ValueError as e:
        print(" Caught expected exception in numeric ingest:", e)

    print(" Extracting numeric data:")
    while numeric2._storage:
        rank, value = numeric2.output()
        print(f"  Output -> rank {rank}, value {value}")

    print()
    print("Testing text processor")
    text = TextProcessor()

    test4 = ["hola", "como", 1]
    print(f" Trying to validate input '{test4}': {text.validate(test4)}")
    test5 = ["hola", "como", "estas?"]
    print(f" Trying to validate input '{test5}': {text.validate(test5)}")

    print()
    print("Testing text processor (valid & invalid ingest)")
    text2 = TextProcessor()

    text2.ingest("hola")
    text2.ingest(["uno", "dos"])

    try:
        text2.ingest([1, 2, 3])
    except ValueError as e:
        print(" Caught expected exception in text ingest:", e)

    print(" Extracting text data:")
    while text2._storage:
        rank, value = text2.output()
        print(f"  Output -> rank {rank}, value {value}")

    print()
    print("Testing log processor")
    logs = LogProcessor()

    test6 = {1: "Error"}
    print(f" Trying to validate input '{test6}':", logs.validate(test6))
    test7 = {"ERROR": "Error in the matrix"}
    print(f" Trying to validate input '{test7}':", logs.validate(test7))

    print()
    print("Testing log processor (valid & invalid ingest)")
    logs2 = LogProcessor()

    # Valid ingest
    logs2.ingest({"log_level": "INFO", "log_message": "Todo OK"})
    logs2.ingest([
        {"log_level": "WARN", "log_message": "Cuidado"},
        {"log_level": "ERROR", "log_message": "Boom"},
    ])

    try:
        logs2.ingest({"log_level": 123, "log_message": "Nope"})
    except ValueError as e:
        print(" Caught expected exception in log ingest:", e)

    print(" Extracting log data:")
    while logs2._storage:
        rank, value = logs2.output()
        print(f"  Output -> rank {rank}, value {value}")


if __name__ == "__main__":
    data_processor()
