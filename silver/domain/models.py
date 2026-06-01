from datetime import date
from dataclasses import dataclass


@dataclass
class SelicCleanedRecord:
    data: date
    valor: float
