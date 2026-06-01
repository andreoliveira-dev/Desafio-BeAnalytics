import time
import requests
from typing import List
from bronze.ports.output_ports import SelicSourcePort
from bronze.domain.models import SelicRawRecord


class CircuitBreakerOpenError(RuntimeError):
    pass


class BcbApiAdapter(SelicSourcePort):
    _failure_count = 0
    _last_failure_time = 0.0
    _state = "CLOSED"  # CLOSED, OPEN, HALF-OPEN

    FAILURE_THRESHOLD = 5
    COOLDOWN_PERIOD = 60.0  # seconds

    def __init__(
        self,
        base_url: str = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados",
        backoff_factor: float = 1.0,
        max_retries: int = 3
    ):
        self.base_url = base_url
        self.backoff_factor = backoff_factor
        self.max_retries = max_retries

    @classmethod
    def _reset_circuit_state_for_tests(cls):
        cls._failure_count = 0
        cls._last_failure_time = 0.0
        cls._state = "CLOSED"

    @classmethod
    def _check_circuit(cls):
        if cls._state == "OPEN":
            if time.time() - cls._last_failure_time > cls.COOLDOWN_PERIOD:
                cls._state = "HALF-OPEN"
            else:
                raise CircuitBreakerOpenError("Circuit is OPEN. Requests temporarily blocked.")

    @classmethod
    def _record_success(cls):
        cls._failure_count = 0
        cls._state = "CLOSED"

    @classmethod
    def _record_failure(cls):
        cls._failure_count += 1
        cls._last_failure_time = time.time()
        if cls._failure_count >= cls.FAILURE_THRESHOLD:
            cls._state = "OPEN"

    def fetch_data(self, start_date: str, end_date: str) -> List[SelicRawRecord]:
        self._check_circuit()

        params = {
            "formato": "json",
            "dataInicial": start_date,
            "dataFinal": end_date
        }

        last_exception = None
        for attempt in range(self.max_retries):
            try:
                response = requests.get(self.base_url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()

                self._record_success()

                return [
                    SelicRawRecord(data=item["data"], valor=item["valor"])
                    for item in data
                ]
            except Exception as e:
                last_exception = e
                if attempt < self.max_retries - 1:
                    sleep_time = self.backoff_factor * (2 ** attempt)
                    time.sleep(sleep_time)

        self._record_failure()
        raise RuntimeError(f"Error fetching data from BCB API: {str(last_exception)}") from last_exception
