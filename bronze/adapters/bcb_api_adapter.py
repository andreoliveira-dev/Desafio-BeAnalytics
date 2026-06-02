import time
import requests
from typing import List
from bronze.ports.output_ports import SelicSourcePort, CircuitBreakerStatePort
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
        max_retries: int = 3,
        state_port: CircuitBreakerStatePort = None
    ):
        self.base_url = base_url
        self.backoff_factor = backoff_factor
        self.max_retries = max_retries
        self.state_port = state_port
        self.name = "bcb_api"

    @classmethod
    def _reset_circuit_state_for_tests(cls):
        cls._failure_count = 0
        cls._last_failure_time = 0.0
        cls._state = "CLOSED"

    def _check_circuit(self):
        if self.state_port:
            state_data = self.state_port.get_state(self.name)
            state = state_data["state"]
            last_failure_time = state_data["last_failure_time"]
        else:
            state = self._state
            last_failure_time = self._last_failure_time

        if state == "OPEN":
            if time.time() - last_failure_time > self.COOLDOWN_PERIOD:
                if self.state_port:
                    self.state_port.update_state(self.name, "HALF-OPEN", 0, 0.0)
                else:
                    self.__class__._state = "HALF-OPEN"
            else:
                raise CircuitBreakerOpenError("Circuit is OPEN. Requests temporarily blocked.")

    def _record_success(self):
        if self.state_port:
            self.state_port.update_state(self.name, "CLOSED", 0, 0.0)
        else:
            self.__class__._failure_count = 0
            self.__class__._state = "CLOSED"

    def _record_failure(self):
        if self.state_port:
            state_data = self.state_port.get_state(self.name)
            failures = state_data["failure_count"] + 1
            last_failure_time = time.time()
            state = "OPEN" if failures >= self.FAILURE_THRESHOLD else state_data["state"]
            self.state_port.update_state(self.name, state, failures, last_failure_time)
        else:
            self.__class__._failure_count += 1
            self.__class__._last_failure_time = time.time()
            if self.__class__._failure_count >= self.FAILURE_THRESHOLD:
                self.__class__._state = "OPEN"

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
