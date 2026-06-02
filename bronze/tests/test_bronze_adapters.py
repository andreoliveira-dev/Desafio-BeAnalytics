import os
import pytest
from unittest.mock import patch, MagicMock
from bronze.adapters.bcb_api_adapter import BcbApiAdapter
from bronze.adapters.parquet_storage_adapter import LocalParquetStorageAdapter
from bronze.domain.models import SelicRawRecord
import pandas as pd


def test_bcb_api_adapter_fetch_success():

    adapter = BcbApiAdapter()
    mock_response = MagicMock()
    mock_response.json.return_value = [
        {"data": "02/01/2020", "valor": "0.017089"},
        {"data": "03/01/2020", "valor": "0.017089"}
    ]
    mock_response.raise_for_status = MagicMock()

    with patch("requests.get", return_value=mock_response) as mock_get:

        result = adapter.fetch_data("01/01/2020", "31/12/2024")

        assert len(result) == 2
        assert result[0] == SelicRawRecord(data="02/01/2020", valor="0.017089")
        assert result[1] == SelicRawRecord(data="03/01/2020", valor="0.017089")
        mock_get.assert_called_once_with(
            "https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados",
            params={"formato": "json", "dataInicial": "01/01/2020", "dataFinal": "31/12/2024"},
            timeout=30
        )


def test_bcb_api_adapter_fetch_failure():

    adapter = BcbApiAdapter(backoff_factor=0.0)

    with patch("requests.get", side_effect=Exception("Timeout")):

        with pytest.raises(RuntimeError) as excinfo:
            adapter.fetch_data("01/01/2020", "31/12/2024")

        assert "Error fetching data from BCB API" in str(excinfo.value)


def test_bcb_api_adapter_exponential_backoff():

    adapter = BcbApiAdapter(backoff_factor=0.001, max_retries=3)

    mock_success = MagicMock()
    mock_success.json.return_value = [{"data": "02/01/2020", "valor": "0.01"}]
    mock_success.raise_for_status = MagicMock()

    with patch("requests.get", side_effect=[Exception("API Error"), Exception("API Error"), mock_success]) as mock_get:
        result = adapter.fetch_data("01/01/2020", "31/12/2024")
        assert len(result) == 1
        assert mock_get.call_count == 3


def test_bcb_api_adapter_circuit_breaker():

    BcbApiAdapter._reset_circuit_state_for_tests()
    adapter = BcbApiAdapter(backoff_factor=0.0, max_retries=1)

    from bronze.adapters.bcb_api_adapter import CircuitBreakerOpenError

    with patch("requests.get", side_effect=Exception("API Error")):
        for _ in range(5):
            with pytest.raises(RuntimeError):
                adapter.fetch_data("01/01/2020", "31/12/2024")

    with patch("requests.get") as mock_get:
        with pytest.raises(CircuitBreakerOpenError):
            adapter.fetch_data("01/01/2020", "31/12/2024")
        mock_get.assert_not_called()
    BcbApiAdapter._reset_circuit_state_for_tests()


def test_local_parquet_storage_adapter_save_success(tmp_path):

    temp_dir = tmp_path / "data" / "bronze"
    adapter = LocalParquetStorageAdapter(output_dir=str(temp_dir))

    records = [
        SelicRawRecord(data="02/01/2020", valor="0.017089"),
        SelicRawRecord(data="03/01/2020", valor="0.017089")
    ]

    output_path = adapter.save_data(records)

    assert os.path.exists(output_path)
    df = pd.read_parquet(output_path)
    assert len(df) == 2
    assert list(df.columns) == ["data", "valor"]
    assert df.iloc[0]["data"] == "02/01/2020"
    assert df.iloc[0]["valor"] == "0.017089"


def test_sql_circuit_breaker_state_adapter():
    from bronze.adapters.circuit_breaker_state_adapter import SqlCircuitBreakerStateAdapter

    adapter = SqlCircuitBreakerStateAdapter(connection_string="sqlite:///:memory:")

    state = adapter.get_state("test_breaker")
    assert state == {"state": "CLOSED", "failure_count": 0, "last_failure_time": 0.0}

    adapter.update_state("test_breaker", "OPEN", 5, 123.45)
    state = adapter.get_state("test_breaker")
    assert state == {"state": "OPEN", "failure_count": 5, "last_failure_time": 123.45}


def test_bcb_api_adapter_circuit_breaker_with_sql_adapter():
    from bronze.adapters.circuit_breaker_state_adapter import SqlCircuitBreakerStateAdapter
    from bronze.adapters.bcb_api_adapter import CircuitBreakerOpenError

    state_adapter = SqlCircuitBreakerStateAdapter(connection_string="sqlite:///:memory:")
    api_adapter = BcbApiAdapter(backoff_factor=0.0, max_retries=1, state_port=state_adapter)

    with patch("requests.get", side_effect=Exception("API Error")):
        for _ in range(5):
            with pytest.raises(RuntimeError):
                api_adapter.fetch_data("01/01/2020", "31/12/2024")

    assert state_adapter.get_state("bcb_api")["state"] == "OPEN"
    assert state_adapter.get_state("bcb_api")["failure_count"] == 5

    with patch("requests.get") as mock_get:
        with pytest.raises(CircuitBreakerOpenError):
            api_adapter.fetch_data("01/01/2020", "31/12/2024")
        mock_get.assert_not_called()
