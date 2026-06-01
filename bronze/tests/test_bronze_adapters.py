import os
import pytest
from unittest.mock import patch, MagicMock
from bronze.adapters.bcb_api_adapter import BcbApiAdapter
from bronze.adapters.parquet_storage_adapter import LocalParquetStorageAdapter
from bronze.domain.models import SelicRawRecord
import pandas as pd


def test_bcb_api_adapter_fetch_success():
    # Arrange
    adapter = BcbApiAdapter()
    mock_response = MagicMock()
    mock_response.json.return_value = [
        {"data": "02/01/2020", "valor": "0.017089"},
        {"data": "03/01/2020", "valor": "0.017089"}
    ]
    mock_response.raise_for_status = MagicMock()

    with patch("requests.get", return_value=mock_response) as mock_get:
        # Act
        result = adapter.fetch_data("01/01/2020", "31/12/2024")

        # Assert
        assert len(result) == 2
        assert result[0] == SelicRawRecord(data="02/01/2020", valor="0.017089")
        assert result[1] == SelicRawRecord(data="03/01/2020", valor="0.017089")
        mock_get.assert_called_once_with(
            "https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados",
            params={"formato": "json", "dataInicial": "01/01/2020", "dataFinal": "31/12/2024"},
            timeout=30
        )


def test_bcb_api_adapter_fetch_failure():
    # Arrange
    adapter = BcbApiAdapter()

    with patch("requests.get", side_effect=Exception("Timeout")):
        # Act & Assert
        with pytest.raises(RuntimeError) as excinfo:
            adapter.fetch_data("01/01/2020", "31/12/2024")

        assert "Error fetching data from BCB API" in str(excinfo.value)


def test_local_parquet_storage_adapter_save_success(tmp_path):
    # Arrange
    temp_dir = tmp_path / "data" / "bronze"
    adapter = LocalParquetStorageAdapter(output_dir=str(temp_dir))

    records = [
        SelicRawRecord(data="02/01/2020", valor="0.017089"),
        SelicRawRecord(data="03/01/2020", valor="0.017089")
    ]

    # Act
    output_path = adapter.save_data(records)

    # Assert
    assert os.path.exists(output_path)
    df = pd.read_parquet(output_path)
    assert len(df) == 2
    assert list(df.columns) == ["data", "valor"]
    assert df.iloc[0]["data"] == "02/01/2020"
    assert df.iloc[0]["valor"] == "0.017089"
