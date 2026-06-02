import pytest
from unittest.mock import MagicMock
from bronze.domain.models import SelicRawRecord
from bronze.ports.output_ports import SelicSourcePort, RawStoragePort
from bronze.services.ingest_service import IngestService


def test_ingest_service_success():

    mock_source = MagicMock(spec=SelicSourcePort)
    mock_storage = MagicMock(spec=RawStoragePort)

    mock_records = [
        SelicRawRecord(data="02/01/2020", valor="0.017089"),
        SelicRawRecord(data="03/01/2020", valor="0.017089")
    ]
    mock_source.fetch_data.return_value = mock_records
    mock_storage.save_data.return_value = "data/bronze/selic_raw.parquet"

    service = IngestService(source=mock_source, storage=mock_storage)

    result = service.execute(start_date="01/01/2020", end_date="31/12/2024")

    assert result == "data/bronze/selic_raw.parquet"
    mock_source.fetch_data.assert_called_once_with("01/01/2020", "31/12/2024")
    mock_storage.save_data.assert_called_once_with(mock_records)


def test_ingest_service_empty_data_raises_error():

    mock_source = MagicMock(spec=SelicSourcePort)
    mock_storage = MagicMock(spec=RawStoragePort)

    mock_source.fetch_data.return_value = []

    service = IngestService(source=mock_source, storage=mock_storage)

    with pytest.raises(ValueError) as excinfo:
        service.execute(start_date="01/01/2020", end_date="31/12/2024")

    assert "No records fetched" in str(excinfo.value)
    mock_storage.save_data.assert_not_called()


def test_ingest_service_source_error_propagates():

    mock_source = MagicMock(spec=SelicSourcePort)
    mock_storage = MagicMock(spec=RawStoragePort)

    mock_source.fetch_data.side_effect = RuntimeError("API connection failure")

    service = IngestService(source=mock_source, storage=mock_storage)

    with pytest.raises(RuntimeError) as excinfo:
        service.execute(start_date="01/01/2020", end_date="31/12/2024")

    assert "API connection failure" in str(excinfo.value)
    mock_storage.save_data.assert_not_called()
