import pytest
import polars as pl
import datetime
from unittest.mock import MagicMock
from silver.ports.output_ports import RawDataReaderPort, CleanDataWriterPort
from silver.services.transform_service import TransformService


def test_transform_service_success():
    # Arrange
    mock_reader = MagicMock(spec=RawDataReaderPort)
    mock_writer = MagicMock(spec=CleanDataWriterPort)

    # Raw data with some duplicates and nulls to clean
    raw_df = pl.DataFrame([
        {"data": "02/01/2020", "valor": "0.017089"},
        {"data": "02/01/2020", "valor": "0.017089"},  # Duplicate
        {"data": "03/01/2020", "valor": "0.017090"},
        {"data": None, "valor": "0.017090"},         # Null date
        {"data": "04/01/2020", "valor": None},         # Null valor
        {"data": "05/01/2020", "valor": "invalid_num"}  # Invalid valor
    ])

    mock_reader.read_raw_data.return_value = raw_df.lazy()
    mock_writer.write_clean_data.return_value = "data/silver/selic_clean.parquet"

    service = TransformService(reader=mock_reader, writer=mock_writer)

    # Act
    result = service.execute()

    # Assert
    assert result == "data/silver/selic_clean.parquet"
    mock_reader.read_raw_data.assert_called_once()

    # Get the dataframe passed to write_clean_data
    called_args = mock_writer.write_clean_data.call_args[0][0]

    # Should only contain 02/01/2020 and 03/01/2020
    assert called_args.height == 2
    assert called_args["data"][0] == datetime.date(2020, 1, 2)
    assert called_args["valor"][0] == pytest.approx(0.017089)
    assert called_args["data"][1] == datetime.date(2020, 1, 3)
    assert called_args["valor"][1] == pytest.approx(0.017090)


def test_transform_service_empty_input_raises_error():
    # Arrange
    mock_reader = MagicMock(spec=RawDataReaderPort)
    mock_writer = MagicMock(spec=CleanDataWriterPort)

    mock_reader.read_raw_data.return_value = pl.DataFrame([]).lazy()

    service = TransformService(reader=mock_reader, writer=mock_writer)

    # Act & Assert
    with pytest.raises(ValueError) as excinfo:
        service.execute()
    assert "Raw data is empty" in str(excinfo.value)


def test_transform_service_no_valid_data_left_raises_error():
    # Arrange
    mock_reader = MagicMock(spec=RawDataReaderPort)
    mock_writer = MagicMock(spec=CleanDataWriterPort)

    raw_df = pl.DataFrame([
        {"data": None, "valor": None},
        {"data": "02/01/2020", "valor": "not_a_number"}
    ])
    mock_reader.read_raw_data.return_value = raw_df.lazy()

    service = TransformService(reader=mock_reader, writer=mock_writer)

    # Act & Assert
    with pytest.raises(ValueError) as excinfo:
        service.execute()
    assert "No valid records left" in str(excinfo.value)

