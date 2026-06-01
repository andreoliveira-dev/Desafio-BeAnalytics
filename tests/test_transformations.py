import pytest
import logging
import pandas as pd
from unittest.mock import MagicMock
from silver.ports.output_ports import RawDataReaderPort, CleanDataWriterPort
from silver.services.transform_service import TransformService


def test_silver_transformation_success_and_types():
    # Arrange
    mock_reader = MagicMock(spec=RawDataReaderPort)
    mock_writer = MagicMock(spec=CleanDataWriterPort)

    raw_df = pd.DataFrame([
        {"data": "01/01/2020", "valor": "0.015"},
        {"data": "02/01/2020", "valor": "0.020"},
        {"data": "02/01/2020", "valor": "0.020"},  # Duplicate
    ])

    mock_reader.read_raw_data.return_value = raw_df
    mock_writer.write_clean_data.side_effect = lambda df: "data/silver/selic_cleaned.parquet"

    service = TransformService(reader=mock_reader, writer=mock_writer)

    # Act
    output_path = service.execute()

    # Assert
    assert output_path == "data/silver/selic_cleaned.parquet"
    mock_writer.write_clean_data.assert_called_once()

    called_df = mock_writer.write_clean_data.call_args[0][0]

    # Verify duplicates are dropped
    assert len(called_df) == 2

    # Verify column data types
    assert pd.api.types.is_datetime64_any_dtype(called_df["data"])
    assert pd.api.types.is_float_dtype(called_df["valor"])

    # Verify values are correctly converted
    assert called_df.iloc[0]["valor"] == 0.015
    assert called_df.iloc[1]["valor"] == 0.020


def test_silver_transformation_logs_warnings_for_out_of_bounds_rates(caplog):
    # Arrange
    mock_reader = MagicMock(spec=RawDataReaderPort)
    mock_writer = MagicMock(spec=CleanDataWriterPort)

    raw_df = pd.DataFrame([
        {"data": "01/01/2020", "valor": "0.015"},
        {"data": "02/01/2020", "valor": "-0.005"},  # Negative rate
        {"data": "03/01/2020", "valor": "1.05"},    # Rate > 1.0% per day
    ])

    mock_reader.read_raw_data.return_value = raw_df
    mock_writer.write_clean_data.side_effect = lambda df: "data/silver/selic_cleaned.parquet"

    service = TransformService(reader=mock_reader, writer=mock_writer)

    # Act
    with caplog.at_level(logging.WARNING):
        service.execute()

    # Assert
    # Verify warning log was emitted
    warnings = [rec.message for rec in caplog.records if rec.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "Data quality warning" in warnings[0]
    assert "-0.005" in warnings[0]
    assert "1.05" in warnings[0]


def test_silver_transformation_empty_bronze_raises_error():
    # Arrange
    mock_reader = MagicMock(spec=RawDataReaderPort)
    mock_writer = MagicMock(spec=CleanDataWriterPort)

    mock_reader.read_raw_data.return_value = pd.DataFrame([])

    service = TransformService(reader=mock_reader, writer=mock_writer)

    # Act & Assert
    with pytest.raises(ValueError) as excinfo:
        service.execute()
    assert "Raw data is empty" in str(excinfo.value)
