import pytest
import logging
import polars as pl
from unittest.mock import MagicMock
from silver.ports.output_ports import RawDataReaderPort, CleanDataWriterPort
from silver.services.transform_service import TransformService


def test_silver_transformation_success_and_types():
    
    mock_reader = MagicMock(spec=RawDataReaderPort)
    mock_writer = MagicMock(spec=CleanDataWriterPort)

    raw_df = pl.DataFrame([
        {"data": "01/01/2020", "valor": "0.015"},
        {"data": "02/01/2020", "valor": "0.020"},
        {"data": "02/01/2020", "valor": "0.020"},  
    ])

    mock_reader.read_raw_data.return_value = raw_df.lazy()
    mock_writer.write_clean_data.side_effect = lambda df: "data/silver/selic_cleaned.parquet"

    service = TransformService(reader=mock_reader, writer=mock_writer)

    
    output_path = service.execute()

    
    assert output_path == "data/silver/selic_cleaned.parquet"
    mock_writer.write_clean_data.assert_called_once()

    called_df = mock_writer.write_clean_data.call_args[0][0]

    
    assert called_df.height == 2

    
    assert called_df.schema["data"] == pl.Date
    assert called_df.schema["valor"] == pl.Float64

    
    assert called_df["valor"][0] == 0.015
    assert called_df["valor"][1] == 0.020


def test_silver_transformation_logs_warnings_for_out_of_bounds_rates(caplog):
   
    mock_reader = MagicMock(spec=RawDataReaderPort)
    mock_writer = MagicMock(spec=CleanDataWriterPort)

    raw_df = pl.DataFrame([
        {"data": "01/01/2020", "valor": "0.015"},
        {"data": "02/01/2020", "valor": "-0.005"},
        {"data": "03/01/2020", "valor": "1.05"},
    ])

    mock_reader.read_raw_data.return_value = raw_df.lazy()
    mock_writer.write_clean_data.side_effect = lambda df: "data/silver/selic_cleaned.parquet"

    service = TransformService(reader=mock_reader, writer=mock_writer)

  
    with caplog.at_level(logging.WARNING):
        service.execute()

    
    warnings = [rec.message for rec in caplog.records if rec.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "Data quality warning" in warnings[0]
    assert "-0.005" in warnings[0]
    assert "1.05" in warnings[0]


def test_silver_transformation_empty_bronze_raises_error():
   
    mock_reader = MagicMock(spec=RawDataReaderPort)
    mock_writer = MagicMock(spec=CleanDataWriterPort)

    mock_reader.read_raw_data.return_value = pl.DataFrame([]).lazy()

    service = TransformService(reader=mock_reader, writer=mock_writer)

    
    with pytest.raises(ValueError) as excinfo:
        service.execute()
    assert "Raw data is empty" in str(excinfo.value)
