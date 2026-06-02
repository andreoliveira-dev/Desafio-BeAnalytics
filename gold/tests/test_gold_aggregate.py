import pytest
from datetime import date
import polars as pl
from unittest.mock import MagicMock
from gold.ports.output_ports import CleanDataReaderPort, MetricsWriterPort
from gold.services.aggregate_service import AggregateService


def test_aggregate_service_success():
    # Arrange
    mock_reader = MagicMock(spec=CleanDataReaderPort)
    mock_writer = MagicMock(spec=MetricsWriterPort)

    # Sample clean data for testing
    clean_df = pl.DataFrame([
        {"data": date(2020, 1, 2), "valor": 0.1},
        {"data": date(2020, 1, 3), "valor": 0.2},
        {"data": date(2020, 2, 1), "valor": 0.3},
        {"data": date(2020, 2, 2), "valor": 0.4},
        {"data": date(2021, 1, 2), "valor": 0.5}
    ])

    mock_reader.read_clean_data.return_value = clean_df.lazy()
    mock_writer.write_metrics.return_value = "data/gold/selic_metrics.parquet"

    service = AggregateService(reader=mock_reader, writer=mock_writer)

    # Act
    saved_path = service.execute()

    # Assert
    assert saved_path == "data/gold/selic_metrics.parquet"

    # Check what dataframe was passed to write_metrics
    called_metrics_df = mock_writer.write_metrics.call_args[0][0]

    # Verify metrics structure
    assert called_metrics_df.height == 3
    # 2020-01
    assert called_metrics_df["ano"][0] == 2020
    assert called_metrics_df["mes"][0] == 1
    assert called_metrics_df["media_mensal"][0] == pytest.approx(0.15)
    assert called_metrics_df["desvio_padrao_mensal"][0] == pytest.approx(0.07071, abs=1e-4)
    assert called_metrics_df["variacao_mensal"][0] == pytest.approx(0.0)
    expected_2020 = (1.001 * 1.002 * 1.003 * 1.004 - 1.0) * 100.0
    assert called_metrics_df["taxa_acumulada_anual"][0] == pytest.approx(expected_2020, abs=1e-6)

    # 2020-02
    assert called_metrics_df["ano"][1] == 2020
    assert called_metrics_df["mes"][1] == 2
    assert called_metrics_df["media_mensal"][1] == pytest.approx(0.35)
    assert called_metrics_df["variacao_mensal"][1] == pytest.approx(133.3333, abs=1e-4)
    assert called_metrics_df["taxa_acumulada_anual"][1] == pytest.approx(expected_2020, abs=1e-6)

    # 2021-01
    assert called_metrics_df["ano"][2] == 2021
    assert called_metrics_df["mes"][2] == 1
    assert called_metrics_df["media_mensal"][2] == pytest.approx(0.5)
    assert called_metrics_df["variacao_mensal"][2] == pytest.approx(42.8571, abs=1e-4)
    assert called_metrics_df["taxa_acumulada_anual"][2] == pytest.approx(0.5)


def test_aggregate_service_empty_input_raises_error():
    # Arrange
    mock_reader = MagicMock(spec=CleanDataReaderPort)
    mock_writer = MagicMock(spec=MetricsWriterPort)
    mock_reader.read_clean_data.return_value = pl.DataFrame([]).lazy()

    service = AggregateService(reader=mock_reader, writer=mock_writer)

    # Act & Assert
    with pytest.raises(ValueError) as excinfo:
        service.execute()
    assert "Clean data is empty" in str(excinfo.value)
