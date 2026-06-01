import pytest
from datetime import date
import pandas as pd
from unittest.mock import MagicMock
from gold.ports.output_ports import CleanDataReaderPort, MetricsWriterPort
from gold.services.aggregate_service import AggregateService


def test_aggregate_service_success():
    # Arrange
    mock_reader = MagicMock(spec=CleanDataReaderPort)
    mock_writer = MagicMock(spec=MetricsWriterPort)

    # Sample clean data for testing
    # 2020-01: Two days, rates 0.1% and 0.2%
    # 2020-02: Two days, rates 0.3% and 0.4%
    # 2021-01: One day, rate 0.5%
    clean_df = pd.DataFrame([
        {"data": date(2020, 1, 2), "valor": 0.1},
        {"data": date(2020, 1, 3), "valor": 0.2},
        {"data": date(2020, 2, 1), "valor": 0.3},
        {"data": date(2020, 2, 2), "valor": 0.4},
        {"data": date(2021, 1, 2), "valor": 0.5}
    ])

    mock_reader.read_clean_data.return_value = clean_df
    mock_writer.write_metrics.return_value = "data/gold/selic_metrics.parquet"

    service = AggregateService(reader=mock_reader, writer=mock_writer)

    # Act
    saved_path = service.execute()

    # Assert
    assert saved_path == "data/gold/selic_metrics.parquet"

    # Check what dataframe was passed to write_metrics
    called_metrics_df = mock_writer.write_metrics.call_args[0][0]

    # Verify metrics structure
    assert len(called_metrics_df) == 3
    # 2020-01
    assert called_metrics_df.iloc[0]["ano"] == 2020
    assert called_metrics_df.iloc[0]["mes"] == 1
    assert called_metrics_df.iloc[0]["media_mensal"] == pytest.approx(0.15)
    # desvio padrao of 0.1 and 0.2 is 0.07071
    assert called_metrics_df.iloc[0]["desvio_padrao_mensal"] == pytest.approx(0.07071, abs=1e-4)
    assert called_metrics_df.iloc[0]["variacao_mensal"] == pytest.approx(0.0)
    expected_2020 = (1.001 * 1.002 * 1.003 * 1.004 - 1.0) * 100.0
    assert called_metrics_df.iloc[0]["taxa_acumulada_anual"] == pytest.approx(expected_2020, abs=1e-6)

    # 2020-02
    assert called_metrics_df.iloc[1]["ano"] == 2020
    assert called_metrics_df.iloc[1]["mes"] == 2
    assert called_metrics_df.iloc[1]["media_mensal"] == pytest.approx(0.35)
    # variation from 0.15 to 0.35 is (0.35 - 0.15)/0.15 * 100 = 133.333% percentage
    assert called_metrics_df.iloc[1]["variacao_mensal"] == pytest.approx(133.3333, abs=1e-4)
    assert called_metrics_df.iloc[1]["taxa_acumulada_anual"] == pytest.approx(expected_2020, abs=1e-6)

    # 2021-01
    assert called_metrics_df.iloc[2]["ano"] == 2021
    assert called_metrics_df.iloc[2]["mes"] == 1
    assert called_metrics_df.iloc[2]["media_mensal"] == pytest.approx(0.5)
    # Wait, in the merged df, variacao_mensal is calculated chronologically across all rows.
    # Grouped sort has order: 2020-01, 2020-02, 2021-01.
    # So 2021-01 is compared to 2020-02!
    # Let's check: media_mensal of 2020-02 is 0.35, 2021-01 is 0.5.
    # Pct change from 0.35 to 0.5 is (0.5 - 0.35)/0.35 * 100.0 = 42.8571% !
    # Ah! Let's verify if variacao_mensal calculates across years chronologically.
    # Yes, `grouped = grouped.sort_values(by=["ano", "mes"]).reset_index(drop=True)` followed by `.pct_change()`.
    # So 2021-01 row is index 2, and will have variation = (0.5 - 0.35)/0.35 * 100.0 = 42.8571%.
    # Let's assert:
    assert called_metrics_df.iloc[2]["variacao_mensal"] == pytest.approx(42.8571, abs=1e-4)
    assert called_metrics_df.iloc[2]["taxa_acumulada_anual"] == pytest.approx(0.5)


def test_aggregate_service_empty_input_raises_error():
    # Arrange
    mock_reader = MagicMock(spec=CleanDataReaderPort)
    mock_writer = MagicMock(spec=MetricsWriterPort)
    mock_reader.read_clean_data.return_value = pd.DataFrame([])

    service = AggregateService(reader=mock_reader, writer=mock_writer)

    # Act & Assert
    with pytest.raises(ValueError) as excinfo:
        service.execute()
    assert "Clean data is empty" in str(excinfo.value)
