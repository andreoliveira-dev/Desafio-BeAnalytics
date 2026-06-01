import os
import pytest
import pandas as pd
from gold.adapters.parquet_reader_adapter import ParquetCleanReaderAdapter
from gold.adapters.parquet_writer_adapter import ParquetMetricsWriterAdapter


def test_parquet_clean_reader_adapter_missing_file():
    # Arrange
    adapter = ParquetCleanReaderAdapter(file_path="non_existent_file.parquet")

    # Act & Assert
    with pytest.raises(FileNotFoundError):
        adapter.read_clean_data()


def test_parquet_metrics_writer_and_reader_success(tmp_path):
    # Arrange
    temp_dir = tmp_path / "data" / "gold"
    writer = ParquetMetricsWriterAdapter(output_dir=str(temp_dir))

    monthly_df = pd.DataFrame([
        {"ano": 2020, "mes": 1, "media_mensal": 0.15, "desvio_padrao_mensal": 0.05,
            "variacao_absoluta_mensal": 0.0, "variacao_percentual_mensal": 0.0}
    ])

    annual_df = pd.DataFrame([
        {"ano": 2020, "taxa_acumulada_anual": 4.5}
    ])

    # Act
    monthly_path, annual_path = writer.write_metrics(monthly_df, annual_df)

    # Assert
    assert os.path.exists(monthly_path)
    assert os.path.exists(annual_path)

    # Verify reading using reader
    reader = ParquetCleanReaderAdapter(file_path=monthly_path)
    read_monthly_df = reader.read_clean_data()
    assert len(read_monthly_df) == 1
    assert read_monthly_df.iloc[0]["media_mensal"] == pytest.approx(0.15)
