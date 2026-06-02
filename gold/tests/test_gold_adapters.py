import os
import pytest
import polars as pl
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

    metrics_df = pl.DataFrame([
        {"ano": 2020, "mes": 1, "media_mensal": 0.15, "desvio_padrao_mensal": 0.05,
         "variacao_mensal": 0.0, "taxa_acumulada_anual": 4.5}
    ])

    # Act
    saved_path = writer.write_metrics(metrics_df)

    # Assert
    assert os.path.exists(saved_path)

    # Verify reading using reader
    reader = ParquetCleanReaderAdapter(file_path=saved_path)
    read_metrics_df = reader.read_clean_data().collect()
    assert read_metrics_df.height == 1
    assert read_metrics_df["media_mensal"][0] == pytest.approx(0.15)
