import os
import pytest
import pandas as pd
from silver.adapters.parquet_reader_adapter import ParquetRawReaderAdapter
from silver.adapters.parquet_writer_adapter import ParquetCleanWriterAdapter


def test_parquet_raw_reader_adapter_missing_file():
    # Arrange
    adapter = ParquetRawReaderAdapter(file_path="non_existent_file.parquet")

    # Act & Assert
    with pytest.raises(FileNotFoundError):
        adapter.read_raw_data()


def test_parquet_clean_writer_and_reader_success(tmp_path):
    # Arrange
    temp_dir = tmp_path / "data" / "silver"
    writer = ParquetCleanWriterAdapter(output_dir=str(temp_dir))

    df = pd.DataFrame([
        {"data": "2020-01-02", "valor": 0.017089},
        {"data": "2020-01-03", "valor": 0.017090}
    ])

    # Act
    output_path = writer.write_clean_data(df)

    # Assert
    assert os.path.exists(output_path)

    # Verify reading using reader
    reader = ParquetRawReaderAdapter(file_path=output_path)
    read_df = reader.read_raw_data()

    assert len(read_df) == 2
    assert list(read_df.columns) == ["data", "valor"]
    assert read_df.iloc[0]["data"] == "2020-01-02"
    assert read_df.iloc[0]["valor"] == pytest.approx(0.017089)
