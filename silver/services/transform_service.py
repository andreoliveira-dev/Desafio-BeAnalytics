import logging
import polars as pl
from silver.ports.input_ports import TransformUseCase
from silver.ports.output_ports import RawDataReaderPort, CleanDataWriterPort


class TransformService(TransformUseCase):
    def __init__(self, reader: RawDataReaderPort, writer: CleanDataWriterPort):
        self.reader = reader
        self.writer = writer

    def execute(self) -> str:
        # 1. Read raw LazyFrame
        df_lazy = self.reader.read_raw_data()

        # 2. Check if it's empty (Bronze data check)
        try:
            # Check if there is at least one record using a fast limit(1) scan
            if df_lazy.limit(1).collect().height == 0:
                raise ValueError("Data quality check failed: Raw data is empty.")
        except Exception as e:
            if isinstance(e, ValueError) and "Data quality check failed" in str(e):
                raise
            raise ValueError(f"Data quality check failed: Raw data is empty or corrupted: {str(e)}") from e

        # 3. Clean and standardize lazily
        # Drop rows where critical fields are null
        df_lazy = df_lazy.filter(pl.col("data").is_not_null() & pl.col("valor").is_not_null())

        # Convert 'data' column from dd/MM/yyyy to Date type, and 'valor' to Float64
        # We handle string parsing for dates
        df_lazy = df_lazy.with_columns([
            pl.col("data").str.strptime(pl.Date, format="%d/%m/%Y"),
            pl.col("valor").cast(pl.Float64, strict=False)
        ])

        # Drop rows where 'valor' conversion failed (resulting in null)
        df_lazy = df_lazy.filter(pl.col("valor").is_not_null() & pl.col("data").is_not_null())

        # Handle duplicates on 'data'
        df_lazy = df_lazy.unique(subset=["data"])

        # Sort chronologically by date
        df_lazy = df_lazy.sort("data")

        # 4. Collect using streaming=True to process memory-efficiently
        try:
            df = df_lazy.collect(streaming=True)
        except Exception as e:
            raise ValueError(f"Data quality check failed during transformation collect: {str(e)}") from e

        # Check if any valid records are left
        if df.height == 0:
            raise ValueError("Data quality check failed: No valid records left after transformation.")

        # Data Quality Warning Check: Alert if rate is negative or > 1% per day
        out_of_bounds = df.filter((pl.col("valor") < 0.0) | (pl.col("valor") > 1.0))
        if out_of_bounds.height > 0:
            # Format date as string for clean printing if needed
            out_of_bounds_str = out_of_bounds.with_columns(pl.col("data").dt.strftime("%Y-%m-%d"))
            logging.warning(
                f"Data quality warning: Detected {out_of_bounds.height} rates outside standard market limits "
                f"(negative or > 1% per day): {out_of_bounds_str.select(['data', 'valor']).to_dicts()}"
            )

        # 5. Save clean data to Silver Parquet storage
        output_path = self.writer.write_clean_data(df)
        return output_path
