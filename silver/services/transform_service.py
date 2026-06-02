import logging
import polars as pl
from silver.ports.input_ports import TransformUseCase
from silver.ports.output_ports import RawDataReaderPort, CleanDataWriterPort


class TransformService(TransformUseCase):
    def __init__(self, reader: RawDataReaderPort, writer: CleanDataWriterPort):
        self.reader = reader
        self.writer = writer

    def execute(self) -> str:
        
        df_lazy = self.reader.read_raw_data()

        try:
            if df_lazy.limit(1).collect().height == 0:
                raise ValueError("Data quality check failed: Raw data is empty.")
        except Exception as e:
            if isinstance(e, ValueError) and "Data quality check failed" in str(e):
                raise
            raise ValueError(f"Data quality check failed: Raw data is empty or corrupted: {str(e)}") from e

        df_lazy = df_lazy.filter(pl.col("data").is_not_null() & pl.col("valor").is_not_null())

        df_lazy = df_lazy.with_columns([
            pl.col("data").str.strptime(pl.Date, format="%d/%m/%Y"),
            pl.col("valor").cast(pl.Float64, strict=False)
        ])

        df_lazy = df_lazy.filter(pl.col("valor").is_not_null() & pl.col("data").is_not_null())

        df_lazy = df_lazy.unique(subset=["data"])

        df_lazy = df_lazy.sort("data")

        try:
            df = df_lazy.collect(streaming=True)
        except Exception as e:
            raise ValueError(f"Data quality check failed during transformation collect: {str(e)}") from e

        if df.height == 0:
            raise ValueError("Data quality check failed: No valid records left after transformation.")

        out_of_bounds = df.filter((pl.col("valor") < 0.0) | (pl.col("valor") > 1.0))
        if out_of_bounds.height > 0:
            out_of_bounds_str = out_of_bounds.with_columns(pl.col("data").dt.strftime("%Y-%m-%d"))
            logging.warning(
                f"Data quality warning: Detected {out_of_bounds.height} rates outside standard market limits "
                f"(negative or > 1% per day): {out_of_bounds_str.select(['data', 'valor']).to_dicts()}"
            )

        output_path = self.writer.write_clean_data(df)
        return output_path
