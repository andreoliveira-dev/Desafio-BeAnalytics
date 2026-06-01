import pandas as pd
from silver.ports.input_ports import TransformUseCase
from silver.ports.output_ports import RawDataReaderPort, CleanDataWriterPort


class TransformService(TransformUseCase):
    def __init__(self, reader: RawDataReaderPort, writer: CleanDataWriterPort):
        self.reader = reader
        self.writer = writer

    def execute(self) -> str:
        # 1. Read raw DataFrame
        df = self.reader.read_raw_data()

        # 2. Check if DataFrame is empty (Bronze data check)
        if df is None or df.empty:
            raise ValueError("Data quality check failed: Raw data is empty.")

        # 3. Clean and standardize
        # Drop rows where critical fields are completely null
        df = df.dropna(subset=["data", "valor"])

        if df.empty:
            raise ValueError("Data quality check failed: No valid records left after dropping nulls.")

        # Convert 'data' column from dd/MM/yyyy to date type
        try:
            # We convert to datetime, then extract date object
            df["data"] = pd.to_datetime(df["data"], format="%d/%m/%Y").dt.date
        except Exception as e:
            raise ValueError(f"Data quality check failed: Invalid date format in raw data: {str(e)}") from e

        # Convert 'valor' column to numeric
        try:
            df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
            # Drop rows with invalid non-numeric rates
            df = df.dropna(subset=["valor"])
        except Exception as e:
            raise ValueError(f"Data quality check failed: Invalid rate values: {str(e)}") from e

        # Double check if any valid records are left
        if df.empty:
            raise ValueError("Data quality check failed: No valid records left after transformation.")

        # Handle duplicates on 'data' (we want unique dates in our time-series)
        df = df.drop_duplicates(subset=["data"], keep="first")

        # Sort chronologically by date
        df = df.sort_values(by="data").reset_index(drop=True)

        # 4. Save clean data to Silver Parquet storage
        output_path = self.writer.write_clean_data(df)
        return output_path
