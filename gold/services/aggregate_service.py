import pandas as pd
from typing import Tuple
from gold.ports.input_ports import AggregateUseCase
from gold.ports.output_ports import CleanDataReaderPort, MetricsWriterPort


class AggregateService(AggregateUseCase):
    def __init__(self, reader: CleanDataReaderPort, writer: MetricsWriterPort):
        self.reader = reader
        self.writer = writer

    def execute(self) -> Tuple[str, str]:
        # 1. Read cleaned data
        df = self.reader.read_clean_data()

        # 2. Check if clean data is empty
        if df is None or df.empty:
            raise ValueError("Data quality check failed: Clean data is empty.")

        # 3. Calculate Monthly Metrics
        monthly_df = self._calculate_monthly_metrics(df)

        # 4. Calculate Annual Metrics
        annual_df = self._calculate_annual_metrics(df)

        # 5. Data Quality Checks on Outputs
        self._validate_metrics(monthly_df, annual_df)

        # 6. Save Metrics
        saved_paths = self.writer.write_metrics(monthly_df, annual_df)
        return saved_paths

    def _calculate_monthly_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        df_temp = df.copy()
        df_temp["data"] = pd.to_datetime(df_temp["data"])
        df_temp["ano"] = df_temp["data"].dt.year
        df_temp["mes"] = df_temp["data"].dt.month

        # Group by year and month
        grouped = df_temp.groupby(["ano", "mes"])["valor"].agg(["mean", "std"]).reset_index()
        grouped = grouped.rename(columns={"mean": "media_mensal", "std": "desvio_padrao_mensal"})

        # Fill NaN standard deviation (e.g. if a month has only 1 data point) with 0.0
        grouped["desvio_padrao_mensal"] = grouped["desvio_padrao_mensal"].fillna(0.0)

        # Sort chronologically to compute variation correctly
        grouped = grouped.sort_values(by=["ano", "mes"]).reset_index(drop=True)

        # Compute variations compared to the previous month
        grouped["variacao_absoluta_mensal"] = grouped["media_mensal"].diff()
        grouped["variacao_percentual_mensal"] = grouped["media_mensal"].pct_change() * 100.0

        # Fill first row's variations with 0.0
        grouped["variacao_absoluta_mensal"] = grouped["variacao_absoluta_mensal"].fillna(0.0)
        grouped["variacao_percentual_mensal"] = grouped["variacao_percentual_mensal"].fillna(0.0)

        return grouped

    def _calculate_annual_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        df_temp = df.copy()
        df_temp["data"] = pd.to_datetime(df_temp["data"])
        df_temp["ano"] = df_temp["data"].dt.year

        # Compounding formula for daily rates:
        # Rate (%) = [ Product (1 + rate_i / 100) - 1 ] * 100
        def compound_rate(series):
            factors = 1.0 + (series / 100.0)
            return (factors.prod() - 1.0) * 100.0

        grouped = df_temp.groupby("ano")["valor"].agg(compound_rate).reset_index()
        grouped = grouped.rename(columns={"valor": "taxa_acumulada_anual"})

        return grouped

    def _validate_metrics(self, monthly_df: pd.DataFrame, annual_df: pd.DataFrame) -> None:
        # Ensure we have records
        if monthly_df.empty or annual_df.empty:
            raise ValueError("Data quality check failed: Generated metrics are empty.")

        # Check that averages are within expected economic bounds for Selic rates
        # (0% to 50% monthly is a very safe limit)
        if not monthly_df["media_mensal"].between(0.0, 50.0).all():
            bad_values = monthly_df[~monthly_df["media_mensal"].between(0.0, 50.0)]
            bad_list = bad_values[['ano', 'mes', 'media_mensal']].to_dict('records')
            raise ValueError(
                f"Data quality check failed: Out of bounds monthly average: {bad_list}"
            )

        # Check for nulls in the calculated metrics
        if monthly_df.isnull().any().any() or annual_df.isnull().any().any():
            raise ValueError("Data quality check failed: Generated metrics contain null values.")
