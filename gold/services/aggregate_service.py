import polars as pl
from gold.ports.input_ports import AggregateUseCase
from gold.ports.output_ports import CleanDataReaderPort, MetricsWriterPort


class AggregateService(AggregateUseCase):
    def __init__(self, reader: CleanDataReaderPort, writer: MetricsWriterPort):
        self.reader = reader
        self.writer = writer

    def execute(self) -> str:

        df_lazy = self.reader.read_clean_data()

        try:
            if df_lazy.limit(1).collect().height == 0:
                raise ValueError("Data quality check failed: Clean data is empty.")
        except Exception as e:
            if isinstance(e, ValueError) and "Data quality check failed" in str(e):
                raise
            raise ValueError(f"Data quality check failed: Clean data is empty or corrupted: {str(e)}") from e

        monthly_lazy = self._calculate_monthly_metrics(df_lazy)

        annual_lazy = self._calculate_annual_metrics(df_lazy)

        metrics_lazy = monthly_lazy.join(annual_lazy, on="ano", how="left")

        try:
            metrics_df = metrics_lazy.collect(streaming=True)
        except Exception as e:
            raise ValueError(f"Data quality check failed during metrics aggregation: {str(e)}") from e

        self._validate_metrics(metrics_df)

        saved_path = self.writer.write_metrics(metrics_df)
        return saved_path

    def _calculate_monthly_metrics(self, df_lazy: pl.LazyFrame) -> pl.LazyFrame:
        df_temp = df_lazy.with_columns([
            pl.col("data").dt.year().alias("ano"),
            pl.col("data").dt.month().alias("mes")
        ])

        grouped = (
            df_temp
            .group_by(["ano", "mes"])
            .agg([
                pl.col("valor").mean().alias("media_mensal"),
                pl.col("valor").std().alias("desvio_padrao_mensal")
            ])
            .with_columns(
                pl.col("desvio_padrao_mensal").fill_null(0.0)
            )
            .sort(["ano", "mes"])
        )

        grouped = grouped.with_columns(
            (
                (pl.col("media_mensal") - pl.col("media_mensal").shift(1))
                / pl.col("media_mensal").shift(1)
                * 100.0
            )
            .fill_null(0.0)
            .alias("variacao_mensal")
        )

        return grouped

    def _calculate_annual_metrics(self, df_lazy: pl.LazyFrame) -> pl.LazyFrame:
        df_temp = df_lazy.with_columns(
            pl.col("data").dt.year().alias("ano")
        )

        grouped = (
            df_temp
            .group_by("ano")
            .agg(
                (((1.0 + pl.col("valor") / 100.0).product() - 1.0) * 100.0).alias("taxa_acumulada_anual")
            )
        )

        return grouped

    def _validate_metrics(self, df: pl.DataFrame) -> None:

        if df.height == 0:
            raise ValueError("Data quality check failed: Generated metrics are empty.")

        out_of_bounds = df.filter((pl.col("media_mensal") < 0.0) | (pl.col("media_mensal") > 50.0))
        if out_of_bounds.height > 0:
            bad_list = out_of_bounds.select(['ano', 'mes', 'media_mensal']).to_dicts()
            raise ValueError(
                f"Data quality check failed: Out of bounds monthly average: {bad_list}"
            )

        for col in df.columns:
            if df[col].null_count() > 0:
                raise ValueError("Data quality check failed: Generated metrics contain null values.")
