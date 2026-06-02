import sys


def run_gold(
    input_path: str = None,
    output_path: str = None
) -> str:
    """
    Aggregates cleaned SELIC rate data into analytical metrics.
    """
    import os
    from gold.adapters.parquet_reader_adapter import ParquetCleanReaderAdapter
    from gold.adapters.parquet_writer_adapter import ParquetMetricsWriterAdapter
    from gold.services.aggregate_service import AggregateService

    storage_type = os.getenv("STORAGE_TYPE", "local").lower()
    if input_path is None:
        input_path = (
            "s3://selic-bucket/silver/selic_cleaned.parquet"
            if storage_type == "s3"
            else "data/silver/selic_cleaned.parquet"
        )
    if output_path is None:
        output_path = (
            "s3://selic-bucket/gold/selic_metrics.parquet"
            if storage_type == "s3"
            else "data/gold/selic_metrics.parquet"
        )

    reader = ParquetCleanReaderAdapter(file_path=input_path)
    writer = ParquetMetricsWriterAdapter(file_path=output_path)
    service = AggregateService(reader=reader, writer=writer)

    return service.execute()


if __name__ == "__main__":
    in_path = sys.argv[1] if len(sys.argv) > 1 else "data/silver/selic_cleaned.parquet"
    out_path = sys.argv[2] if len(sys.argv) > 2 else "data/gold/selic_metrics.parquet"
    run_gold(in_path, out_path)
