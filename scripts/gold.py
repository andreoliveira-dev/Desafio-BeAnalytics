import sys


def run_gold(
    input_path: str = "data/silver/selic_cleaned.parquet",
    output_path: str = "data/gold/selic_metrics.parquet"
) -> str:
    """
    Aggregates cleaned SELIC rate data into analytical metrics.
    """
    from gold.adapters.parquet_reader_adapter import ParquetCleanReaderAdapter
    from gold.adapters.parquet_writer_adapter import ParquetMetricsWriterAdapter
    from gold.services.aggregate_service import AggregateService

    reader = ParquetCleanReaderAdapter(file_path=input_path)
    writer = ParquetMetricsWriterAdapter(file_path=output_path)
    service = AggregateService(reader=reader, writer=writer)

    return service.execute()


if __name__ == "__main__":
    in_path = sys.argv[1] if len(sys.argv) > 1 else "data/silver/selic_cleaned.parquet"
    out_path = sys.argv[2] if len(sys.argv) > 2 else "data/gold/selic_metrics.parquet"
    run_gold(in_path, out_path)
