import sys
from gold.adapters.parquet_reader_adapter import ParquetCleanReaderAdapter
from gold.adapters.parquet_writer_adapter import ParquetMetricsWriterAdapter
from gold.services.aggregate_service import AggregateService


def run(
    input_path: str = "data/silver/selic_cleaned.parquet",
    output_path: str = "data/gold/selic_metrics.parquet"
) -> str:
    print(f"Starting Gold Aggregation. Input clean: {input_path}")

    reader = ParquetCleanReaderAdapter(file_path=input_path)
    writer = ParquetMetricsWriterAdapter(file_path=output_path)
    service = AggregateService(reader=reader, writer=writer)

    try:
        saved_path = service.execute()
        print("Gold Aggregation finished successfully.")
        print(f"Metrics saved to: {saved_path}")
        return saved_path
    except Exception as e:
        print(f"Gold Aggregation failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    in_path = sys.argv[1] if len(sys.argv) > 1 else "data/silver/selic_cleaned.parquet"
    out_path = sys.argv[2] if len(sys.argv) > 2 else "data/gold/selic_metrics.parquet"
    run(in_path, out_path)
