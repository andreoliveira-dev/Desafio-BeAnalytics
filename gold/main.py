import sys
from gold.adapters.parquet_reader_adapter import ParquetCleanReaderAdapter
from gold.adapters.parquet_writer_adapter import ParquetMetricsWriterAdapter
from gold.services.aggregate_service import AggregateService


def run(input_path: str = "data/silver/selic_clean.parquet", output_dir: str = "data/gold") -> tuple:
    print(f"Starting Gold Aggregation. Input clean: {input_path}")

    reader = ParquetCleanReaderAdapter(file_path=input_path)
    writer = ParquetMetricsWriterAdapter(output_dir=output_dir)
    service = AggregateService(reader=reader, writer=writer)

    try:
        monthly_path, annual_path = service.execute()
        print("Gold Aggregation finished successfully.")
        print(f"Monthly metrics saved to: {monthly_path}")
        print(f"Annual metrics saved to: {annual_path}")
        return monthly_path, annual_path
    except Exception as e:
        print(f"Gold Aggregation failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    in_path = sys.argv[1] if len(sys.argv) > 1 else "data/silver/selic_clean.parquet"
    out_dir = sys.argv[2] if len(sys.argv) > 2 else "data/gold"
    run(in_path, out_dir)
