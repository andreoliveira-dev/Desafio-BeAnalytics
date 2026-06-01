import sys
from silver.adapters.parquet_reader_adapter import ParquetRawReaderAdapter
from silver.adapters.parquet_writer_adapter import ParquetCleanWriterAdapter
from silver.services.transform_service import TransformService


def run(input_path: str = "data/bronze/selic_raw.parquet", output_dir: str = "data/silver") -> str:
    print(f"Starting Silver Transformation. Input raw: {input_path}")

    reader = ParquetRawReaderAdapter(file_path=input_path)
    writer = ParquetCleanWriterAdapter(output_dir=output_dir)
    service = TransformService(reader=reader, writer=writer)

    try:
        output_path = service.execute()
        print(f"Silver Transformation finished successfully. Clean data saved to: {output_path}")
        return output_path
    except Exception as e:
        print(f"Silver Transformation failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    in_path = sys.argv[1] if len(sys.argv) > 1 else "data/bronze/selic_raw.parquet"
    out_dir = sys.argv[2] if len(sys.argv) > 2 else "data/silver"
    run(in_path, out_dir)
