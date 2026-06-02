import sys
from silver.adapters.parquet_reader_adapter import ParquetRawReaderAdapter
from silver.adapters.parquet_writer_adapter import ParquetCleanWriterAdapter
from silver.services.transform_service import TransformService


def run(input_path: str = None, output_path: str = None) -> str:
    import os
    storage_type = os.getenv("STORAGE_TYPE", "local").lower()

    if input_path is None:
        input_path = (
            "s3://selic-bucket/bronze/selic_raw.parquet"
            if storage_type == "s3"
            else "data/bronze/selic_raw.parquet"
        )
    if output_path is None:
        output_path = (
            "s3://selic-bucket/silver/selic_cleaned.parquet"
            if storage_type == "s3"
            else "data/silver/selic_cleaned.parquet"
        )

    print(f"Starting Silver Transformation. Input raw: {input_path}")

    reader = ParquetRawReaderAdapter(file_path=input_path)
    writer = ParquetCleanWriterAdapter(file_path=output_path)
    service = TransformService(reader=reader, writer=writer)

    try:
        res_path = service.execute()
        print(f"Silver Transformation finished successfully. Clean data saved to: {res_path}")
        return res_path
    except Exception as e:
        print(f"Silver Transformation failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    in_path = sys.argv[1] if len(sys.argv) > 1 else None
    out_path = sys.argv[2] if len(sys.argv) > 2 else None
    run(in_path, out_path)
