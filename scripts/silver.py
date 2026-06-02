import sys


def run_silver(
    input_path: str = None,
    output_path: str = None
) -> str:
    """
    Cleans and standardizes raw SELIC rate data.
    """
    import os
    from silver.adapters.parquet_reader_adapter import ParquetRawReaderAdapter
    from silver.adapters.parquet_writer_adapter import ParquetCleanWriterAdapter
    from silver.services.transform_service import TransformService

    storage_type = os.getenv("STORAGE_TYPE", "local").lower()
    if input_path is None:
        input_path = "s3://selic-bucket/bronze/selic_raw.parquet" if storage_type == "s3" else "data/bronze/selic_raw.parquet"
    if output_path is None:
        output_path = "s3://selic-bucket/silver/selic_cleaned.parquet" if storage_type == "s3" else "data/silver/selic_cleaned.parquet"

    reader = ParquetRawReaderAdapter(file_path=input_path)
    writer = ParquetCleanWriterAdapter(file_path=output_path)
    service = TransformService(reader=reader, writer=writer)

    return service.execute()



if __name__ == "__main__":
    in_path = sys.argv[1] if len(sys.argv) > 1 else "data/bronze/selic_raw.parquet"
    out_path = sys.argv[2] if len(sys.argv) > 2 else "data/silver/selic_cleaned.parquet"
    run_silver(in_path, out_path)
