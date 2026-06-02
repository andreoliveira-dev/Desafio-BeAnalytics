import os
import sys


def run_bronze(
    start_date: str = "01/01/2020",
    end_date: str = "31/12/2024",
    output_path: str = "data/bronze/selic_raw.parquet"
) -> str:
    """
    Ingests raw SELIC rate data from BCB API and saves to Parquet format.
    """
    from bronze.adapters.bcb_api_adapter import BcbApiAdapter
    from bronze.adapters.parquet_storage_adapter import LocalParquetStorageAdapter
    from bronze.services.ingest_service import IngestService

    if not output_path.startswith("s3://"):
        output_dir = os.path.dirname(output_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

    from bronze.adapters.circuit_breaker_state_adapter import SqlCircuitBreakerStateAdapter

    state_adapter = SqlCircuitBreakerStateAdapter()
    source = BcbApiAdapter(state_port=state_adapter)

    storage_type = os.getenv("STORAGE_TYPE", "local").lower()
    if storage_type == "s3" or output_path.startswith("s3://"):
        from bronze.adapters.s3_storage_adapter import S3ParquetStorageAdapter
        storage = S3ParquetStorageAdapter(file_path=output_path)
    else:
        storage = LocalParquetStorageAdapter(file_path=output_path)

    service = IngestService(source=source, storage=storage)

    return service.execute(start_date, end_date)


if __name__ == "__main__":
    s_date = sys.argv[1] if len(sys.argv) > 1 else "01/01/2020"
    e_date = sys.argv[2] if len(sys.argv) > 2 else "31/12/2024"
    out_path = sys.argv[3] if len(sys.argv) > 3 else "data/bronze/selic_raw.parquet"
    run_bronze(s_date, e_date, out_path)
