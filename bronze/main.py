import sys
from bronze.adapters.bcb_api_adapter import BcbApiAdapter
from bronze.adapters.parquet_storage_adapter import LocalParquetStorageAdapter
from bronze.services.ingest_service import IngestService


def run(start_date: str = "01/01/2020", end_date: str = "31/12/2024") -> str:
    print(f"Starting Bronze Ingestion for period: {start_date} to {end_date}")

    from bronze.adapters.circuit_breaker_state_adapter import SqlCircuitBreakerStateAdapter

    state_adapter = SqlCircuitBreakerStateAdapter()
    source = BcbApiAdapter(state_port=state_adapter)
    storage = LocalParquetStorageAdapter()
    service = IngestService(source=source, storage=storage)

    try:
        output_path = service.execute(start_date, end_date)
        print(f"Bronze Ingestion finished successfully. Raw data saved to: {output_path}")
        return output_path
    except Exception as e:
        print(f"Bronze Ingestion failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    s_date = sys.argv[1] if len(sys.argv) > 1 else "01/01/2020"
    e_date = sys.argv[2] if len(sys.argv) > 2 else "31/12/2024"
    run(s_date, e_date)
