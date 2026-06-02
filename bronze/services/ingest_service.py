from bronze.ports.input_ports import IngestUseCase
from bronze.ports.output_ports import SelicSourcePort, RawStoragePort


class IngestService(IngestUseCase):
    def __init__(self, source: SelicSourcePort, storage: RawStoragePort):
        self.source = source
        self.storage = storage

    def execute(self, start_date: str, end_date: str) -> str:
        records = self.source.fetch_data(start_date, end_date)

        if not records:
            raise ValueError("Data quality check failed: No records fetched from source API.")
        output_path = self.storage.save_data(records)
        return output_path
