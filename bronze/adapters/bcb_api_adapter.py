import requests
from typing import List
from bronze.ports.output_ports import SelicSourcePort
from bronze.domain.models import SelicRawRecord


class BcbApiAdapter(SelicSourcePort):
    def __init__(self, base_url: str = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados"):
        self.base_url = base_url

    def fetch_data(self, start_date: str, end_date: str) -> List[SelicRawRecord]:
        params = {
            "formato": "json",
            "dataInicial": start_date,
            "dataFinal": end_date
        }
        try:
            response = requests.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            return [
                SelicRawRecord(data=item["data"], valor=item["valor"])
                for item in data
            ]
        except Exception as e:
            raise RuntimeError(f"Error fetching data from BCB API: {str(e)}") from e
