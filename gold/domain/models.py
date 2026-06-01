from dataclasses import dataclass


@dataclass
class SelicMonthlyMetrics:
    ano: int
    mes: int
    media_mensal: float
    desvio_padrao_mensal: float
    variacao_absoluta_mensal: float
    variacao_percentual_mensal: float


@dataclass
class SelicAnnualMetrics:
    ano: int
    taxa_acumulada_anual: float
