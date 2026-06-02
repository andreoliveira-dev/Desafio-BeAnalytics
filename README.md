# Selic Rate Orchestration Pipeline (Banco Central do Brasil)

Este repositório contém a orquestração de um pipeline de dados de nível de produção utilizando o **Apache Airflow** para consumir, tratar e consolidar dados históricos da Taxa Selic diária do **Banco Central do Brasil (SGS Série 11)**, para o período de **01/01/2020 a 31/12/2024**.

O projeto foi projetado e construído seguindo princípios rigorosos de **Clean Code**, **SOLID**, **TDD (Test-Driven Development)**, **Arquitetura Hexagonal (Ports and Adapters)**, resiliência ativa contra falhas de API e verificações rígidas de **Qualidade de Dados (Data Quality Gates)** entre cada camada da arquitetura Medallion.

---

## Arquitetura de Dados (Medallion, Ports & Adapters com Polars & MinIO)

O pipeline de dados está dividido em três camadas lógicas independentes (Bronze, Silver e Gold), isoladas em pacotes Python modulares. Cada pacote possui fronteiras bem delimitadas através de portas (interfaces) e adaptadores (infraestrutura), suportando armazenamento local ou Object Storage S3 (MinIO) de forma transparente.

```mermaid
graph TD
    API([API BCB - SGS 11]) -->|Ingestão & Resiliência - bronze/| B_Stg[(S3 ou Local: selic_raw.parquet)]
    B_Stg -->|Polars Lazy API & Streaming - silver/| S_Stg[(S3 ou Local: selic_cleaned.parquet)]
    S_Stg -->|Polars Agregações Vetorizadas - gold/| G_Stg[(S3 ou Local: selic_metrics.parquet)]

    subgraph Airflow DAG [Orquestração Airflow]
        T1[Task 1: Ingestão Bronze] --> T2[Task 2: Transformação Silver]
        T2 --> T3[Task 3: Agregação Gold]
    end
```

### Detalhamento Técnico das Camadas (Processamento com Polars Lazy API)

1. **Bronze (Ingestion)**:
   - **Responsabilidade**: Consumir dados brutos da API SGS.
   - **Mecanismos de Resiliência Ativa**:
     - **Exponential Backoff**: Tenta realizar até 3 chamadas com delays crescentes ($2^{\text{tentativa}}$) se a API retornar instabilidade de rede ou erros 5xx.
     - **Circuit Breaker**: Previne sobrecarga e falhas repetidas. Se ocorrerem 5 falhas consecutivas, o circuito abre por **60 segundos**, negando qualquer nova chamada imediatamente (`CircuitBreakerOpenError`) sem consumir recursos de rede.
   - **Persistência**: Parquet (`data/bronze/selic_raw.parquet` ou `s3://selic-bucket/bronze/selic_raw.parquet`).
   - **Trilha de Auditoria (JSON)**: Salva adicionalmente o payload original retornado pela API em formato JSON (`selic_raw.json`) no mesmo diretório de destino.

2. **Silver (Transformation)**:
   - **Responsabilidade**: Sanitização e padronização rápida utilizando **Polars LazyFrame**.
   - **Transformações**:
     - Conversão de `data` para tipo nativo de data (`Date`).
     - Conversão de `valor` (taxa percentual diária) para numérico (`Float64`).
     - Exclusão de duplicidades temporais e registros nulos (`drop_nulls`, `unique`).
     - Ordenação cronológica estrita.
   - **Performance**: Executado em modo lazy com streaming (`streaming=True` no `collect()`) para otimização de plano física e memória.
   - **Quality Gates**: Emissão de alertas (`warnings`) no log caso as taxas diárias estejam fora de limites normais de mercado (ex: negativas ou acima de 1,0% ao dia).
   - **Persistência**: Parquet unificado (`data/silver/selic_cleaned.parquet` ou `s3://selic-bucket/silver/selic_cleaned.parquet`) e **particionado** no estilo Hive (`partitioned/year=YYYY/month=MM/data.parquet`) sob a mesma pasta.

3. **Gold (Analytics & Aggregation)**:
   - **Responsabilidade**: Agregações analíticas e consolidação de métricas via **Polars Lazy API**.
   - **Métricas**:
     - `media_mensal` e `desvio_padrao_mensal` (volatilidade).
     - `variacao_mensal` (comparativo percentual em relação ao mês anterior via expressões de janela do Polars).
     - `taxa_acumulada_anual`: Juros compostos calculados por expressões vetorizadas rápidas do Polars:
       $$\text{Taxa Acumulada (\%)} = \left[ \prod_{i=1}^{N} \left(1 + \frac{\text{taxa}_i}{100}\right) - 1 \right] \times 100$$
   - **Quality Gates**: Rejeita saídas com métricas nulas e valida se a média mensal está dentro da amplitude de normalidade econômica real (0% a 50% mensal).
   - **Persistência**: Parquet consolidado (`data/gold/selic_metrics.parquet` ou `s3://selic-bucket/gold/selic_metrics.parquet`) e **particionado** no estilo Hive (`partitioned/year=YYYY/month=MM/data.parquet`) sob a mesma pasta.

---

## Estrutura de Pastas (Arquitetura Hexagonal)

Cada pacote (`bronze/`, `silver/`, `gold/`) está estruturado da seguinte forma:

```
[camada]/
├── domain/                  # Entidades puras e regras de negócio
├── ports/                   # Interfaces de fronteira (Ports)
│   ├── input_ports.py       # Interfaces chamadas pelo orquestrador (Casos de Uso)
│   └── output_ports.py      # Interfaces para sistemas externos (Storage, API)
├── adapters/                # Implementações de infraestrutura concretas (Adapters)
│   ├── bcb_api_adapter.py        # Adaptador HTTP com Circuit Breaker & Backoff
│   ├── parquet_reader_adapter.py # Adaptador de leitura física Parquet
│   └── parquet_writer_adapter.py # Adaptador de escrita física Parquet
├── services/                # Regras de fluxo e Quality Gates (Casos de Uso)
├── tests/                   # Suíte de testes unitários isolados
├── main.py                  # CLI executável
```

---

## ⚙️ Configurações de Ambiente (.env)

Todas as variáveis sensíveis, credenciais de banco e parametrizações do Airflow estão desacopladas do código de infraestrutura e configuradas via arquivo `.env`. Crie o arquivo `.env` na raiz do projeto com a seguinte estrutura:

```ini

AIRFLOW_UID=1000

POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow_secure_password
POSTGRES_DB=airflow
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow_secure_password@postgres/airflow

AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=admin
AIRFLOW_ADMIN_EMAIL=admin@beanalytic.com.br

# Armazenamento: local ou s3
STORAGE_TYPE=s3
S3_ENDPOINT_URL=http://localhost:9000
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_DEFAULT_REGION=us-east-1
```


---

## Execução via Docker Compose

A stack roda com três containers orquestrados com restart automático e verificação de saúde (`healthcheck` no PostgreSQL):

```bash

docker compose up -d


docker compose ps
```

1. Acesse o painel do Airflow em [http://localhost:8080](http://localhost:8080) com os dados definidos no seu `.env` (`admin`/`admin`).
2. A DAG `dag_selic_medallion` estará pronta para ser ativada e disparada manualmente.

---

## Qualidade de Software, Testes & CI/CD

### 1. Testes Locais e Linter
Mantemos uma alta cobertura de testes que cobre transformações matemáticas, fluxos de erros, estados de Circuit Breaker e integridade da topologia da DAG.

```bash

python3 -m venv venv
source venv/bin/activate


pip install --upgrade pip
pip install -r requirements.txt


PYTHONPATH=. pytest --cov=bronze --cov=silver --cov=gold --cov-fail-under=80


flake8 bronze silver gold scripts tests
```

### 2. Esteira de Integração Contínua (GitHub Actions)
A esteira configurada em `.github/workflows/ci-cd-pipeline.yml` valida todos os Pull Requests e pushes nos branches `main` e `develop`:
- Executa a validação sintática do linter com o `flake8` (máximo de 120 caracteres por linha).
- Roda os testes unitários (`pytest-cov`).
- **Quality Gate de Cobertura**: A execução falha automaticamente no GitHub caso a cobertura de linhas caia abaixo de **80%** (cobertura atual do projeto: **95,18%**).
- **Compatibilidade do Pendulum**: As dependências do projeto contam com a pinagem estrita `pendulum==2.1.2`, mitigando bugs de tipagem conhecidos nas versões mais recentes da biblioteca em execuções de scheduling do Airflow.

---

## 🛠️ Execução via CLI (Manual)

Se necessário rodar o pipeline fora do Airflow de forma manual, utilize os scripts modulares:

```bash

PYTHONPATH=. python scripts/bronze.py "01/01/2020" "31/12/2024" "data/bronze/selic_raw.parquet"


PYTHONPATH=. python scripts/silver.py "data/bronze/selic_raw.parquet" "data/silver/selic_cleaned.parquet"
PYTHONPATH=. python scripts/gold.py "data/silver/selic_cleaned.parquet" "data/gold/selic_metrics.parquet"
```
