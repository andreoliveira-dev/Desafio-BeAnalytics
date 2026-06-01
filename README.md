# Selic Rate Orchestration Pipeline (Banco Central do Brasil)

Este repositório contém a orquestração de um pipeline de dados utilizando o **Apache Airflow** para consumir dados históricos da Taxa Selic diária do **Banco Central do Brasil (SGS Série 11)**, para o período de **01/01/2020 a 31/12/2024**.

O projeto foi inteiramente desenhado seguindo princípios de **Clean Code**, **SOLID**, **TDD (Test-Driven Development)**, **Arquitetura Hexagonal (Ports and Adapters)**, e conta com verificações rigorosas de **Qualidade de Dados** entre as camadas.

---

## 🏗️ Arquitetura do Projeto

O pipeline de dados é estruturado em três camadas clássicas de Data Lakehouse (Bronze, Silver e Gold), isoladas de forma modular em pacotes Python independentes. Cada camada implementa sua própria Arquitetura Hexagonal para garantir desacoplamento absoluto entre regras de negócio e infraestrutura.

```mermaid
graph TD
    API([API BCB - SGS 11]) -->|Ingestão - bronze/| B_Stg[(data/bronze/selic_raw.parquet)]
    B_Stg -->|Limpeza & Tipagem - silver/| S_Stg[(data/silver/selic_clean.parquet)]
    S_Stg -->|Métricas & Agregação - gold/| G_Stg_M[(data/gold/selic_mensal.parquet)]
    S_Stg -->|Métricas & Agregação - gold/| G_Stg_A[(data/gold/selic_anual.parquet)]

    subgraph Airflow DAG [Orquestração Airflow]
        T1[Task 1: Ingestão Bronze] --> T2[Task 2: Transformação Silver]
        T2 --> T3[Task 3: Agregação Gold]
    end
```

### Detalhamento das Camadas

1. **Bronze (Ingestion)**:
   - **Responsabilidade**: Consome a API do Banco Central e salva os dados brutos exatamente como retornados.
   - **Formato**: Parquet (`data/bronze/selic_raw.parquet`).
   - **Qualidade de Dados**: Garante que o retorno da API não esteja vazio e que a persistência em disco ocorreu corretamente.
   
2. **Silver (Transformation)**:
   - **Responsabilidade**: Realiza a limpeza e a tipagem dos dados.
   - **Transformações**:
     - Conversão do campo `data` de `dd/MM/aaaa` para data ISO (`YYYY-MM-DD` / tipo Date).
     - Conversão de `valor` (string decimal contendo a taxa diária) para Float.
     - Remoção de nulos (`dropna`) e exclusão de duplicidades de datas (`drop_duplicates`).
     - Ordenação cronológica.
   - **Formato**: Parquet (`data/silver/selic_clean.parquet`).
   - **Qualidade de Dados**: Garante que nenhuma linha nula persista, valida o formato das datas e as faixas de valores de taxa Selic diária.

3. **Gold (Aggregation)**:
   - **Responsabilidade**: Consolidação e geração de métricas analíticas prontas para consumo de BI ou modelos.
   - **Métricas Calculadas**:
     - **Média Mensal** (`media_mensal`): A média aritmética das taxas diárias em cada mês.
     - **Desvio Padrão Mensal** (`desvio_padrao_mensal`): Medida de volatilidade da taxa no mês.
     - **Variação Absoluta Mensal** (`variacao_absoluta_mensal`): Diferença absoluta de média mensal comparado ao mês anterior (em pontos percentuais).
     - **Variação Percentual Mensal** (`variacao_percentual_mensal`): Variação percentual sobre a média do mês anterior.
     - **Taxa Acumulada Anual** (`taxa_acumulada_anual`): Acumulação da taxa composta diária usando a fórmula oficial de juros compostos:
       $$\text{Taxa Acumulada (\%)} = \left[ \prod_{i=1}^{N} \left(1 + \frac{\text{taxa}_i}{100}\right) - 1 \right] \times 100$$
   - **Formatos**: Parquet (`data/gold/selic_mensal.parquet` e `data/gold/selic_anual.parquet`).
   - **Qualidade de Dados**: Assegura que médias estejam dentro dos limites de taxas reais de mercado (entre 0% e 50% mensal) e rejeita qualquer dado nulo ou inconsistente.

---

## 📐 Estrutura de Pastas (Arquitetura Hexagonal)

Cada pacote (`bronze/`, `silver/`, `gold/`) está estruturado da seguinte forma:

```
[camada]/
├── domain/            # Modelos e regras puras de negócio (Dataclasses)
├── ports/             # Interfaces que delimitam as fronteiras
│   ├── input_ports.py    # Interfaces de entrada (Use Cases executados pelo orquestrador/main)
│   └── output_ports.py   # Interfaces de saída (Storage, API Reader, API Writer)
├── adapters/          # Implementações concretas de infraestrutura
│   ├── bcb_api_adapter.py      # Chamada HTTP à API
│   ├── parquet_reader_adapter.py # Leitura de arquivos Parquet locais
│   └── parquet_writer_adapter.py # Escrita de arquivos Parquet locais
├── services/          # Orquestrador do fluxo da camada (Usecase Implementation)
│   └── *_service.py            # Executa os adapters através das portas e checa qualidade
├── tests/             # Suíte de testes unitários (TDD)
│   ├── test_[camada]_adapters.py
│   └── test_[camada]_[fluxo].py
└── main.py            # Ponto de entrada executável da camada
```

Esta arquitetura segue o **SOLID**:
- **Single Responsibility Principle (SRP)**: Cada adapter e service faz apenas uma coisa.
- **Open-Closed Principle (OCP)**: Se precisarmos alterar o storage de Parquet local para S3, basta criar um novo adapter implementando a interface `RawStoragePort` sem alterar o `IngestService`.
- **Dependency Inversion Principle (DIP)**: O `IngestService` depende de abstrações (`SelicSourcePort`, `RawStoragePort`), não de implementações de APIs ou sistemas de arquivos concretos.

---

## 🚀 Como Executar Localmente

### 1. Requisitos Prévios
- Python 3.10 ou superior
- Docker e Docker Compose (se quiser subir o Airflow)

### 2. Configurar o Ambiente Virtual Python
Crie e ative o ambiente virtual para instalar as dependências e executar os testes:

```bash
# Criar o ambiente virtual
python3 -m venv venv

# Ativar no Linux/Mac
source venv/bin/activate

# Atualizar o pip e instalar os pacotes
pip install --upgrade pip
pip install -r requirements.txt
```

### 3. Rodando os Testes Unitários (TDD)
Os testes cobrem todas as regras de transformação, cálculos matemáticos compostos de juros, tratamento de exceções e persistência de dados. Execute a suíte de testes com o `pytest`:

```bash
PYTHONPATH=. pytest
```

### 4. Executando as Camadas Manualmente
Você pode rodar as camadas sequencialmente para verificar a geração local dos arquivos Parquet sem necessitar do Airflow:

```bash
# Executa a Ingestão (Bronze) - baixa dados de 2020 a 2024 e salva Parquet bruto
PYTHONPATH=. python -m bronze.main

# Executa a Limpeza (Silver) - tipa, limpa nulos e duplicados
PYTHONPATH=. python -m silver.main

# Executa as Agregações (Gold) - gera métricas mensais e anuais acumuladas
PYTHONPATH=. python -m gold.main
```

Após executar, você encontrará os arquivos salvos em:
- `data/bronze/selic_raw.parquet`
- `data/silver/selic_clean.parquet`
- `data/gold/selic_mensal.parquet`
- `data/gold/selic_anual.parquet`

---

## 🐳 Executando o Airflow via Docker Compose

A infraestrutura do Airflow é orquestrada de forma isolada em containers Docker Compose utilizando um banco PostgreSQL como metastore de metadados.

### 1. Configurar o Arquivo `.env`
O projeto necessita de um arquivo `.env` na raiz do diretório para mapear o `AIRFLOW_UID` (evitando problemas de permissão com o host) e definir as credenciais do banco e do Airflow. 

Você pode criar ou complementar o `.env` com a seguinte estrutura padrão:

```env
AIRFLOW_UID=1000

# Configurações do Banco de Dados
POSTGRES_USER=airflow
POSTGRES_PASSWORD=sua_senha_segura
POSTGRES_DB=airflow

# Conexão do Airflow
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:sua_senha_segura@postgres/airflow

# Usuário Administrador do Airflow
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=sua_senha_segura_admin
AIRFLOW_ADMIN_EMAIL=admin@example.com
```

*Dica: Para adicionar automaticamente seu UID do Linux ao `.env`, você pode executar:*
```bash
echo "AIRFLOW_UID=$(id -u)" >> .env
```

### 2. Inicializar os Serviços do Docker Compose
Execute o comando a seguir para subir os containers (Postgres, Webserver e Scheduler):

```bash
docker compose up -d
```

### 3. Acessar a Interface do Airflow
1. Abra o navegador em: [http://localhost:8080](http://localhost:8080)
2. Faça login utilizando as credenciais definidas nas variáveis `AIRFLOW_ADMIN_USER` e `AIRFLOW_ADMIN_PASSWORD` do seu arquivo `.env` (Padrão: `admin` / `sua_senha_segura_admin`).
3. Ative a DAG `dag_selic_medallion` e execute-a clicando no ícone de play (**Trigger DAG**).

### 4. Finalizar o Ambiente
Para parar os containers e limpar os recursos:

```bash
docker compose down
```

---

## 🛠️ Decisões Técnicas

- **Persistência em Parquet**: Escolhido por ser um formato colunar com alta taxa de compressão e excelente performance para leitura de grandes volumes de dados (comum no ecossistema Big Data e integrado com Pandas via PyArrow).
- **Tratamento de Erros e Retentativas**: Configurado na DAG (`dags/selic_pipeline_dag.py`) com retentativas e backoff de tempo. Caso a chamada de API do Banco Central venha a falhar por oscilação de rede, o Airflow orquestra as retentativas automaticamente.
- **Isolamento de Estado**: Cada etapa só roda após o sucesso da anterior. Os checks de qualidade impedem que dados corrompidos ou incompletos avancem de fase, gerando alertas rápidos através de falhas de Tasks no Airflow.
