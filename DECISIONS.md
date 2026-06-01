# Registro de Decisões de Arquitetura (Architecture Decision Records - ADR)

Este documento descreve as principais decisões de design, arquitetura e infraestrutura tomadas ao longo do desenvolvimento e refatoração do pipeline de dados da Taxa Selic, bem como as motivações por trás de cada escolha.

---

## 1. Adoção da Arquitetura Hexagonal (Ports & Adapters)

### Contexto
O pipeline consome dados da API pública do Banco Central do Brasil (SGS), realiza transformações de limpeza/tipagem e calcula métricas analíticas complexas, persistindo os resultados em arquivos locais no formato Parquet.

### Decisão
Toda a lógica de negócios das camadas (Bronze, Silver e Gold) foi estruturada seguindo o padrão de **Arquitetura Hexagonal**:
- **Domain**: Dataclasses puras sem dependência de frameworks.
- **Ports**: Interfaces abstratas de entrada (Use Cases executados pelo Airflow ou CLI) e de saída (Storage, API Reader, API Writer).
- **Adapters**: Implementações concretas de infraestrutura (chamadas HTTP com `requests`, manipulação de arquivos Parquet locais com `pandas`/`pyarrow`).

### Racional / Benefícios
- **Desacoplamento e SOLID**: Garante isolamento completo entre as regras de negócio de cálculo de juros/validações de limites e a infraestrutura externa.
- **Facilidade de Migração**: Caso o local de armazenamento precise mudar de arquivos Parquet locais para tabelas em banco relacional (ex: PostgreSQL) ou armazenamento em nuvem (ex: AWS S3), basta escrever um novo Adapter implementando a respectiva Port de saída, sem tocar na lógica de negócios ou nos serviços centrais.
- **Testabilidade**: Permite testar todos os serviços de forma puramente unitária utilizando Mocks para as portas de infraestrutura (TDD).

---

## 2. Estrutura de Pipeline Medallion (Bronze, Silver e Gold)

### Contexto
O pipeline necessitava de uma trilha de auditoria clara desde a ingestão da API bruta até o cálculo final das métricas analíticas.

### Decisão
Dividimos o pipeline em 3 camadas de dados bem definidas com seus respectivos formatos:
1. **Bronze (Ingestion)**: Salva o JSON recebido da API de forma puramente bruta e intacta no formato Parquet (`selic_raw.parquet`).
2. **Silver (Clean/Transform)**: Aplica regras de limpeza, remove duplicidades de datas, remove registros nulos, e padroniza a tipagem técnica (`datetime64[ns]` para as datas e `float64` para as taxas). Os dados limpos são salvos em `selic_cleaned.parquet`.
3. **Gold (Analytics)**: Agrega as taxas em métricas de média mensal, volatilidade (desvio padrão), variação percentual mensal e calcula a taxa acumulada composta anual.

---

## 3. Unificação de Métricas na Camada Gold (`selic_metrics.parquet`)

### Contexto
Originalmente, a camada Gold gerava arquivos separados para as métricas mensais (`selic_mensal.parquet`) e para a taxa acumulada anual (`selic_anual.parquet`).

### Decisão
Consolidamos o cálculo e a escrita da camada Gold para que ambos os conjuntos de métricas fossem unificados em um único DataFrame compilado (mesclados a partir da chave do ano) e salvos em um único arquivo: `data/gold/selic_metrics.parquet`.

### Racional / Benefícios
- **Simplificação de Consumo**: Facilita a leitura e integração com ferramentas de visualização e Business Intelligence (como Power BI, Tableau ou Metabase), eliminando a necessidade de as ferramentas finais executarem joins complexos entre tabelas de granularidades diferentes.
- **Single File Output**: Alinha-se ao objetivo de ter uma única fonte de verdade analítica consolidada por camada.

---

## 4. Parametrização Dinâmica de Caminhos de Arquivos (Adapters e CLI)

### Contexto
Os adaptadores originais recebiam o diretório de destino (`output_dir`) e hardcodavam internamente o nome dos arquivos (ex: `selic_raw.parquet`). Isso impedia a reutilização dos adaptadores para caminhos arbitrários ou testes específicos.

### Decisão
- Refatoramos os construtores dos adaptadores para aceitar o parâmetro completo `file_path`.
- Adicionamos suporte de retrocompatibilidade: caso o `file_path` não seja informado, o adaptador aceita o parâmetro antigo `output_dir` e monta o caminho padrão internamente.

### Racional / Benefícios
- **Flexibilidade no Airflow**: Permite especificar de forma centralizada na DAG ou no orquestrador os caminhos exatos que devem ser consumidos e escritos em cada etapa do DAG.
- **Preservação de Testes (OCP)**: A adição da lógica de retrocompatibilidade preservou a suíte de testes antigos de integração e unidade sem quebrar assinaturas legadas.

---

## 5. Arquitetura do Airflow Conteinerizada com Postgres Metastore

### Contexto
O desenvolvimento local precisava de um ambiente de orquestração isolado, reprodutível e condizente com ambientes de staging/produção reais.

### Decisão
Atualizamos a infraestrutura do `docker-compose.yaml` para executar 3 serviços dedicados:
1. **`postgres` (Banco de Metadados)**: Banco PostgreSQL dedicado com checagem de saúde (`healthcheck`) para gerenciar o estado interno do Airflow de forma persistente.
2. **`airflow-webserver`**: Servidor de interface web montado sob a porta local `8080`.
3. **`airflow-scheduler`**: Agendador de tarefas responsável por inicializar o banco de dados do Airflow, criar o usuário administrador de teste e triggar as execuções da DAG.

### Racional / Benefícios
- **Robustez**: Substitui o executor sequencial padrão (`SequentialExecutor`) que rodava sob banco embarcado SQLite (o qual gera gargalos e não aceita concorrência real) por um ambiente PostgreSQL completo capaz de operar em `LocalExecutor`.

---

## 6. Centralização de Segredos e Variáveis no `.env`

### Contexto
O arquivo `docker-compose.yaml` possuía senhas de banco de dados e credenciais de login de administradores hardcodadas diretamente no código, violando boas práticas de segurança.

### Decisão
Toda a configuração de credenciais, URL de conexões SQL e dados de usuário administrador do Airflow foram externalizadas para um arquivo `.env` na raiz do projeto (não commitado no repositório público), sendo lidas dinamicamente pelo Docker Compose.

### Racional / Benefícios
- **Segurança da Informação**: Evita o vazamento acidental de chaves, URLs e senhas críticas em repositórios do GitHub.
- **Permissão de Host Mapeada (`AIRFLOW_UID`)**: A variável `AIRFLOW_UID` definida dinamicamente com o UID do usuário local garante que os arquivos Parquet gerados no container sejam escritos com a propriedade do usuário host, eliminando problemas de permissões ao tentar ler ou apagar os arquivos no Linux local.

---

## 7. Garantia de Qualidade com TDD, Linter (PEP-8) e CI/CD

### Contexto
Era necessário garantir que novos commits não quebrassem as regras de negócio de transformação de taxas ou cálculos de juros compostos.

### Decisão
- Desenvolvemos testes unitários isolados sob `/tests/test_transformations.py` para validar a sanitização dos tipos de dados de forma offline (sem fazer requisições reais ou gravar em bancos de dados).
- Configuramos um linter estrito (`flake8`) limitando a linha de código máxima em 120 caracteres.
- Criamos um pipeline de Integração Contínua (`ci-cd-pipeline.yml`) via GitHub Actions para rodar a checagem sintática (`flake8`) e a suíte de testes unitários (`pytest`) a cada push ou Pull Request direcionado aos branches `main` e `develop`.

---

## 8. Implementação de Circuit Breaker com Exponential Backoff (Resiliência de API)

### Contexto
A API externa do Banco Central do Brasil (SGS 11) pode sofrer indisponibilidades temporárias ou rate limits. A tentativa constante e desenfreada de bater na API indisponível consome recursos e posterga falhas.

### Decisão
Implementamos no adaptador `BcbApiAdapter` da camada Bronze:
1. **Exponential Backoff**: Um loop de retentativas interno (máximo de 3 tentativas por execução) com intervalos de tempo crescentes calculados exponencialmente (`sleep_time = backoff_factor * (2 ** attempt)`).
2. **Circuit Breaker**: Uma máquina de estados em memória (`CLOSED`, `OPEN`, `HALF-OPEN`) que monitora o número de falhas consecutivas do adaptador.
   - O circuito abre (`OPEN`) após **5 falhas consecutivas**.
   - Quando o circuito está `OPEN`, qualquer nova requisição falha imediatamente (lançando `CircuitBreakerOpenError`) sem sequer tentar chamar a API externa, poupando a rede e a máquina.
   - Após um cooldown de **60 segundos**, o circuito entra em `HALF-OPEN` permitindo uma requisição de teste.

### Racional / Benefícios
- **Autopreservação e Proteção da API**: Evita sobrecarregar a API pública externa com requisições repetidas se ela estiver instável.
- **Configurabilidade para Testes**: Os parâmetros de retentativa e backoff factor são configuráveis no construtor do adaptador, permitindo que os testes unitários desliguem o delay (`backoff_factor=0.0`) para rodarem em milissegundos sem lentidão.
