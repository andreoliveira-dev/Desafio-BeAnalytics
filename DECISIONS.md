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
- **Quality Gate de Cobertura**: Integramos o `pytest-cov` no pipeline de testes unitários e estabelecemos uma restrição rígida de cobertura mínima de código de **80%** (`--cov-fail-under=80`).
- **Configuração de Exclusões**: Criamos o arquivo `.coveragerc` para desconsiderar arquivos de entrypoint (`main.py`) e diretórios de teste, evitando distorções estatísticas nos relatórios.

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

---

## 9. Testabilidade e Integridade Estrutural da DAG

### Contexto
Erros de importação no scheduler do Airflow e alterações indesejadas em propriedades críticas da DAG (ex: políticas de retry e conexões entre tasks) muitas vezes só eram detectados em tempo de execução no ambiente conteinerizado.

### Decisão
Implementamos um teste de integridade estrutural e topológica dedicado da DAG em `tests/test_dag.py`. O teste:
1. Importa o módulo da DAG diretamente no pytest.
2. Assegura que não há exceções de importação em tempo de análise.
3. Valida a estrutura da DAG (IDs de tarefas e fluxo unidirecional rígido: `ingest_bronze >> transform_silver >> aggregate_gold`).
4. Garante a configuração de parâmetros fundamentais de compliance (como `catchup=False`, `retries=2` e `retry_delay=timedelta(minutes=5)`).

### Racional / Benefícios
- **Falha Rápida (Fail-Fast)**: Erros simples de importação ou de conexões entre operators quebram a esteira de CI/CD imediatamente, antes do deploy no servidor Airflow.

---

## 10. Resolução de Compatibilidade de Dependências (Airflow vs Pendulum)

### Contexto
O Apache Airflow 2.6.3 foi projetado e homologado para a biblioteca `pendulum` na versão `2.x`. O lançamento do `pendulum==3.x` gerou incompatibilidade na inicialização do scheduler do Airflow, quebrando conversões de fuso horário (`TypeError: 'module' object is not callable` ao referenciar `pendulum.tz`).

### Decisão
- Pinamos explicitamente a dependência de compatibilidade estável `pendulum==2.1.2` no arquivo `requirements.txt`.
- Adicionamos o `pendulum==2.1.2` na variável `_PIP_ADDITIONAL_REQUIREMENTS` do `docker-compose.yaml` para impedir atualizações automáticas para versões 3.x na inicialização dos containers.

### Racional / Benefícios
- **Consistência de Ambiente**: Garante estabilidade nos fluxos locais, containers e esteira de CI/CD, eliminando quebras silenciosas por atualização de pacotes terceiros.

---

## 11. Persistência de Estado do Circuit Breaker em Banco Relacional (PostgreSQL / SQLite)

### Contexto
O Circuit Breaker padrão armazena seu estado em variáveis de classe em memória (`stateless`). Em ambientes de produção reais com Airflow (como `LocalExecutor` ou `KubernetesExecutor`), as tarefas rodam em processos isolados e efêmeros, fazendo com que o estado do Circuit Breaker seja perdido entre execuções ou entre tarefas diferentes. Para instalações on-premise, adicionar novos serviços de cache distribuído (como Redis ou NATS) acrescenta custo operacional de manutenção e riscos de compliance/licenciamento.

### Decisão
Implementamos um modelo persistente e minimalista para armazenamento do estado do Circuit Breaker:
1. **Nova Porta de Saída (`CircuitBreakerStatePort`)**: Abstrai as operações de leitura (`get_state`) e escrita (`update_state`) do estado do disjuntor.
2. **Adaptador SQL (`SqlCircuitBreakerStateAdapter`)**: Implementação concreta usando **SQLAlchemy** que cria automaticamente e gerencia a tabela `circuit_breaker_states` no banco relacional.
   - **PostgreSQL (Produção)**: Conecta-se automaticamente ao banco metastore do Airflow utilizando a conexão `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` já presente no container.
   - **SQLite (Fallback Local/Testes)**: Caso a conexão do Airflow não esteja disponível (execuções locais ou testes unitários offline), ele faz fallback automático para um arquivo SQLite local sob `data/circuit_breaker.db` (ou `:memory:` para testes isolados).
3. **Adaptador In-Memory (`InMemoryCircuitBreakerStateAdapter`)**: Mantido para testes de unidade rápidos e retrocompatibilidade de execução offline.

### Racional / Benefícios
- **Custo Operacional Zero**: Aproveita o banco de dados PostgreSQL existente que já roda como banco de metadados do Airflow, eliminando a necessidade de implantar, monitorar e licenciar serviços novos (como Redis ou NATS) em infraestruturas privadas on-premise.
- **Stateful de Verdade**: O estado do Circuit Breaker passa a ser compartilhado de forma persistente e persistente entre qualquer tarefa ou processo que o chame no ecossistema, sobrevivendo ao encerramento dos containers do Airflow.
- **Portabilidade**: O auto-criar da tabela (`metadata.create_all`) garante que o setup do banco de dados ocorra de forma transparente na primeira chamada do pipeline, sem necessidade de migrations manuais.

---

## 12. Migração de Pandas para Polars (Lazy API & Streaming)

### Contexto
O pipeline de dados original utilizava a biblioteca **Pandas** para processar e manipular dados. Pandas é focado em processamento em memória de forma eagerly (avaliação imediata) e de thread única, o que impõe limitações físicas severas de escala e performance ao lidar com volumes massivos de dados típicos de Big Data.

### Decisão
Migramos toda a lógica de manipulação e transformação de dados nas camadas Bronze, Silver e Gold para **Polars**, utilizando a **Lazy API** (`LazyFrame`) e habilitando o modo de streaming (`streaming=True`) na coleta final dos dados (`collect`).

### Racional / Benefícios
- **Otimização de Consultas (Lazy API)**: Polars constrói um grafo de execução lógica (query plan) que otimiza automaticamente operações como predicados (filter pushdown) e projeções (projection pushdown) antes de iniciar a computação, reduzindo o volume de dados carregados e processados.
- **Processamento Além da Memória (Out-of-Core/Streaming)**: Ao habilitar `streaming=True`, o Polars divide o processamento dos dados em chunks processados em paralelo de forma eficiente, permitindo que pipelines de dados rodem de forma robusta em datasets muito maiores que a RAM física disponível na máquina executora.
- **Performance Multithreaded Nativa**: Desenvolvido em Rust e utilizando Apache Arrow como formato de memória, o Polars aproveita ao máximo todos os núcleos de CPU disponíveis, superando o gargalo de thread única do Pandas.

---

## 13. Integração com Nuvem e MinIO (Object Storage S3-compatível) com Seleção Dinâmica

### Contexto
Originalmente, a persistência de dados do pipeline estava restrita ao sistema de arquivos local (`data/`). Para habilitar o pipeline a operar em ambientes de produção modernos baseados em nuvem (ou on-premise Kubernetes/Private Cloud), era preciso demonstrar suporte a Object Storage robusto compatível com a API do AWS S3.

### Decisão
- Desenvolvemos adaptadores baseados em S3 (`S3ParquetStorageAdapter`, etc.) utilizando a biblioteca `s3fs` e adicionamos suporte a buckets remotos nas leituras com Polars.
- Habilitamos seleção de armazenamento dinâmico via variável de ambiente `STORAGE_TYPE` (configurando caminhos `s3://` versus caminhos locais e buckets correspondentes de forma transparente).
- Fornecemos um contêiner MinIO no `docker-compose.yaml` local para simular o ambiente de nuvem de forma offline e reprodutível.

### Racional / Benefícios
- **Conformidade em Ambientes de Produção Cloud-Native**: A mesma base de código pode ser executada localmente no disco rígido do desenvolvedor (ou ambientes de testes simples) ou apontada para um bucket de produção no AWS S3, Google Cloud Storage, ou cluster MinIO on-premise apenas trocando variáveis de ambiente.
- **Resolução de Compatibilidade de Protocolo**: Configuramos parâmetros de conexão planos (`aws_access_key_id`, `aws_secret_access_key`, `endpoint_url` e `aws_region`) para alimentar diretamente o backend em Rust do Polars, assegurando performance nativa de rede na leitura/escrita de arquivos remotos sem gargalos de serialização.

---

## 14. Arquivamento de JSON Bruto (Bronze) e Particionamento Físico de Dados (Silver & Gold)

### Contexto
Para fins de governança de dados, auditoria e facilidade de depuração, é uma boa prática em engenharia de dados armazenar o payload original retornado por APIs de terceiros exatamente no formato original (JSON bruto), permitindo auditorias e reprocessamentos idênticos. Além disso, no carregamento de grandes volumes de dados (Big Data), a ausência de particionamento físico dificulta consultas downstream em subset de dados de anos/meses específicos (gerando full table scans indesejados).

### Decisão
1. **JSON Arquivamento (Bronze)**: Atualizamos os adaptadores de gravação física da camada Bronze para salvar um arquivo `.json` bruto com a lista de registros retornados da chamada SGS API, localizado exatamente na mesma pasta do arquivo parquet unificado (tanto local quanto em S3/MinIO).
2. **Particionamento Físico (Silver & Gold)**: Implementamos nos adaptadores de escrita da camada Silver e Gold uma lógica dinâmica para gravar os dados de forma particionada no estilo Hive (`partitioned/year=YYYY/month=MM/data.parquet`).
   - Para retrocompatibilidade e integridade da DAG e suíte de testes existente, o arquivo único consolidado `.parquet` ainda é escrito e serve como entrypoint principal.
   - O particionador realiza o agrupamento (`group_by`) por ano/mês dos registros de forma otimizada e grava cada subconjunto nos respectivos caminhos de partições tanto em S3 quanto localmente.

### Racional / Benefícios
- **Linhagem e Auditabilidade**: Garante um registro imutável do exato payload retornado da API externa, de forma que qualquer divergência possa ser investigada a partir dos bits originais sem depender de nova chamada na API do Banco Central.
- **Eficiência e Prática de Data Lake**: A cópia física de partição demonstra o domínio de arquitetura de Data Lakes on-premise ou cloud, permitindo que motores de consulta eficientes (como Athena, Trino, DuckDB ou o próprio Polars/Spark) leiam somente os caminhos de partição solicitados (partition pruning), cortando drasticamente custos de I/O de disco e rede.

---

## 15. Testes de Integração com S3/MinIO no Pipeline de CI (GitHub Actions)

### Contexto
O pipeline de dados possui suporte a persistência local e no S3 (MinIO). No entanto, testes locais e unitários rodavam de forma offline mockada. Alterações em chamadas de rede do boto3, autenticação ou especificidades do s3fs e do driver Rust do Polars ao ler de caminhos remotos podiam introduzir regressões invisíveis nos testes offline.

### Decisão
1. **MinIO Efêmero no GitHub Actions**: Adicionamos o serviço `minio/minio:latest` ao workflow `.github/workflows/ci-cd-pipeline.yml` para subir um servidor de Object Storage S3 compatível durante a execução das esteiras de CI.
2. **Suíte de Integração S3 Dedicada**: Criamos o arquivo `tests/test_s3_integration.py` que executa testes ponta a ponta simulando as três camadas (Bronze, Silver e Gold) gravando e lendo do MinIO real via s3fs e Polars.
3. **Execução Condicional Eficiente**: Os testes de S3 utilizam uma marcação de skip automático caso a variável `S3_ENDPOINT_URL` não esteja acessível (evitando quebras no pytest rodado localmente offline pelos desenvolvedores), mas rodam obrigatoriamente na esteira de CI que configura essas variáveis no ambiente.

### Racional / Benefícios
- **Validação Efetiva de Infraestrutura Cloud-Native**: Assegura que a compatibilidade entre a biblioteca s3fs de Python, o cliente boto3 e o motor Rust do Polars está saudável e consegue interagir de forma real com a API S3, elevando a confiança antes do merge em produção.
- **Desenvolvimento Local Preservado**: A verificação de acessibilidade da porta do MinIO faz com que os testes de integração se adaptem ao ambiente do desenvolvedor sem impor a obrigação de ter um container MinIO ativo na máquina pessoal para rodar a suíte local.
