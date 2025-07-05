# Case Técnico Data Architect - iFood

## Objetivo

Este projeto tem como finalidade atender à proposta do case técnico para a vaga de **Data Architect** no iFood. A solução abrange as etapas de ingestão, modelagem, transformação, disponibilização e análise de dados relacionados às corridas de táxis da cidade de Nova York.

## Requisitos do Desafio

- Realizar a ingestão dos dados brutos de táxis de NY (TLC) no Data Lake.
- Estruturar os dados em camadas: `landing`, `bronze`, `silver` e `gold`.
- Disponibilizar os dados para consumo via SQL e Delta Lake.
- Implementar validações de qualidade, deduplicidade e conformidade de schema.
- Responder a perguntas analíticas com PySpark ou SQL.

## Fontes de Dados

Todos os dados utilizados foram obtidos no portal oficial:
[NYC Taxi & Limousine Commission - TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

Neste projeto, consideraram-se os meses de **Janeiro a Maio de 2023**.

## Arquitetura do Projeto

A solução está dividida em módulos estruturados da seguinte forma:

```
.
├── infrastructure                # IaC com Terraform para buckets e infraestrutura
├── pipe                         # Códigos de ingestão e transformação
│   ├── ingest                   # Ingestão de dados da TLC para S3 (landing)
│   └── layers                   # Pipelines Spark para bronze, silver e gold
│       ├── config              # Configurações do Spark e dependências
│       ├── logs                # Logs e eventos do Spark
│       └── src
│           ├── schema         # Definição de schemas PySpark (StructType)
│           ├── transform      # Funções de limpeza, hash, SCD2, etc.
│           ├── utils          # Controle de schema, logger, otimização Delta
│           ├── trips_landing_to_bronze.py
│           ├── trips_bronze_to_silver.py
│           ├── trips_amout_silver_to_gold.py
│           └── validation_results.txt
└── readme.md
```

## Funcionalidades Principais

- **Ingestão Paralela e Sequencial** dos arquivos TLC (Parquet) com boto3 e PySpark.
- **Controle de Schema**: coercção, validação e aplicação via `schema_control.py`.
- **Validação de Qualidade**: nulos, tipos, deduplicidade, padronização de datas.
- **SCD Tipo 2 com Delta Lake**: histórico e atualizações por `merge.py` e `scd.py`.
- **Particionamento e Z-Ordering**: particionamento por `ano_mes`, otimização com `OPTIMIZE` e `VACUUM`.
- **Marquez + OpenLineage**: linha do tempo de execução e lineage configurado nos jobs.

## Respostas Analíticas

Arquivo: `pipe/layers/src/validation_results.txt`

1. **Média de valor total (total_amount) por mês - Yellow Taxis**
2. **Média de passageiros por hora no mês de maio - Todos os táxis**

> Todas as análises foram realizadas com PySpark e salvas em tabela Delta na camada `gold`.

## Como Executar

### Pré-requisitos:
- Docker + Docker Compose
- AWS CLI configurado (caso use S3)
- Python 3.10+

### Subir ambiente Spark:

```bash
bash download_jars.sh
cd pipe/layers
bash running_cluster.sh
```

### Executar pipeline completo:

```bash
# Ingestão landing
python pipe/ingest/main_parallel.py

# Ingestão Bronze → Silver
python pipe/layers/src/trips_landing_to_bronze.py
python pipe/layers/src/trips_bronze_to_silver.py

# Construção Gold
python pipe/layers/src/trips_amout_silver_to_gold.py

# Validações
python pipe/layers/src/trips_yellow_report.py
```

## Contato

Este projeto foi desenvolvido como parte de um case técnico do processo seletivo para a vaga de **Data Architect - iFood**.

Em caso de dúvidas, sugestões ou colaborações, fique à vontade para entrar em contato.
