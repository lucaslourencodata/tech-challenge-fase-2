# Arquitetura da Solução

## 1. Visão geral

Este projeto implementa uma arquitetura de dados híbrida, combinando processamento em lote e processamento de eventos em streaming para análise dos indicadores de alfabetização no Brasil.

A solução utiliza a arquitetura Medallion, dividida nas camadas:

- Bronze: dados brutos, preservados com mínima transformação;
- Silver: dados limpos, padronizados, validados e preparados para análise;
- Gold: dados agregados e orientados aos indicadores de negócio.

A infraestrutura em nuvem utiliza o Google Cloud Platform, com armazenamento analítico no BigQuery.

---

## 2. Diagrama da arquitetura

```mermaid
flowchart LR
    A[Base dos Dados<br>BigQuery Público] --> B[Ingestão Batch]
    B --> C[Bronze Local<br>Parquet]
    C --> D[Transformações e Limpeza]
    D --> E[Silver Local<br>Parquet]
    E --> F[Validações de Qualidade]
    F --> G[Agregações Analíticas]
    G --> H[Gold Local<br>Parquet]

    H --> I[Produtor de Eventos]
    I --> J[Eventos JSONL<br>Streaming Bronze]
    J --> K[Consumidor de Eventos]
    K --> L[Streaming Silver<br>Parquet]

    C --> M[BigQuery tc_bronze]
    E --> N[BigQuery tc_silver]
    H --> O[BigQuery tc_gold]
    J --> M

    F --> P[Relatórios de Qualidade]
    O --> Q[FinOps e Dry Run]
    B --> R[Monitoramento da Pipeline]
    D --> R
    F --> R
    G --> R