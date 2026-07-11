# Tech Challenge – Fase 2

## Pipeline Híbrido para Análise da Alfabetização no Brasil

Projeto desenvolvido como parte do Tech Challenge da pós-graduação em Ciência de Dados e Inteligência Artificial da FIAP.

## Objetivo

Construir uma pipeline de dados híbrida, com processamento em lote e simulação de streaming, para integrar, tratar e disponibilizar dados relacionados à alfabetização infantil no Brasil.

A solução seguirá a Arquitetura Medalhão:

* **Bronze:** armazenamento dos dados brutos;
* **Silver:** limpeza, padronização e integração dos dados;
* **Gold:** criação de dados analíticos prontos para consultas, dashboards e modelos de inteligência artificial.

## Tecnologias iniciais

* Python
* Pandas
* PyArrow
* Parquet
* Git e GitHub

## Estrutura do projeto

```text
data/
├── bronze/
├── silver/
└── gold/

src/
├── ingestion/
├── transformation/
├── quality/
└── streaming/

docs/
notebooks/
tests/
config/
```

## Status

Projeto em desenvolvimento.
