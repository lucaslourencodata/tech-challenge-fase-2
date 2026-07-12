# Relatório FinOps — BigQuery

Gerado em UTC: `2026-07-12T21:21:25.885599+00:00`

## Resumo da infraestrutura

- Projeto: `tc-alfabetizacao-lucas`
- Região: `US`
- Datasets analisados: 3
- Tabelas analisadas: 17
- Registros armazenados: 7,885,085
- Armazenamento lógico: 741.92 MiB

## Estimativa de armazenamento mensal

- Armazenamento ativo: 0.724530 GiB
- Armazenamento de longo prazo: 0.000000 GiB
- Franquia considerada: 10.00 GiB
- Estimativa mensal após a franquia: US$ 0.000000

## Histórico de consultas — últimos 30 dias

- Consultas concluídas: 16
- Consultas atendidas por cache: 0
- Bytes processados: 1.00 GiB
- Bytes faturáveis: 1.08 GiB
- Uso em TiB: 0.00105667
- Estimativa após a franquia: US$ 0.000000

## Dry run das consultas analíticas

| Consulta | Dados estimados |
|---|---:|
| taxa_media_por_uf_ano | 468.65 KiB |
| status_meta_por_uf | 99.46 KiB |
| tendencia_por_uf | 238.73 KiB |

## Cenário mensal de dashboard

- Execuções mensais de cada consulta: 100
- Dados estimados no mês: 78.79 MiB
- Uso estimado: 0.00007514 TiB
- Estimativa após a franquia: US$ 0.000000

## Premissas de preço

- Consulta sob demanda: US$ 6.2500 por TiB
- Armazenamento lógico ativo: US$ 0.0200 por GiB/mês
- Armazenamento de longo prazo: US$ 0.0100 por GiB/mês
- Os valores apresentados são estimativas técnicas, não uma fatura oficial.

## Recomendações

- Avaliar particionamento nas tabelas maiores: tc_bronze.alunos, tc_silver.alunos. A tabela de alunos pode ser particionada por ano.
- O armazenamento atual está dentro dos 10 GiB considerados na faixa gratuita.
- O uso de consultas dos últimos 30 dias permanece dentro de 1 TiB.
- O cenário projetado do dashboard permanece dentro da faixa gratuita de consultas.
- Existem datasets com expiração automática igual ou inferior a 60 dias.
- Utilizar dry run antes de consultas para estimar os bytes processados.
- Evitar SELECT * e selecionar apenas as colunas necessárias.
- Definir maximum_bytes_billed para limitar consultas inesperadamente caras.
- Manter arquivos Parquet com compressão ZSTD e organização por ano.
- Consultar preferencialmente a camada Gold, evitando varreduras recorrentes dos microdados.