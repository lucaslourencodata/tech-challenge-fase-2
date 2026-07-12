# Roteiro de Apresentação — Tech Challenge Fase 2

## Duração estimada

Entre 4 minutos e 30 segundos e 5 minutos.

---

## 0:00–0:25 — Apresentação do projeto

### Mostrar na tela

- Página inicial do repositório no GitHub;
- título e resumo do `README.md`.

### Falar

Olá, meu nome é Lucas Lourenço e este é o projeto desenvolvido para o Tech Challenge da Fase 2.

O objetivo foi construir uma pipeline de dados híbrida para analisar indicadores de alfabetização no Brasil, combinando processamento em lote, simulação de streaming, arquitetura Medallion, qualidade de dados, armazenamento em nuvem, observabilidade e FinOps.

---

## 0:25–0:55 — Fonte de dados e objetivo analítico

### Mostrar na tela

No `README.md`, mostrar:

- fonte dos dados;
- tabelas utilizadas;
- objetivo do projeto.

### Falar

Os dados utilizados são públicos e foram obtidos do projeto de avaliação de alfabetização da Base dos Dados, disponível no BigQuery.

Foram utilizadas tabelas de alunos, municípios, unidades federativas e metas de alfabetização do Brasil, dos estados e dos municípios.

Os microdados de alunos analisados contemplam os anos de 2023 e 2024, totalizando aproximadamente 3,8 milhões de registros.

A solução permite analisar indicadores municipais, comparar resultados com metas e verificar a evolução da alfabetização entre os dois anos.

---

## 0:55–1:35 — Arquitetura da solução

### Mostrar na tela

Abrir:

```text
docs/arquitetura.md
```

Mostrar o diagrama Mermaid da arquitetura.

### Falar

A solução utiliza a arquitetura Medallion, dividida nas camadas Bronze, Silver e Gold.

Na camada Bronze, os dados são preservados com mínima transformação.

Na Silver, os registros são limpos, tipados, padronizados e validados.

Na Gold, são criadas tabelas analíticas prontas para consultas e dashboards.

A arquitetura possui dois fluxos. O fluxo batch realiza a extração e transformação dos dados históricos. Já o fluxo de streaming simula a chegada contínua de atualizações dos indicadores.

As três camadas também foram carregadas no Google BigQuery.

---

## 1:35–2:10 — Ingestão e transformação

### Mostrar na tela

No VS Code, abrir a pasta:

```text
src/ingestion
```

Mostrar rapidamente:

```text
ingestao_batch.py
ingestao_alunos.py
```

Depois abrir:

```text
src/transformation
```

### Falar

A ingestão das tabelas menores é feita por um script batch específico.

Como a tabela de alunos possui milhões de registros, ela é extraída por ano e em blocos, evitando o carregamento integral dos dados na memória.

Os dados são armazenados localmente em formato Parquet e organizados por ano e data de ingestão.

Na transformação, são realizadas conversões de tipos, padronização de textos, tratamento de valores nulos, remoção de duplicidades e inclusão de metadados de processamento.

---

## 2:10–2:45 — Camada Gold e resultados analíticos

### Mostrar na tela

Abrir:

```text
src/transformation/criar_camada_gold.py
```

Depois mostrar no README as quatro tabelas Gold.

### Falar

Na camada Gold foram produzidas quatro tabelas principais.

A primeira contém os indicadores de alfabetização por município, ano e rede de ensino.

A segunda contém as metas municipais.

A terceira compara o resultado observado com a meta esperada e classifica o cumprimento da meta.

A quarta compara os anos de 2023 e 2024, identificando evolução, redução ou estabilidade.

Essas tabelas reduzem a necessidade de consultar diretamente os microdados e deixam a solução preparada para relatórios e dashboards.

---

## 2:45–3:15 — Qualidade dos dados

### Mostrar na tela

Abrir no terminal ou no README o resultado:

```text
26 verificações
25 aprovadas
1 alerta
0 falhas
```

Mostrar os arquivos:

```text
docs/relatorio_qualidade_silver.csv
docs/relatorio_qualidade_gold.csv
```

### Falar

A pipeline possui validações nas camadas Silver e Gold.

São verificadas duplicidades, campos obrigatórios, integridade das chaves, anos esperados, códigos de rede, percentuais válidos, reconciliação entre camadas e consistência entre resultados e metas.

Na camada Gold foram executadas 26 verificações, com 25 aprovações, um alerta e nenhuma falha.

O alerta representa combinações de município e rede que não possuem dados nos dois anos necessários para a comparação temporal.

---

## 3:15–3:45 — Streaming

### Mostrar na tela

Abrir:

```text
src/streaming/produtor_eventos.py
src/streaming/consumidor_eventos.py
```

Opcionalmente, mostrar o terminal executando:

```powershell
py src/streaming/produtor_eventos.py --quantidade 10 --intervalo 0
py src/streaming/consumidor_eventos.py
```

### Falar

O componente de streaming simula atualizações contínuas dos indicadores.

O produtor gera eventos JSONL a partir de registros da camada Gold.

O consumidor valida o formato, os campos obrigatórios, os tipos, os percentuais e os códigos de rede.

Também foi implementado controle de duplicidade e idempotência. Dessa forma, executar o consumidor novamente não duplica eventos já processados.

---

## 3:45–4:15 — Google Cloud e BigQuery

### Mostrar na tela

Abrir o Console do BigQuery e mostrar os datasets:

```text
tc_bronze
tc_silver
tc_gold
```

Mostrar rapidamente algumas tabelas de cada dataset.

### Falar

A infraestrutura em nuvem foi implementada no Google Cloud Platform.

Foram criados três datasets no BigQuery, representando as camadas Bronze, Silver e Gold.

Os scripts de carga enviam os arquivos locais para o BigQuery e realizam a reconciliação entre a quantidade de registros locais e a quantidade armazenada na nuvem.

No total, a análise de FinOps identificou 17 tabelas e aproximadamente 7,8 milhões de registros armazenados no BigQuery.

---

## 4:15–4:40 — Observabilidade e FinOps

### Mostrar na tela

Mostrar a execução monitorada:

```powershell
py src/monitoring/executar_pipeline_monitorada.py --perfil qualidade
```

Depois abrir:

```text
docs/relatorio_finops_bigquery.md
```

### Falar

A solução também possui observabilidade.

Cada execução registra identificador, duração, status, quantidade de arquivos, registros, volume armazenado e logs separados por etapa.

Na análise de FinOps, foram avaliados armazenamento, histórico de consultas e dry runs.

O ambiente possui aproximadamente 742 MiB armazenados e o cenário mensal de dashboard processaria cerca de 79 MiB.

Dentro das premissas e franquias consideradas, o custo estimado foi de zero dólar.

---

## 4:40–4:58 — Versionamento e conclusão

### Mostrar na tela

No GitHub, mostrar:

- histórico de commits;
- branches `feature`;
- estrutura do repositório.

### Falar

O desenvolvimento foi organizado em branches por funcionalidade, com commits separados para ingestão, transformação, qualidade, streaming, nuvem, observabilidade, FinOps e documentação.

Como resultado, foi construída uma pipeline completa, rastreável, escalável e preparada para análises de alfabetização no Brasil.

Obrigado.

---

# Checklist antes de gravar

- Fechar abas e programas desnecessários;
- aumentar o zoom do navegador e do VS Code;
- ocultar notificações do Windows;
- confirmar que nenhuma credencial está visível;
- deixar o GitHub, BigQuery e VS Code já abertos;
- testar o microfone;
- gravar em resolução de pelo menos 1080p;
- manter o vídeo abaixo de 5 minutos;
- não executar novamente a carga completa durante a gravação;
- mostrar apenas comandos rápidos e resultados já gerados.

# Ordem das telas

1. GitHub e README;
2. fonte dos dados;
3. diagrama da arquitetura;
4. scripts de ingestão e transformação;
5. tabelas Gold;
6. relatórios de qualidade;
7. scripts de streaming;
8. datasets no BigQuery;
9. monitoramento;
10. relatório FinOps;
11. branches e commits no GitHub.