import argparse
import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from google.api_core.exceptions import Forbidden, GoogleAPIError
from google.cloud import bigquery


PROJECT_ID = "tc-alfabetizacao-lucas"
LOCATION = "US"
REGION_QUALIFIER = "region-us"

DATASETS = [
    "tc_bronze",
    "tc_silver",
    "tc_gold",
]

PROJECT_ROOT = Path(__file__).resolve().parents[2]

REPORT_CSV_PATH = (
    PROJECT_ROOT
    / "docs"
    / "relatorio_finops_bigquery.csv"
)

REPORT_MD_PATH = (
    PROJECT_ROOT
    / "docs"
    / "relatorio_finops_bigquery.md"
)

GIB = 1024 ** 3
TIB = 1024 ** 4

# Franquias mensais consideradas na estimativa.
FREE_STORAGE_GIB = 10.0
FREE_QUERY_TIB = 1.0

# Valores de referência em dólar.
DEFAULT_QUERY_PRICE_PER_TIB_USD = 6.25
DEFAULT_ACTIVE_STORAGE_GIB_MONTH_USD = 0.02
DEFAULT_LONG_TERM_STORAGE_GIB_MONTH_USD = 0.01


CONSULTAS_DRY_RUN = {
    "taxa_media_por_uf_ano": f"""
        SELECT
            ano,
            id_uf,
            AVG(taxa_alfabetizacao) AS taxa_media
        FROM
            `{PROJECT_ID}.tc_gold.indicador_alfabetizacao_municipio`
        GROUP BY
            ano,
            id_uf
    """,
    "status_meta_por_uf": f"""
        SELECT
            id_uf,
            status_meta,
            COUNT(*) AS quantidade
        FROM
            `{PROJECT_ID}.tc_gold.comparacao_meta_resultado_municipio`
        GROUP BY
            id_uf,
            status_meta
    """,
    "tendencia_por_uf": f"""
        SELECT
            id_uf,
            tendencia,
            COUNT(*) AS quantidade
        FROM
            `{PROJECT_ID}.tc_gold.evolucao_alfabetizacao_municipio`
        GROUP BY
            id_uf,
            tendencia
    """,
}


def bytes_para_gib(valor: int) -> float:
    """Converte bytes para GiB."""

    return valor / GIB


def bytes_para_tib(valor: int) -> float:
    """Converte bytes para TiB."""

    return valor / TIB


def formatar_bytes(valor: int) -> str:
    """Formata bytes em uma unidade legível."""

    unidades = [
        "B",
        "KiB",
        "MiB",
        "GiB",
        "TiB",
    ]

    quantidade = float(valor)

    for unidade in unidades:
        if quantidade < 1024 or unidade == unidades[-1]:
            return f"{quantidade:.2f} {unidade}"

        quantidade /= 1024

    return f"{valor} B"


def obter_expiracao_dataset(
    client: bigquery.Client,
    dataset_id: str,
) -> float | None:
    """Obtém a expiração padrão das tabelas em dias."""

    dataset_completo = f"{PROJECT_ID}.{dataset_id}"

    dataset = client.get_dataset(
        dataset_completo
    )

    expiracao_ms = (
        dataset.default_table_expiration_ms
    )

    if expiracao_ms is None:
        return None

    segundos = expiracao_ms / 1000
    dias = segundos / 86400

    return round(dias, 2)


def coletar_tabelas(
    client: bigquery.Client,
) -> list[dict[str, Any]]:
    """Coleta métricas das tabelas das três camadas."""

    resultados: list[dict[str, Any]] = []

    for dataset_id in DATASETS:
        dataset_completo = (
            f"{PROJECT_ID}.{dataset_id}"
        )

        expiracao_padrao_dias = (
            obter_expiracao_dataset(
                client=client,
                dataset_id=dataset_id,
            )
        )

        tabelas = list(
            client.list_tables(
                dataset_completo
            )
        )

        for tabela_resumida in tabelas:
            tabela_completa = client.get_table(
                tabela_resumida.reference
            )

            bytes_totais = int(
                tabela_completa.num_bytes or 0
            )

            # A classe Table da biblioteca não fornece
            # num_long_term_bytes. Como os dados deste projeto
            # foram carregados recentemente, todo o volume é
            # considerado armazenamento ativo.
            bytes_longo_prazo = 0

            bytes_ativos = max(
                0,
                bytes_totais - bytes_longo_prazo,
            )

            particionada = bool(
                tabela_completa.time_partitioning
                or tabela_completa.range_partitioning
            )

            clusterizacao = ", ".join(
                tabela_completa.clustering_fields
                or []
            )

            expiracao_tabela = (
                tabela_completa.expires.isoformat()
                if tabela_completa.expires
                else ""
            )

            resultados.append(
                {
                    "dataset": dataset_id,
                    "tabela": tabela_completa.table_id,
                    "tipo": tabela_completa.table_type,
                    "linhas": int(
                        tabela_completa.num_rows or 0
                    ),
                    "bytes_totais": bytes_totais,
                    "bytes_ativos": bytes_ativos,
                    "bytes_longo_prazo": bytes_longo_prazo,
                    "tamanho_legivel": formatar_bytes(
                        bytes_totais
                    ),
                    "particionada": particionada,
                    "clusterizacao": (
                        clusterizacao
                        if clusterizacao
                        else "Não"
                    ),
                    "expiracao_tabela": expiracao_tabela,
                    "expiracao_padrao_dataset_dias": (
                        expiracao_padrao_dias
                    ),
                }
            )

    return resultados


def consultar_historico_queries(
    client: bigquery.Client,
) -> dict[str, Any]:
    """Consulta a utilização de queries dos últimos 30 dias."""

    consulta = f"""
        SELECT
            COUNT(*) AS quantidade_queries,

            COUNTIF(
                cache_hit = TRUE
            ) AS quantidade_cache_hits,

            COALESCE(
                SUM(total_bytes_processed),
                0
            ) AS bytes_processados,

            COALESCE(
                SUM(total_bytes_billed),
                0
            ) AS bytes_faturados

        FROM
            `{REGION_QUALIFIER}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT

        WHERE
            creation_time >= TIMESTAMP_SUB(
                CURRENT_TIMESTAMP(),
                INTERVAL 30 DAY
            )

            AND job_type = 'QUERY'
            AND state = 'DONE'
            AND error_result IS NULL

            AND (
                statement_type IS NULL
                OR statement_type <> 'SCRIPT'
            )
    """

    try:
        linhas = list(
            client.query(
                consulta,
                location=LOCATION,
            ).result()
        )

        if not linhas:
            return {
                "disponivel": True,
                "quantidade_queries": 0,
                "quantidade_cache_hits": 0,
                "bytes_processados": 0,
                "bytes_faturados": 0,
                "erro": "",
            }

        resultado = linhas[0]

        return {
            "disponivel": True,
            "quantidade_queries": int(
                resultado.quantidade_queries or 0
            ),
            "quantidade_cache_hits": int(
                resultado.quantidade_cache_hits or 0
            ),
            "bytes_processados": int(
                resultado.bytes_processados or 0
            ),
            "bytes_faturados": int(
                resultado.bytes_faturados or 0
            ),
            "erro": "",
        }

    except (Forbidden, GoogleAPIError) as erro:
        return {
            "disponivel": False,
            "quantidade_queries": 0,
            "quantidade_cache_hits": 0,
            "bytes_processados": 0,
            "bytes_faturados": 0,
            "erro": str(erro),
        }


def executar_dry_runs(
    client: bigquery.Client,
) -> list[dict[str, Any]]:
    """Estima os bytes das consultas sem executá-las."""

    resultados: list[dict[str, Any]] = []

    for nome_consulta, consulta in (
        CONSULTAS_DRY_RUN.items()
    ):
        configuracao = bigquery.QueryJobConfig(
            dry_run=True,
            use_query_cache=False,
        )

        job = client.query(
            consulta,
            job_config=configuracao,
            location=LOCATION,
        )

        bytes_estimados = int(
            job.total_bytes_processed or 0
        )

        resultados.append(
            {
                "consulta": nome_consulta,
                "bytes_estimados": bytes_estimados,
                "tamanho_estimado": formatar_bytes(
                    bytes_estimados
                ),
            }
        )

    return resultados


def calcular_custo_armazenamento(
    tabelas: list[dict[str, Any]],
    preco_ativo: float,
    preco_longo_prazo: float,
) -> dict[str, float]:
    """Calcula a estimativa mensal de armazenamento."""

    bytes_ativos = sum(
        int(tabela["bytes_ativos"])
        for tabela in tabelas
    )

    bytes_longo_prazo = sum(
        int(tabela["bytes_longo_prazo"])
        for tabela in tabelas
    )

    gib_ativos = bytes_para_gib(
        bytes_ativos
    )

    gib_longo_prazo = bytes_para_gib(
        bytes_longo_prazo
    )

    gratuito_restante = FREE_STORAGE_GIB

    gib_ativos_cobraveis = max(
        0.0,
        gib_ativos - gratuito_restante,
    )

    gratuito_restante = max(
        0.0,
        gratuito_restante - gib_ativos,
    )

    gib_longo_prazo_cobraveis = max(
        0.0,
        gib_longo_prazo - gratuito_restante,
    )

    custo_ativo = (
        gib_ativos_cobraveis
        * preco_ativo
    )

    custo_longo_prazo = (
        gib_longo_prazo_cobraveis
        * preco_longo_prazo
    )

    return {
        "gib_ativos": gib_ativos,
        "gib_longo_prazo": gib_longo_prazo,
        "gib_totais": (
            gib_ativos
            + gib_longo_prazo
        ),
        "gib_ativos_cobraveis": (
            gib_ativos_cobraveis
        ),
        "gib_longo_prazo_cobraveis": (
            gib_longo_prazo_cobraveis
        ),
        "custo_ativo_usd": custo_ativo,
        "custo_longo_prazo_usd": (
            custo_longo_prazo
        ),
        "custo_total_usd": (
            custo_ativo
            + custo_longo_prazo
        ),
    }


def calcular_custo_queries(
    bytes_mensais: int,
    preco_por_tib: float,
) -> dict[str, float]:
    """Calcula a estimativa mensal de consultas."""

    tib_mensais = bytes_para_tib(
        bytes_mensais
    )

    tib_cobraveis = max(
        0.0,
        tib_mensais - FREE_QUERY_TIB,
    )

    custo_usd = (
        tib_cobraveis
        * preco_por_tib
    )

    return {
        "tib_mensais": tib_mensais,
        "tib_cobraveis": tib_cobraveis,
        "custo_total_usd": custo_usd,
    }


def calcular_cenario_dashboard(
    dry_runs: list[dict[str, Any]],
    execucoes_por_consulta: int,
    preco_por_tib: float,
) -> dict[str, float]:
    """Projeta o uso mensal das consultas do dashboard."""

    bytes_por_ciclo = sum(
        int(resultado["bytes_estimados"])
        for resultado in dry_runs
    )

    bytes_mensais = (
        bytes_por_ciclo
        * execucoes_por_consulta
    )

    custo = calcular_custo_queries(
        bytes_mensais=bytes_mensais,
        preco_por_tib=preco_por_tib,
    )

    return {
        "bytes_por_ciclo": bytes_por_ciclo,
        "bytes_mensais": bytes_mensais,
        "execucoes_por_consulta": (
            execucoes_por_consulta
        ),
        **custo,
    }


def gerar_recomendacoes(
    tabelas: list[dict[str, Any]],
    historico: dict[str, Any],
    armazenamento: dict[str, float],
    cenario_dashboard: dict[str, float],
) -> list[str]:
    """Gera recomendações automáticas de FinOps."""

    recomendacoes: list[str] = []

    tabelas_grandes_nao_particionadas = [
        tabela
        for tabela in tabelas
        if (
            int(tabela["bytes_totais"])
            >= 50 * 1024 * 1024
            and not bool(tabela["particionada"])
        )
    ]

    if tabelas_grandes_nao_particionadas:
        nomes = ", ".join(
            (
                f"{tabela['dataset']}."
                f"{tabela['tabela']}"
            )
            for tabela in tabelas_grandes_nao_particionadas
        )

        recomendacoes.append(
            "Avaliar particionamento nas tabelas maiores: "
            f"{nomes}. A tabela de alunos pode ser "
            "particionada por ano."
        )

    tabelas_grandes_sem_cluster = [
        tabela
        for tabela in tabelas
        if (
            int(tabela["bytes_totais"])
            >= 10 * 1024 * 1024
            and tabela["clusterizacao"] == "Não"
        )
    ]

    if tabelas_grandes_sem_cluster:
        nomes = ", ".join(
            (
                f"{tabela['dataset']}."
                f"{tabela['tabela']}"
            )
            for tabela in tabelas_grandes_sem_cluster
        )

        recomendacoes.append(
            "Avaliar clusterização nas tabelas: "
            f"{nomes}."
        )

    if (
        armazenamento["gib_totais"]
        <= FREE_STORAGE_GIB
    ):
        recomendacoes.append(
            "O armazenamento atual está dentro dos "
            "10 GiB considerados na faixa gratuita."
        )
    else:
        recomendacoes.append(
            "O armazenamento ultrapassa a faixa gratuita. "
            "Avalie expiração de tabelas e remoção de versões antigas."
        )

    if historico["disponivel"]:
        uso_historico_tib = bytes_para_tib(
            int(historico["bytes_faturados"])
        )

        if uso_historico_tib <= FREE_QUERY_TIB:
            recomendacoes.append(
                "O uso de consultas dos últimos 30 dias "
                "permanece dentro de 1 TiB."
            )
        else:
            recomendacoes.append(
                "As consultas ultrapassaram 1 TiB nos últimos "
                "30 dias. Configure cotas e maximum_bytes_billed."
            )
    else:
        recomendacoes.append(
            "Não foi possível consultar o histórico de jobs. "
            "Verifique as permissões do projeto."
        )

    if (
        cenario_dashboard["tib_mensais"]
        <= FREE_QUERY_TIB
    ):
        recomendacoes.append(
            "O cenário projetado do dashboard permanece "
            "dentro da faixa gratuita de consultas."
        )
    else:
        recomendacoes.append(
            "O cenário projetado ultrapassa a faixa gratuita. "
            "Reduza atualizações ou materialize agregações."
        )

    if any(
        tabela["expiracao_padrao_dataset_dias"]
        is not None
        and float(
            tabela["expiracao_padrao_dataset_dias"]
        ) <= 60
        for tabela in tabelas
    ):
        recomendacoes.append(
            "Existem datasets com expiração automática "
            "igual ou inferior a 60 dias."
        )

    recomendacoes.extend(
        [
            (
                "Utilizar dry run antes de consultas "
                "para estimar os bytes processados."
            ),
            (
                "Evitar SELECT * e selecionar apenas "
                "as colunas necessárias."
            ),
            (
                "Definir maximum_bytes_billed para limitar "
                "consultas inesperadamente caras."
            ),
            (
                "Manter arquivos Parquet com compressão ZSTD "
                "e organização por ano."
            ),
            (
                "Consultar preferencialmente a camada Gold, "
                "evitando varreduras recorrentes dos microdados."
            ),
        ]
    )

    return recomendacoes


def salvar_relatorio_csv(
    tabelas: list[dict[str, Any]],
) -> None:
    """Salva as métricas das tabelas em CSV."""

    REPORT_CSV_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    campos = [
        "dataset",
        "tabela",
        "tipo",
        "linhas",
        "bytes_totais",
        "bytes_ativos",
        "bytes_longo_prazo",
        "tamanho_legivel",
        "particionada",
        "clusterizacao",
        "expiracao_tabela",
        "expiracao_padrao_dataset_dias",
    ]

    with REPORT_CSV_PATH.open(
        mode="w",
        encoding="utf-8-sig",
        newline="",
    ) as arquivo:
        escritor = csv.DictWriter(
            arquivo,
            fieldnames=campos,
        )

        escritor.writeheader()
        escritor.writerows(tabelas)


def salvar_relatorio_markdown(
    tabelas: list[dict[str, Any]],
    historico: dict[str, Any],
    dry_runs: list[dict[str, Any]],
    armazenamento: dict[str, float],
    custo_historico: dict[str, float],
    cenario_dashboard: dict[str, float],
    recomendacoes: list[str],
    argumentos: argparse.Namespace,
) -> None:
    """Gera o relatório executivo em Markdown."""

    momento = datetime.now(
        timezone.utc
    ).isoformat()

    total_linhas = sum(
        int(tabela["linhas"])
        for tabela in tabelas
    )

    total_bytes = sum(
        int(tabela["bytes_totais"])
        for tabela in tabelas
    )

    linhas: list[str] = [
        "# Relatório FinOps — BigQuery",
        "",
        f"Gerado em UTC: `{momento}`",
        "",
        "## Resumo da infraestrutura",
        "",
        f"- Projeto: `{PROJECT_ID}`",
        f"- Região: `{LOCATION}`",
        f"- Datasets analisados: {len(DATASETS)}",
        f"- Tabelas analisadas: {len(tabelas)}",
        f"- Registros armazenados: {total_linhas:,}",
        f"- Armazenamento lógico: {formatar_bytes(total_bytes)}",
        "",
        "## Estimativa de armazenamento mensal",
        "",
        (
            f"- Armazenamento ativo: "
            f"{armazenamento['gib_ativos']:.6f} GiB"
        ),
        (
            f"- Armazenamento de longo prazo: "
            f"{armazenamento['gib_longo_prazo']:.6f} GiB"
        ),
        (
            f"- Franquia considerada: "
            f"{FREE_STORAGE_GIB:.2f} GiB"
        ),
        (
            f"- Estimativa mensal após a franquia: "
            f"US$ {armazenamento['custo_total_usd']:.6f}"
        ),
        "",
        "## Histórico de consultas — últimos 30 dias",
        "",
    ]

    if historico["disponivel"]:
        linhas.extend(
            [
                (
                    f"- Consultas concluídas: "
                    f"{historico['quantidade_queries']:,}"
                ),
                (
                    f"- Consultas atendidas por cache: "
                    f"{historico['quantidade_cache_hits']:,}"
                ),
                (
                    f"- Bytes processados: "
                    f"{formatar_bytes(historico['bytes_processados'])}"
                ),
                (
                    f"- Bytes faturáveis: "
                    f"{formatar_bytes(historico['bytes_faturados'])}"
                ),
                (
                    f"- Uso em TiB: "
                    f"{custo_historico['tib_mensais']:.8f}"
                ),
                (
                    f"- Estimativa após a franquia: "
                    f"US$ {custo_historico['custo_total_usd']:.6f}"
                ),
            ]
        )
    else:
        linhas.extend(
            [
                "- Histórico indisponível.",
                f"- Motivo: `{historico['erro']}`",
            ]
        )

    linhas.extend(
        [
            "",
            "## Dry run das consultas analíticas",
            "",
            "| Consulta | Dados estimados |",
            "|---|---:|",
        ]
    )

    for resultado in dry_runs:
        linhas.append(
            "| "
            f"{resultado['consulta']} | "
            f"{resultado['tamanho_estimado']} |"
        )

    linhas.extend(
        [
            "",
            "## Cenário mensal de dashboard",
            "",
            (
                f"- Execuções mensais de cada consulta: "
                f"{argumentos.execucoes_dashboard}"
            ),
            (
                f"- Dados estimados no mês: "
                f"{formatar_bytes(cenario_dashboard['bytes_mensais'])}"
            ),
            (
                f"- Uso estimado: "
                f"{cenario_dashboard['tib_mensais']:.8f} TiB"
            ),
            (
                f"- Estimativa após a franquia: "
                f"US$ {cenario_dashboard['custo_total_usd']:.6f}"
            ),
            "",
            "## Premissas de preço",
            "",
            (
                f"- Consulta sob demanda: "
                f"US$ {argumentos.preco_query_tib:.4f} por TiB"
            ),
            (
                f"- Armazenamento lógico ativo: "
                f"US$ {argumentos.preco_storage_ativo:.4f} "
                "por GiB/mês"
            ),
            (
                f"- Armazenamento de longo prazo: "
                f"US$ {argumentos.preco_storage_longo_prazo:.4f} "
                "por GiB/mês"
            ),
            (
                "- Os valores apresentados são estimativas "
                "técnicas, não uma fatura oficial."
            ),
            "",
            "## Recomendações",
            "",
        ]
    )

    for recomendacao in recomendacoes:
        linhas.append(
            f"- {recomendacao}"
        )

    REPORT_MD_PATH.write_text(
        "\n".join(linhas),
        encoding="utf-8",
    )


def exibir_resumo(
    tabelas: list[dict[str, Any]],
    historico: dict[str, Any],
    armazenamento: dict[str, float],
    custo_historico: dict[str, float],
    cenario_dashboard: dict[str, float],
) -> None:
    """Exibe o resumo no terminal."""

    total_bytes = sum(
        int(tabela["bytes_totais"])
        for tabela in tabelas
    )

    total_linhas = sum(
        int(tabela["linhas"])
        for tabela in tabelas
    )

    print("\n==========================================")
    print("RESUMO FINOPS — BIGQUERY")
    print("==========================================")
    print(f"Tabelas analisadas: {len(tabelas)}")
    print(f"Registros armazenados: {total_linhas:,}")

    print(
        "Armazenamento lógico: "
        f"{formatar_bytes(total_bytes)}"
    )

    print(
        "Custo mensal estimado de armazenamento: "
        f"US$ {armazenamento['custo_total_usd']:.6f}"
    )

    if historico["disponivel"]:
        print(
            "Queries nos últimos 30 dias: "
            f"{historico['quantidade_queries']:,}"
        )

        print(
            "Dados faturáveis no período: "
            f"{formatar_bytes(historico['bytes_faturados'])}"
        )

        print(
            "Custo estimado das queries históricas: "
            f"US$ {custo_historico['custo_total_usd']:.6f}"
        )
    else:
        print(
            "Histórico de consultas: indisponível"
        )

    print(
        "Cenário mensal de dashboard: "
        f"{formatar_bytes(cenario_dashboard['bytes_mensais'])}"
    )

    print(
        "Custo estimado do cenário: "
        f"US$ {cenario_dashboard['custo_total_usd']:.6f}"
    )

    print(
        f"Relatório CSV: {REPORT_CSV_PATH}"
    )

    print(
        f"Relatório Markdown: {REPORT_MD_PATH}"
    )


def obter_argumentos() -> argparse.Namespace:
    """Lê os parâmetros informados no terminal."""

    parser = argparse.ArgumentParser(
        description=(
            "Analisa a utilização e estima os custos "
            "do BigQuery para o Tech Challenge."
        )
    )

    parser.add_argument(
        "--execucoes-dashboard",
        type=int,
        default=100,
        help=(
            "Quantidade mensal de execuções de cada "
            "consulta representativa."
        ),
    )

    parser.add_argument(
        "--preco-query-tib",
        type=float,
        default=DEFAULT_QUERY_PRICE_PER_TIB_USD,
        help="Preço em dólar por TiB consultado.",
    )

    parser.add_argument(
        "--preco-storage-ativo",
        type=float,
        default=(
            DEFAULT_ACTIVE_STORAGE_GIB_MONTH_USD
        ),
        help=(
            "Preço mensal em dólar por GiB "
            "de armazenamento ativo."
        ),
    )

    parser.add_argument(
        "--preco-storage-longo-prazo",
        type=float,
        default=(
            DEFAULT_LONG_TERM_STORAGE_GIB_MONTH_USD
        ),
        help=(
            "Preço mensal em dólar por GiB "
            "de armazenamento de longo prazo."
        ),
    )

    return parser.parse_args()


def main() -> None:
    """Executa a análise de FinOps."""

    argumentos = obter_argumentos()

    if argumentos.execucoes_dashboard < 0:
        raise ValueError(
            "O número de execuções não pode ser negativo."
        )

    print("\nIniciando análise FinOps do BigQuery")
    print(f"Projeto: {PROJECT_ID}")
    print(f"Região: {LOCATION}")

    client = bigquery.Client(
        project=PROJECT_ID,
        location=LOCATION,
    )

    tabelas = coletar_tabelas(
        client=client
    )

    historico = consultar_historico_queries(
        client=client
    )

    dry_runs = executar_dry_runs(
        client=client
    )

    armazenamento = calcular_custo_armazenamento(
        tabelas=tabelas,
        preco_ativo=argumentos.preco_storage_ativo,
        preco_longo_prazo=(
            argumentos.preco_storage_longo_prazo
        ),
    )

    custo_historico = calcular_custo_queries(
        bytes_mensais=int(
            historico["bytes_faturados"]
        ),
        preco_por_tib=argumentos.preco_query_tib,
    )

    cenario_dashboard = calcular_cenario_dashboard(
        dry_runs=dry_runs,
        execucoes_por_consulta=(
            argumentos.execucoes_dashboard
        ),
        preco_por_tib=argumentos.preco_query_tib,
    )

    recomendacoes = gerar_recomendacoes(
        tabelas=tabelas,
        historico=historico,
        armazenamento=armazenamento,
        cenario_dashboard=cenario_dashboard,
    )

    salvar_relatorio_csv(
        tabelas=tabelas
    )

    salvar_relatorio_markdown(
        tabelas=tabelas,
        historico=historico,
        dry_runs=dry_runs,
        armazenamento=armazenamento,
        custo_historico=custo_historico,
        cenario_dashboard=cenario_dashboard,
        recomendacoes=recomendacoes,
        argumentos=argumentos,
    )

    exibir_resumo(
        tabelas=tabelas,
        historico=historico,
        armazenamento=armazenamento,
        custo_historico=custo_historico,
        cenario_dashboard=cenario_dashboard,
    )

    print(
        "\nAnálise FinOps concluída com sucesso."
    )


if __name__ == "__main__":
    main()