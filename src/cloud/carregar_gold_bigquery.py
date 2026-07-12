from pathlib import Path

import pyarrow.parquet as parquet
from google.api_core.exceptions import GoogleAPIError
from google.cloud import bigquery


PROJECT_ID = "tc-alfabetizacao-lucas"
DATASET_ID = "tc_gold"
LOCATION = "US"

PROJECT_ROOT = Path(__file__).resolve().parents[2]
GOLD_PATH = PROJECT_ROOT / "data" / "gold"

TABELAS = {
    "indicador_alfabetizacao_municipio": {
        "descricao": (
            "Indicadores de alfabetização por município, ano e rede."
        ),
        "clusterizacao": [
            "id_uf",
            "codigo_rede",
        ],
    },
    "metas_alfabetizacao_municipio": {
        "descricao": (
            "Metas de alfabetização municipais em formato longitudinal."
        ),
        "clusterizacao": [
            "id_uf",
            "codigo_rede",
        ],
    },
    "comparacao_meta_resultado_municipio": {
        "descricao": (
            "Comparação entre indicadores municipais e metas compatíveis."
        ),
        "clusterizacao": [
            "id_uf",
            "codigo_rede",
        ],
    },
    "evolucao_alfabetizacao_municipio": {
        "descricao": (
            "Evolução da alfabetização municipal entre 2023 e 2024."
        ),
        "clusterizacao": [
            "id_uf",
            "codigo_rede",
        ],
    },
}


def localizar_arquivo_mais_recente(
    nome_tabela: str,
) -> Path:
    """Localiza o arquivo Parquet Gold mais recente."""

    pasta_tabela = GOLD_PATH / nome_tabela

    pastas_processamento = sorted(
        pasta_tabela.glob("data_processamento=*"),
        reverse=True,
    )

    if not pastas_processamento:
        raise FileNotFoundError(
            f"Nenhum processamento encontrado para {nome_tabela}."
        )

    arquivo = pastas_processamento[0] / "dados.parquet"

    if not arquivo.exists():
        raise FileNotFoundError(
            f"Arquivo não encontrado: {arquivo}"
        )

    return arquivo


def contar_linhas_parquet(
    arquivo: Path,
) -> int:
    """Obtém a quantidade de linhas pelo metadado do Parquet."""

    arquivo_parquet = parquet.ParquetFile(
        arquivo
    )

    return arquivo_parquet.metadata.num_rows


def carregar_tabela(
    client: bigquery.Client,
    nome_tabela: str,
    descricao: str,
    clusterizacao: list[str],
) -> None:
    """Carrega uma tabela Gold local para o BigQuery."""

    arquivo = localizar_arquivo_mais_recente(
        nome_tabela
    )

    quantidade_local = contar_linhas_parquet(
        arquivo
    )

    tabela_destino = (
        f"{PROJECT_ID}.{DATASET_ID}.{nome_tabela}"
    )

    configuracao = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=(
            bigquery.WriteDisposition.WRITE_TRUNCATE
        ),
        clustering_fields=clusterizacao,
    )

    print(f"\nCarregando: {nome_tabela}")
    print(f"Origem: {arquivo}")
    print(f"Destino: {tabela_destino}")
    print(f"Linhas locais: {quantidade_local:,}")

    try:
        with arquivo.open("rb") as arquivo_binario:
            job = client.load_table_from_file(
                arquivo_binario,
                tabela_destino,
                job_config=configuracao,
                location=LOCATION,
            )

            job.result()

        tabela = client.get_table(
            tabela_destino
        )

        tabela.description = descricao

        tabela.labels = {
            "projeto": "tech_challenge_fase_2",
            "camada": "gold",
            "ambiente": "desenvolvimento",
        }

        tabela = client.update_table(
            tabela,
            [
                "description",
                "labels",
            ],
        )

        quantidade_bigquery = tabela.num_rows

        if quantidade_bigquery != quantidade_local:
            raise ValueError(
                f"Quantidade divergente em {nome_tabela}: "
                f"local={quantidade_local:,}, "
                f"BigQuery={quantidade_bigquery:,}."
            )

        print(
            f"[OK] {nome_tabela} | "
            f"{quantidade_bigquery:,} linhas carregadas"
        )

    except GoogleAPIError as erro:
        raise RuntimeError(
            f"Erro ao carregar {nome_tabela} para o BigQuery: "
            f"{erro}"
        ) from erro


def listar_tabelas_gold(
    client: bigquery.Client,
) -> None:
    """Lista as tabelas existentes no dataset Gold."""

    dataset_completo = (
        f"{PROJECT_ID}.{DATASET_ID}"
    )

    print("\nTabelas disponíveis no BigQuery:\n")

    tabelas_encontradas = {
        tabela.table_id
        for tabela in client.list_tables(
            dataset_completo
        )
    }

    for nome_tabela in TABELAS:
        status = (
            "OK"
            if nome_tabela in tabelas_encontradas
            else "NÃO ENCONTRADA"
        )

        print(
            f"- {nome_tabela}: {status}"
        )


def main() -> None:
    """Carrega a camada Gold local para o BigQuery."""

    print("\nIniciando carga da camada Gold no BigQuery")
    print(f"Projeto: {PROJECT_ID}")
    print(f"Dataset: {DATASET_ID}")
    print(f"Localização: {LOCATION}")

    client = bigquery.Client(
        project=PROJECT_ID,
        location=LOCATION,
    )

    for nome_tabela, configuracao in TABELAS.items():
        carregar_tabela(
            client=client,
            nome_tabela=nome_tabela,
            descricao=configuracao["descricao"],
            clusterizacao=configuracao["clusterizacao"],
        )

    listar_tabelas_gold(
        client=client
    )

    print(
        "\nCarga da camada Gold concluída com sucesso."
    )


if __name__ == "__main__":
    main()
