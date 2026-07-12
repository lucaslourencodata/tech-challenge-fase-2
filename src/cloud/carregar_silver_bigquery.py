from pathlib import Path

import pyarrow.parquet as parquet
from google.api_core.exceptions import GoogleAPIError
from google.cloud import bigquery


PROJECT_ID = "tc-alfabetizacao-lucas"
DATASET_ID = "tc_silver"
LOCATION = "US"

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SILVER_PATH = PROJECT_ROOT / "data" / "silver"

TABELAS_REFERENCIA = {
    "uf": (
        "Resultados de alfabetização agregados por UF, "
        "limpos e padronizados."
    ),
    "municipio": (
        "Resultados de alfabetização agregados por município, "
        "limpos e padronizados."
    ),
    "meta_alfabetizacao_brasil": (
        "Metas nacionais de alfabetização tratadas."
    ),
    "meta_alfabetizacao_uf": (
        "Metas de alfabetização por UF tratadas."
    ),
    "meta_alfabetizacao_municipio": (
        "Metas de alfabetização por município tratadas."
    ),
}

DESCRICAO_ALUNOS = (
    "Microdados de alunos da avaliação de alfabetização, "
    "limpos, padronizados e particionados por ano na origem."
)


def localizar_arquivo_referencia(
    nome_tabela: str,
) -> Path:
    """Localiza o arquivo Silver mais recente de uma tabela menor."""

    pasta_tabela = SILVER_PATH / nome_tabela

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


def localizar_arquivos_alunos() -> list[Path]:
    """Localiza os arquivos Silver mais recentes dos alunos."""

    pasta_alunos = SILVER_PATH / "alunos"
    arquivos: list[Path] = []

    for pasta_ano in sorted(
        pasta_alunos.glob("ano=*")
    ):
        pastas_processamento = sorted(
            pasta_ano.glob("data_processamento=*"),
            reverse=True,
        )

        if not pastas_processamento:
            raise FileNotFoundError(
                f"Nenhum processamento encontrado em {pasta_ano}."
            )

        arquivos_ano = sorted(
            pastas_processamento[0].glob("*.parquet")
        )

        if not arquivos_ano:
            raise FileNotFoundError(
                f"Nenhum arquivo Parquet encontrado em "
                f"{pastas_processamento[0]}."
            )

        arquivos.extend(arquivos_ano)

    if not arquivos:
        raise FileNotFoundError(
            "Nenhum arquivo Silver de alunos foi encontrado."
        )

    return arquivos


def contar_linhas_parquet(
    arquivos: list[Path],
) -> int:
    """Soma as linhas registradas nos metadados dos Parquets."""

    total = 0

    for arquivo in arquivos:
        arquivo_parquet = parquet.ParquetFile(
            arquivo
        )

        total += arquivo_parquet.metadata.num_rows

    return total


def criar_configuracao_carga(
    write_disposition: str,
    clustering_fields: list[str] | None = None,
) -> bigquery.LoadJobConfig:
    """Cria a configuração de uma carga Parquet."""

    configuracao = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=write_disposition,
    )

    if clustering_fields:
        configuracao.clustering_fields = clustering_fields

    return configuracao


def atualizar_metadados_tabela(
    client: bigquery.Client,
    tabela_destino: str,
    descricao: str,
) -> bigquery.Table:
    """Adiciona descrição e rótulos à tabela do BigQuery."""

    tabela = client.get_table(
        tabela_destino
    )

    tabela.description = descricao

    tabela.labels = {
        "projeto": "tech_challenge_fase_2",
        "camada": "silver",
        "ambiente": "desenvolvimento",
    }

    return client.update_table(
        tabela,
        [
            "description",
            "labels",
        ],
    )


def carregar_arquivo(
    client: bigquery.Client,
    arquivo: Path,
    tabela_destino: str,
    write_disposition: str,
    clustering_fields: list[str] | None = None,
) -> None:
    """Envia um arquivo Parquet local ao BigQuery."""

    configuracao = criar_configuracao_carga(
        write_disposition=write_disposition,
        clustering_fields=clustering_fields,
    )

    with arquivo.open("rb") as arquivo_binario:
        job = client.load_table_from_file(
            arquivo_binario,
            tabela_destino,
            job_config=configuracao,
            location=LOCATION,
        )

        job.result()


def carregar_tabela_referencia(
    client: bigquery.Client,
    nome_tabela: str,
    descricao: str,
) -> None:
    """Carrega uma tabela Silver de referência."""

    arquivo = localizar_arquivo_referencia(
        nome_tabela
    )

    quantidade_local = contar_linhas_parquet(
        [arquivo]
    )

    tabela_destino = (
        f"{PROJECT_ID}.{DATASET_ID}.{nome_tabela}"
    )

    print(f"\nCarregando: {nome_tabela}")
    print(f"Origem: {arquivo}")
    print(f"Destino: {tabela_destino}")
    print(f"Linhas locais: {quantidade_local:,}")

    try:
        carregar_arquivo(
            client=client,
            arquivo=arquivo,
            tabela_destino=tabela_destino,
            write_disposition=(
                bigquery.WriteDisposition.WRITE_TRUNCATE
            ),
        )

        tabela = atualizar_metadados_tabela(
            client=client,
            tabela_destino=tabela_destino,
            descricao=descricao,
        )

        if tabela.num_rows != quantidade_local:
            raise ValueError(
                f"Quantidade divergente em {nome_tabela}: "
                f"local={quantidade_local:,}, "
                f"BigQuery={tabela.num_rows:,}."
            )

        print(
            f"[OK] {nome_tabela} | "
            f"{tabela.num_rows:,} linhas carregadas"
        )

    except GoogleAPIError as erro:
        raise RuntimeError(
            f"Erro ao carregar {nome_tabela}: {erro}"
        ) from erro


def carregar_tabela_alunos(
    client: bigquery.Client,
) -> None:
    """Carrega todos os arquivos de alunos em uma única tabela."""

    nome_tabela = "alunos"

    arquivos = localizar_arquivos_alunos()

    quantidade_local = contar_linhas_parquet(
        arquivos
    )

    tabela_destino = (
        f"{PROJECT_ID}.{DATASET_ID}.{nome_tabela}"
    )

    print(f"\nCarregando: {nome_tabela}")
    print(f"Arquivos encontrados: {len(arquivos)}")
    print(f"Destino: {tabela_destino}")
    print(f"Total de linhas locais: {quantidade_local:,}\n")

    try:
        for numero, arquivo in enumerate(
            arquivos,
            start=1,
        ):
            if numero == 1:
                write_disposition = (
                    bigquery.WriteDisposition.WRITE_TRUNCATE
                )
            else:
                write_disposition = (
                    bigquery.WriteDisposition.WRITE_APPEND
                )

            carregar_arquivo(
                client=client,
                arquivo=arquivo,
                tabela_destino=tabela_destino,
                write_disposition=write_disposition,
                clustering_fields=[
                    "ano",
                    "id_municipio",
                    "rede",
                ],
            )

            quantidade_arquivo = contar_linhas_parquet(
                [arquivo]
            )

            print(
                f"[{numero:03d}/{len(arquivos):03d}] "
                f"{arquivo.name} | "
                f"{quantidade_arquivo:,} linhas"
            )

        tabela = atualizar_metadados_tabela(
            client=client,
            tabela_destino=tabela_destino,
            descricao=DESCRICAO_ALUNOS,
        )

        if tabela.num_rows != quantidade_local:
            raise ValueError(
                "Quantidade divergente na tabela alunos: "
                f"local={quantidade_local:,}, "
                f"BigQuery={tabela.num_rows:,}."
            )

        print(
            f"\n[OK] alunos | "
            f"{tabela.num_rows:,} linhas carregadas"
        )

    except GoogleAPIError as erro:
        raise RuntimeError(
            f"Erro ao carregar a tabela alunos: {erro}"
        ) from erro


def listar_tabelas_silver(
    client: bigquery.Client,
) -> None:
    """Lista e confirma as tabelas existentes na Silver."""

    dataset_completo = (
        f"{PROJECT_ID}.{DATASET_ID}"
    )

    tabelas_esperadas = [
        *TABELAS_REFERENCIA.keys(),
        "alunos",
    ]

    tabelas_encontradas = {
        tabela.table_id
        for tabela in client.list_tables(
            dataset_completo
        )
    }

    print("\nTabelas disponíveis no BigQuery:\n")

    for nome_tabela in tabelas_esperadas:
        status = (
            "OK"
            if nome_tabela in tabelas_encontradas
            else "NÃO ENCONTRADA"
        )

        print(f"- {nome_tabela}: {status}")


def main() -> None:
    """Carrega a camada Silver local para o BigQuery."""

    print("\nIniciando carga da camada Silver no BigQuery")
    print(f"Projeto: {PROJECT_ID}")
    print(f"Dataset: {DATASET_ID}")
    print(f"Localização: {LOCATION}")

    client = bigquery.Client(
        project=PROJECT_ID,
        location=LOCATION,
    )

    for nome_tabela, descricao in TABELAS_REFERENCIA.items():
        carregar_tabela_referencia(
            client=client,
            nome_tabela=nome_tabela,
            descricao=descricao,
        )

    carregar_tabela_alunos(
        client=client
    )

    listar_tabelas_silver(
        client=client
    )

    print(
        "\nCarga da camada Silver concluída com sucesso."
    )


if __name__ == "__main__":
    main()
