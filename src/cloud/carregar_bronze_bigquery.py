from pathlib import Path

import pyarrow.parquet as parquet
from google.api_core.exceptions import GoogleAPIError
from google.cloud import bigquery


PROJECT_ID = "tc-alfabetizacao-lucas"
DATASET_ID = "tc_bronze"
LOCATION = "US"

PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_BATCH_PATH = (
    PROJECT_ROOT
    / "data"
    / "bronze"
    / "batch"
)

BRONZE_STREAMING_PATH = (
    PROJECT_ROOT
    / "data"
    / "bronze"
    / "streaming"
    / "eventos"
)

TABELAS_REFERENCIA = {
    "uf": (
        "Dados brutos de resultados de alfabetização "
        "agregados por unidade federativa."
    ),
    "municipio": (
        "Dados brutos de resultados de alfabetização "
        "agregados por município."
    ),
    "meta_alfabetizacao_brasil": (
        "Dados brutos das metas nacionais de alfabetização."
    ),
    "meta_alfabetizacao_uf": (
        "Dados brutos das metas de alfabetização por UF."
    ),
    "meta_alfabetizacao_municipio": (
        "Dados brutos das metas de alfabetização por município."
    ),
}

DESCRICAO_ALUNOS = (
    "Microdados brutos dos alunos da avaliação de alfabetização, "
    "provenientes da Base dos Dados."
)

TABELA_EVENTOS_STREAMING = (
    "eventos_indicador_alfabetizacao"
)

DESCRICAO_EVENTOS_STREAMING = (
    "Eventos brutos simulados de atualização do indicador "
    "de alfabetização, recebidos em formato JSON Lines."
)


def localizar_arquivo_referencia(
    nome_tabela: str,
) -> Path:
    """Localiza o arquivo Bronze mais recente de uma tabela menor."""

    pasta_tabela = (
        BRONZE_BATCH_PATH
        / nome_tabela
    )

    pastas_ingestao = sorted(
        pasta_tabela.glob("data_ingestao=*"),
        reverse=True,
    )

    if not pastas_ingestao:
        raise FileNotFoundError(
            f"Nenhuma ingestão encontrada para {nome_tabela}."
        )

    arquivo = (
        pastas_ingestao[0]
        / "dados.parquet"
    )

    if not arquivo.exists():
        raise FileNotFoundError(
            f"Arquivo não encontrado: {arquivo}"
        )

    return arquivo


def localizar_arquivos_alunos() -> list[Path]:
    """Localiza os arquivos Bronze mais recentes dos alunos."""

    pasta_alunos = (
        BRONZE_BATCH_PATH
        / "alunos"
    )

    arquivos: list[Path] = []

    for pasta_ano in sorted(
        pasta_alunos.glob("ano=*")
    ):
        pastas_ingestao = sorted(
            pasta_ano.glob("data_ingestao=*"),
            reverse=True,
        )

        if not pastas_ingestao:
            raise FileNotFoundError(
                f"Nenhuma ingestão encontrada em {pasta_ano}."
            )

        arquivos_ano = sorted(
            pastas_ingestao[0].glob("*.parquet")
        )

        if not arquivos_ano:
            raise FileNotFoundError(
                "Nenhum arquivo Parquet encontrado em "
                f"{pastas_ingestao[0]}."
            )

        arquivos.extend(
            arquivos_ano
        )

    if not arquivos:
        raise FileNotFoundError(
            "Nenhum arquivo Bronze de alunos foi encontrado."
        )

    return arquivos


def localizar_arquivos_streaming() -> list[Path]:
    """Localiza os arquivos JSONL produzidos pelo streaming."""

    return sorted(
        BRONZE_STREAMING_PATH.glob(
            "eventos_*.jsonl"
        )
    )


def contar_linhas_parquet(
    arquivos: list[Path],
) -> int:
    """Soma a quantidade de linhas dos arquivos Parquet."""

    total_linhas = 0

    for arquivo in arquivos:
        arquivo_parquet = parquet.ParquetFile(
            arquivo
        )

        total_linhas += (
            arquivo_parquet
            .metadata
            .num_rows
        )

    return total_linhas


def contar_linhas_jsonl(
    arquivos: list[Path],
) -> int:
    """Conta os eventos não vazios dos arquivos JSONL."""

    total_linhas = 0

    for arquivo in arquivos:
        with arquivo.open(
            mode="r",
            encoding="utf-8",
        ) as conteudo:
            total_linhas += sum(
                1
                for linha in conteudo
                if linha.strip()
            )

    return total_linhas


def atualizar_metadados_tabela(
    client: bigquery.Client,
    tabela_destino: str,
    descricao: str,
) -> bigquery.Table:
    """Adiciona descrição e rótulos à tabela."""

    tabela = client.get_table(
        tabela_destino
    )

    tabela.description = descricao

    tabela.labels = {
        "projeto": "tech_challenge_fase_2",
        "camada": "bronze",
        "ambiente": "desenvolvimento",
    }

    return client.update_table(
        tabela,
        [
            "description",
            "labels",
        ],
    )


def carregar_parquet(
    client: bigquery.Client,
    arquivo: Path,
    tabela_destino: str,
    write_disposition: str,
    clustering_fields: list[str] | None = None,
) -> None:
    """Carrega um arquivo Parquet local no BigQuery."""

    configuracao = bigquery.LoadJobConfig(
        source_format=(
            bigquery.SourceFormat.PARQUET
        ),
        write_disposition=write_disposition,
    )

    if clustering_fields:
        configuracao.clustering_fields = (
            clustering_fields
        )

    with arquivo.open(
        mode="rb"
    ) as arquivo_binario:
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
    """Carrega uma tabela Bronze de referência."""

    arquivo = localizar_arquivo_referencia(
        nome_tabela
    )

    quantidade_local = contar_linhas_parquet(
        [arquivo]
    )

    tabela_destino = (
        f"{PROJECT_ID}."
        f"{DATASET_ID}."
        f"{nome_tabela}"
    )

    print(f"\nCarregando: {nome_tabela}")
    print(f"Origem: {arquivo}")
    print(f"Destino: {tabela_destino}")
    print(
        f"Linhas locais: "
        f"{quantidade_local:,}"
    )

    try:
        carregar_parquet(
            client=client,
            arquivo=arquivo,
            tabela_destino=tabela_destino,
            write_disposition=(
                bigquery
                .WriteDisposition
                .WRITE_TRUNCATE
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
    """Carrega todos os arquivos brutos dos alunos."""

    nome_tabela = "alunos"

    arquivos = localizar_arquivos_alunos()

    quantidade_local = contar_linhas_parquet(
        arquivos
    )

    tabela_destino = (
        f"{PROJECT_ID}."
        f"{DATASET_ID}."
        f"{nome_tabela}"
    )

    print(f"\nCarregando: {nome_tabela}")
    print(
        f"Arquivos encontrados: "
        f"{len(arquivos)}"
    )
    print(f"Destino: {tabela_destino}")
    print(
        f"Total de linhas locais: "
        f"{quantidade_local:,}\n"
    )

    try:
        for numero, arquivo in enumerate(
            arquivos,
            start=1,
        ):
            if numero == 1:
                write_disposition = (
                    bigquery
                    .WriteDisposition
                    .WRITE_TRUNCATE
                )

                clustering_fields = [
                    "ano",
                    "id_municipio",
                    "rede",
                ]

            else:
                write_disposition = (
                    bigquery
                    .WriteDisposition
                    .WRITE_APPEND
                )

                clustering_fields = None

            carregar_parquet(
                client=client,
                arquivo=arquivo,
                tabela_destino=tabela_destino,
                write_disposition=write_disposition,
                clustering_fields=clustering_fields,
            )

            quantidade_arquivo = (
                contar_linhas_parquet(
                    [arquivo]
                )
            )

            print(
                f"[{numero:03d}/{len(arquivos):03d}] "
                f"{arquivo.parent.parent.name}/"
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
            "Erro ao carregar a tabela alunos: "
            f"{erro}"
        ) from erro


def carregar_eventos_streaming(
    client: bigquery.Client,
) -> bool:
    """Carrega os eventos JSONL brutos no BigQuery."""

    arquivos = localizar_arquivos_streaming()

    if not arquivos:
        print(
            "\n[AVISO] Nenhum arquivo de streaming "
            "foi encontrado. A carga será ignorada."
        )

        return False

    quantidade_local = contar_linhas_jsonl(
        arquivos
    )

    tabela_destino = (
        f"{PROJECT_ID}."
        f"{DATASET_ID}."
        f"{TABELA_EVENTOS_STREAMING}"
    )

    print(
        "\nCarregando eventos brutos de streaming"
    )
    print(
        f"Arquivos encontrados: "
        f"{len(arquivos)}"
    )
    print(f"Destino: {tabela_destino}")
    print(
        f"Total de eventos locais: "
        f"{quantidade_local:,}\n"
    )

    try:
        for numero, arquivo in enumerate(
            arquivos,
            start=1,
        ):
            if numero == 1:
                write_disposition = (
                    bigquery
                    .WriteDisposition
                    .WRITE_TRUNCATE
                )
            else:
                write_disposition = (
                    bigquery
                    .WriteDisposition
                    .WRITE_APPEND
                )

            configuracao = bigquery.LoadJobConfig(
                source_format=(
                    bigquery
                    .SourceFormat
                    .NEWLINE_DELIMITED_JSON
                ),
                autodetect=True,
                write_disposition=write_disposition,
                max_bad_records=0,
                ignore_unknown_values=False,
            )

            with arquivo.open(
                mode="rb"
            ) as arquivo_binario:
                job = client.load_table_from_file(
                    arquivo_binario,
                    tabela_destino,
                    job_config=configuracao,
                    location=LOCATION,
                )

                job.result()

            quantidade_arquivo = (
                contar_linhas_jsonl(
                    [arquivo]
                )
            )

            print(
                f"[{numero:03d}/{len(arquivos):03d}] "
                f"{arquivo.name} | "
                f"{quantidade_arquivo:,} eventos"
            )

        tabela = atualizar_metadados_tabela(
            client=client,
            tabela_destino=tabela_destino,
            descricao=DESCRICAO_EVENTOS_STREAMING,
        )

        if tabela.num_rows != quantidade_local:
            raise ValueError(
                "Quantidade divergente nos eventos: "
                f"local={quantidade_local:,}, "
                f"BigQuery={tabela.num_rows:,}."
            )

        print(
            f"\n[OK] {TABELA_EVENTOS_STREAMING} | "
            f"{tabela.num_rows:,} eventos carregados"
        )

        return True

    except GoogleAPIError as erro:
        raise RuntimeError(
            "Erro ao carregar os eventos de streaming: "
            f"{erro}"
        ) from erro


def listar_tabelas_bronze(
    client: bigquery.Client,
    eventos_carregados: bool,
) -> None:
    """Confirma as tabelas existentes na camada Bronze."""

    dataset_completo = (
        f"{PROJECT_ID}.{DATASET_ID}"
    )

    tabelas_esperadas = [
        *TABELAS_REFERENCIA.keys(),
        "alunos",
    ]

    if eventos_carregados:
        tabelas_esperadas.append(
            TABELA_EVENTOS_STREAMING
        )

    tabelas_encontradas = {
        tabela.table_id
        for tabela in client.list_tables(
            dataset_completo
        )
    }

    print(
        "\nTabelas disponíveis no BigQuery:\n"
    )

    for nome_tabela in tabelas_esperadas:
        status = (
            "OK"
            if nome_tabela in tabelas_encontradas
            else "NÃO ENCONTRADA"
        )

        print(
            f"- {nome_tabela}: {status}"
        )


def main() -> None:
    """Carrega a camada Bronze local para o BigQuery."""

    print(
        "\nIniciando carga da camada Bronze "
        "no BigQuery"
    )
    print(f"Projeto: {PROJECT_ID}")
    print(f"Dataset: {DATASET_ID}")
    print(f"Localização: {LOCATION}")

    client = bigquery.Client(
        project=PROJECT_ID,
        location=LOCATION,
    )

    for nome_tabela, descricao in (
        TABELAS_REFERENCIA.items()
    ):
        carregar_tabela_referencia(
            client=client,
            nome_tabela=nome_tabela,
            descricao=descricao,
        )

    carregar_tabela_alunos(
        client=client
    )

    eventos_carregados = (
        carregar_eventos_streaming(
            client=client
        )
    )

    listar_tabelas_bronze(
        client=client,
        eventos_carregados=eventos_carregados,
    )

    print(
        "\nCarga da camada Bronze "
        "concluída com sucesso."
    )


if __name__ == "__main__":
    main()
