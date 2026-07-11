from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery


PROJECT_ID = "tc-alfabetizacao-lucas"

SOURCE_TABLE = (
    "basedosdados."
    "br_inep_avaliacao_alfabetizacao."
    "alunos"
)

# Primeiro testaremos apenas 2023.
ANOS = [2023, 2024]

TAMANHO_PAGINA = 100_000

PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_PATH = (
    PROJECT_ROOT
    / "data"
    / "bronze"
    / "batch"
    / "alunos"
)


def ingerir_ano(
    client: bigquery.Client,
    ano: int,
    data_ingestao: str,
) -> None:
    """Baixa os dados de alunos por ano em partes."""

    consulta = f"""
        SELECT *
        FROM `{SOURCE_TABLE}`
        WHERE ano = @ano
    """

    configuracao = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "ano",
                "INT64",
                ano,
            )
        ]
    )

    print(f"\nIniciando ingestão dos alunos de {ano}...")

    job = client.query(
        consulta,
        job_config=configuracao,
    )

    resultado = job.result(
        page_size=TAMANHO_PAGINA,
    )

    pasta_destino = (
        BRONZE_PATH
        / f"ano={ano}"
        / f"data_ingestao={data_ingestao}"
    )

    pasta_destino.mkdir(
        parents=True,
        exist_ok=True,
    )

    total_linhas = 0
    total_partes = 0

    for numero_parte, dataframe in enumerate(
        resultado.to_dataframe_iterable(),
        start=1,
    ):
        if dataframe.empty:
            continue

        arquivo_destino = (
            pasta_destino
            / f"parte-{numero_parte:04d}.parquet"
        )

        dataframe.to_parquet(
            arquivo_destino,
            index=False,
            engine="pyarrow",
        )

        quantidade_linhas = len(dataframe)

        total_linhas += quantidade_linhas
        total_partes += 1

        print(
            f"Parte {numero_parte:04d} salva | "
            f"{quantidade_linhas:,} linhas"
        )

    print(
        f"\nAno {ano} concluído | "
        f"{total_linhas:,} linhas | "
        f"{total_partes} arquivos"
    )


def main() -> None:
    """Executa a ingestão da tabela de alunos."""

    data_ingestao = datetime.now(
        timezone.utc
    ).strftime("%Y-%m-%d")

    client = bigquery.Client(
        project=PROJECT_ID
    )

    print(
        "\nIniciando ingestão Batch — "
        "tabela alunos — camada Bronze"
    )

    for ano in ANOS:
        try:
            ingerir_ano(
                client,
                ano,
                data_ingestao,
            )
        except Exception as erro:
            print(
                f"\nErro na ingestão de {ano}: "
                f"{erro}"
            )
            raise

    print(
        "\nIngestão dos alunos concluída "
        "com sucesso."
    )


if __name__ == "__main__":
    main()