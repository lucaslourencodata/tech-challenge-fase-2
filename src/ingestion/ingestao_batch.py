from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery


PROJECT_ID = "tc-alfabetizacao-lucas"
SOURCE_DATASET = "basedosdados.br_inep_avaliacao_alfabetizacao"

TABELAS = [
    "uf",
    "municipio",
    "meta_alfabetizacao_brasil",
    "meta_alfabetizacao_uf",
    "meta_alfabetizacao_municipio",
]

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_PATH = PROJECT_ROOT / "data" / "bronze" / "batch"


def ingerir_tabela(
    client: bigquery.Client,
    tabela: str,
    data_ingestao: str,
) -> None:
    """Baixa uma tabela do BigQuery e salva na camada Bronze em Parquet."""

    consulta = f"""
    SELECT *
    FROM `{SOURCE_DATASET}.{tabela}`
    """

    print(f"Iniciando ingestão: {tabela}")

    dataframe = client.query(consulta).to_dataframe()

    pasta_destino = (
        BRONZE_PATH
        / tabela
        / f"data_ingestao={data_ingestao}"
    )

    pasta_destino.mkdir(parents=True, exist_ok=True)

    arquivo_destino = pasta_destino / "dados.parquet"

    dataframe.to_parquet(
        arquivo_destino,
        index=False,
        engine="pyarrow",
    )

    print(
        f"Concluído: {tabela} | "
        f"{len(dataframe):,} linhas | "
        f"{arquivo_destino}"
    )


def main() -> None:
    """Executa a ingestão batch das tabelas de referência e metas."""

    data_ingestao = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    client = bigquery.Client(project=PROJECT_ID)

    print("\nIniciando pipeline de ingestão Batch — camada Bronze\n")

    for tabela in TABELAS:
        try:
            ingerir_tabela(client, tabela, data_ingestao)
        except Exception as erro:
            print(f"Erro durante a ingestão de {tabela}: {erro}")
            raise

    print("\nIngestão Batch concluída com sucesso.")


if __name__ == "__main__":
    main()