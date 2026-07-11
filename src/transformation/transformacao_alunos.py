from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


ANOS = [2023, 2024]

PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_PATH = (
    PROJECT_ROOT
    / "data"
    / "bronze"
    / "batch"
    / "alunos"
)

SILVER_PATH = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "alunos"
)

COLUNAS_TEXTO = [
    "id_municipio",
    "id_escola",
    "id_aluno",
    "caderno",
    "serie",
    "rede",
    "presenca",
    "preenchimento_caderno",
    "alfabetizado",
]

COLUNAS_NUMERICAS = [
    "ano",
    "proficiencia",
    "peso_aluno",
]


def localizar_pasta_mais_recente(ano: int) -> Path:
    """Localiza a ingestão mais recente de determinado ano."""

    pasta_ano = BRONZE_PATH / f"ano={ano}"

    pastas_ingestao = sorted(
        pasta_ano.glob("data_ingestao=*"),
        reverse=True,
    )

    if not pastas_ingestao:
        raise FileNotFoundError(
            f"Nenhuma ingestão encontrada para o ano {ano}."
        )

    return pastas_ingestao[0]


def limpar_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Limpa, padroniza e converte os tipos dos dados de alunos."""

    dataframe = dataframe.copy()

    dataframe.columns = [
        coluna.strip().lower()
        for coluna in dataframe.columns
    ]

    for coluna in COLUNAS_TEXTO:
        if coluna in dataframe.columns:
            dataframe[coluna] = (
                dataframe[coluna]
                .astype("string")
                .str.strip()
                .replace(
                    {
                        "": pd.NA,
                        "nan": pd.NA,
                        "None": pd.NA,
                        "null": pd.NA,
                    }
                )
            )

    if "ano" in dataframe.columns:
        dataframe["ano"] = pd.to_numeric(
            dataframe["ano"],
            errors="coerce",
        ).astype("Int64")

    for coluna in ["proficiencia", "peso_aluno"]:
        if coluna in dataframe.columns:
            dataframe[coluna] = pd.to_numeric(
                dataframe[coluna],
                errors="coerce",
            )

    dataframe = dataframe.drop_duplicates().reset_index(drop=True)

    return dataframe


def transformar_ano(
    ano: int,
    data_processamento: str,
) -> None:
    """Transforma todos os arquivos Bronze de determinado ano."""

    pasta_origem = localizar_pasta_mais_recente(ano)

    arquivos_origem = sorted(
        pasta_origem.glob("*.parquet")
    )

    if not arquivos_origem:
        raise FileNotFoundError(
            f"Nenhum arquivo Parquet encontrado para {ano}."
        )

    pasta_destino = (
        SILVER_PATH
        / f"ano={ano}"
        / f"data_processamento={data_processamento}"
    )

    pasta_destino.mkdir(
        parents=True,
        exist_ok=True,
    )

    total_recebido = 0
    total_salvo = 0
    total_duplicados = 0

    print(f"\nProcessando ano {ano}")
    print(f"Arquivos encontrados: {len(arquivos_origem)}")

    for numero, arquivo_origem in enumerate(
        arquivos_origem,
        start=1,
    ):
        dataframe = pd.read_parquet(arquivo_origem)

        quantidade_original = len(dataframe)

        dataframe = limpar_dataframe(dataframe)

        quantidade_final = len(dataframe)
        duplicados_removidos = (
            quantidade_original - quantidade_final
        )

        dataframe["_fonte"] = "base_dos_dados"
        dataframe["_camada_origem"] = "bronze"
        dataframe["_processado_em_utc"] = datetime.now(
            timezone.utc
        ).isoformat()

        arquivo_destino = (
            pasta_destino
            / arquivo_origem.name
        )

        dataframe.to_parquet(
            arquivo_destino,
            index=False,
            engine="pyarrow",
        )

        total_recebido += quantidade_original
        total_salvo += quantidade_final
        total_duplicados += duplicados_removidos

        print(
            f"Arquivo {numero:04d}/{len(arquivos_origem):04d} | "
            f"{quantidade_final:,} linhas salvas"
        )

    print(f"\nAno {ano} concluído")
    print(f"Linhas recebidas: {total_recebido:,}")
    print(f"Duplicados removidos: {total_duplicados:,}")
    print(f"Linhas salvas: {total_salvo:,}")


def main() -> None:
    """Executa a transformação Bronze → Silver dos alunos."""

    data_processamento = datetime.now(
        timezone.utc
    ).strftime("%Y-%m-%d")

    print(
        "\nIniciando transformação dos alunos "
        "Bronze → Silver"
    )

    for ano in ANOS:
        try:
            transformar_ano(
                ano=ano,
                data_processamento=data_processamento,
            )
        except Exception as erro:
            print(
                f"\nErro ao processar o ano {ano}: {erro}"
            )
            raise

    print(
        "\nTransformação dos alunos concluída "
        "com sucesso."
    )


if __name__ == "__main__":
    main()