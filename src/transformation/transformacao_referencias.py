from datetime import datetime, timezone
from pathlib import Path
import re

import pandas as pd


TABELAS = [
    "uf",
    "municipio",
    "meta_alfabetizacao_brasil",
    "meta_alfabetizacao_uf",
    "meta_alfabetizacao_municipio",
]

PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_PATH = PROJECT_ROOT / "data" / "bronze" / "batch"
SILVER_PATH = PROJECT_ROOT / "data" / "silver"


def padronizar_nome_coluna(nome: str) -> str:
    """Padroniza o nome das colunas para snake_case."""

    nome = nome.strip().lower()
    nome = re.sub(r"[^a-z0-9_]+", "_", nome)
    nome = re.sub(r"_+", "_", nome)

    return nome.strip("_")


def localizar_arquivo_mais_recente(tabela: str) -> Path:
    """Localiza o arquivo Parquet mais recente da tabela na camada Bronze."""

    pasta_tabela = BRONZE_PATH / tabela

    pastas_ingestao = sorted(
        pasta_tabela.glob("data_ingestao=*"),
        reverse=True,
    )

    if not pastas_ingestao:
        raise FileNotFoundError(
            f"Nenhuma ingestão encontrada para a tabela {tabela}."
        )

    arquivo = pastas_ingestao[0] / "dados.parquet"

    if not arquivo.exists():
        raise FileNotFoundError(
            f"Arquivo não encontrado: {arquivo}"
        )

    return arquivo


def limpar_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Aplica limpeza e padronização básica no DataFrame."""

    dataframe = dataframe.copy()

    dataframe.columns = [
        padronizar_nome_coluna(coluna)
        for coluna in dataframe.columns
    ]

    for coluna in dataframe.columns:
        if (
            pd.api.types.is_object_dtype(dataframe[coluna])
            or pd.api.types.is_string_dtype(dataframe[coluna])
        ):
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

    dataframe = dataframe.drop_duplicates().reset_index(drop=True)

    return dataframe


def transformar_tabela(tabela: str, data_processamento: str) -> None:
    """Transforma uma tabela Bronze e salva o resultado na Silver."""

    arquivo_origem = localizar_arquivo_mais_recente(tabela)

    print(f"\nProcessando tabela: {tabela}")
    print(f"Origem: {arquivo_origem}")

    dataframe = pd.read_parquet(arquivo_origem)

    quantidade_original = len(dataframe)

    dataframe = limpar_dataframe(dataframe)

    quantidade_final = len(dataframe)
    duplicados_removidos = quantidade_original - quantidade_final

    dataframe["_fonte"] = "base_dos_dados"
    dataframe["_camada_origem"] = "bronze"
    dataframe["_processado_em_utc"] = datetime.now(
        timezone.utc
    ).isoformat()

    pasta_destino = (
        SILVER_PATH
        / tabela
        / f"data_processamento={data_processamento}"
    )

    pasta_destino.mkdir(parents=True, exist_ok=True)

    arquivo_destino = pasta_destino / "dados.parquet"

    dataframe.to_parquet(
        arquivo_destino,
        index=False,
        engine="pyarrow",
    )

    print(f"Linhas recebidas: {quantidade_original:,}")
    print(f"Duplicados removidos: {duplicados_removidos:,}")
    print(f"Linhas salvas: {quantidade_final:,}")
    print(f"Destino: {arquivo_destino}")


def main() -> None:
    """Executa a transformação das tabelas de referência."""

    data_processamento = datetime.now(
        timezone.utc
    ).strftime("%Y-%m-%d")

    print("\nIniciando transformação Bronze → Silver")

    for tabela in TABELAS:
        try:
            transformar_tabela(
                tabela=tabela,
                data_processamento=data_processamento,
            )
        except Exception as erro:
            print(f"\nErro ao processar {tabela}: {erro}")
            raise

    print("\nTransformação das tabelas concluída com sucesso.")


if __name__ == "__main__":
    main()