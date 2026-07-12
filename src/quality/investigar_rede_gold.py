from pathlib import Path

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[2]

GOLD_PATH = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "indicador_alfabetizacao_municipio"
)


def main() -> None:
    pastas = sorted(
        GOLD_PATH.glob("data_processamento=*"),
        reverse=True,
    )

    if not pastas:
        raise FileNotFoundError(
            "Nenhum processamento Gold encontrado."
        )

    arquivo = pastas[0] / "dados.parquet"
    caminho = arquivo.as_posix().replace("'", "''")

    conexao = duckdb.connect(database=":memory:")

    resultado = conexao.execute(
        f"""
        SELECT
            codigo_rede,
            rede,
            COUNT(*) AS quantidade
        FROM read_parquet(
            '{caminho}',
            hive_partitioning=false
        )
        GROUP BY
            codigo_rede,
            rede
        ORDER BY
            codigo_rede
        """
    ).fetchdf()

    conexao.close()

    print("\nCódigos de rede encontrados na camada Gold:\n")
    print(resultado.to_string(index=False))


if __name__ == "__main__":
    main()