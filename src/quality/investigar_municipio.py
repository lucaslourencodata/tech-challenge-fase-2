from pathlib import Path

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SILVER_PATH = PROJECT_ROOT / "data" / "silver"


def localizar_pasta_mais_recente(pasta_base: Path) -> Path:
    pastas = sorted(
        pasta_base.glob("data_processamento=*"),
        reverse=True,
    )

    if not pastas:
        raise FileNotFoundError(
            f"Nenhuma pasta encontrada em {pasta_base}"
        )

    return pastas[0]


def main() -> None:
    pasta_municipio = localizar_pasta_mais_recente(
        SILVER_PATH / "municipio"
    )

    arquivos_municipio = (
        pasta_municipio / "*.parquet"
    ).as_posix()

    arquivos_alunos = (
        SILVER_PATH
        / "alunos"
        / "ano=*"
        / "data_processamento=*"
        / "*.parquet"
    ).as_posix()

    conexao = duckdb.connect(database=":memory:")

    consulta = f"""
        SELECT
            alunos.id_municipio,
            COUNT(*) AS quantidade_registros,
            MIN(alunos.ano) AS primeiro_ano,
            MAX(alunos.ano) AS ultimo_ano
        FROM read_parquet(
            '{arquivos_alunos}',
            union_by_name=true,
            hive_partitioning=false
        ) AS alunos
        LEFT JOIN read_parquet(
            '{arquivos_municipio}',
            union_by_name=true,
            hive_partitioning=false
        ) AS municipios
            ON alunos.id_municipio = municipios.id_municipio
        WHERE
            alunos.id_municipio IS NOT NULL
            AND municipios.id_municipio IS NULL
        GROUP BY
            alunos.id_municipio
        ORDER BY
            quantidade_registros DESC
    """

    resultado = conexao.execute(consulta).fetchdf()

    conexao.close()

    print("\nMunicípios presentes em alunos e ausentes em municipio:\n")
    print(resultado.to_string(index=False))


if __name__ == "__main__":
    main()