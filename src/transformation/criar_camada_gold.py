from datetime import datetime, timezone
from pathlib import Path

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[2]

SILVER_PATH = PROJECT_ROOT / "data" / "silver"
GOLD_PATH = PROJECT_ROOT / "data" / "gold"


def localizar_arquivo_mais_recente(tabela: str) -> Path:
    """Localiza o arquivo mais recente de uma tabela Silver."""

    pasta_tabela = SILVER_PATH / tabela

    pastas_processamento = sorted(
        pasta_tabela.glob("data_processamento=*"),
        reverse=True,
    )

    if not pastas_processamento:
        raise FileNotFoundError(
            f"Nenhum processamento Silver encontrado para {tabela}."
        )

    arquivo = pastas_processamento[0] / "dados.parquet"

    if not arquivo.exists():
        raise FileNotFoundError(
            f"Arquivo não encontrado: {arquivo}"
        )

    return arquivo


def preparar_caminho_sql(caminho: Path) -> str:
    """Converte um caminho para utilização segura em SQL."""

    return caminho.as_posix().replace("'", "''")


def salvar_gold(
    conexao: duckdb.DuckDBPyConnection,
    nome_tabela: str,
    consulta: str,
    data_processamento: str,
) -> Path:
    """Executa uma consulta e salva o resultado Gold em Parquet."""

    pasta_destino = (
        GOLD_PATH
        / nome_tabela
        / f"data_processamento={data_processamento}"
    )

    pasta_destino.mkdir(
        parents=True,
        exist_ok=True,
    )

    arquivo_destino = pasta_destino / "dados.parquet"

    # Permite executar novamente o pipeline no mesmo dia.
    if arquivo_destino.exists():
        arquivo_destino.unlink()

    caminho_destino = preparar_caminho_sql(
        arquivo_destino
    )

    consulta_limpa = consulta.strip().rstrip(";")

    conexao.execute(
        f"""
        COPY (
            {consulta_limpa}
        )
        TO '{caminho_destino}'
        (
            FORMAT PARQUET,
            COMPRESSION ZSTD
        )
        """
    )

    quantidade_linhas = conexao.execute(
        f"""
        SELECT COUNT(*)
        FROM read_parquet(
            '{caminho_destino}',
            hive_partitioning=false
        )
        """
    ).fetchone()[0]

    print(
        f"[OK] {nome_tabela} | "
        f"{quantidade_linhas:,} linhas | "
        f"{arquivo_destino}"
    )

    return arquivo_destino


def criar_indicador_municipio(
    conexao: duckdb.DuckDBPyConnection,
    arquivo_municipio: Path,
    data_processamento: str,
) -> Path:
    """Cria o dataset analítico de alfabetização por município."""

    caminho_municipio = preparar_caminho_sql(
        arquivo_municipio
    )

    consulta = f"""
        SELECT
            CAST(ano AS INTEGER) AS ano,

            CAST(
                id_municipio AS VARCHAR
            ) AS id_municipio,

            SUBSTR(
                CAST(id_municipio AS VARCHAR),
                1,
                2
            ) AS id_uf,

            CAST(
                serie AS VARCHAR
            ) AS serie,

            CAST(
                rede AS INTEGER
            ) AS codigo_rede,

            CASE CAST(rede AS INTEGER)
                WHEN 0 THEN 'Total'
                WHEN 1 THEN 'Federal'
                WHEN 2 THEN 'Estadual'
                WHEN 3 THEN 'Municipal'
                WHEN 4 THEN 'Privada'
                ELSE 'Não identificado'
            END AS rede,

            ROUND(
                CAST(
                    taxa_alfabetizacao AS DOUBLE
                ),
                2
            ) AS taxa_alfabetizacao,

            ROUND(
                CAST(
                    media_portugues AS DOUBLE
                ),
                2
            ) AS media_portugues,

            CAST(
                proporcao_aluno_nivel_0 AS DOUBLE
            ) AS proporcao_aluno_nivel_0,

            CAST(
                proporcao_aluno_nivel_1 AS DOUBLE
            ) AS proporcao_aluno_nivel_1,

            CAST(
                proporcao_aluno_nivel_2 AS DOUBLE
            ) AS proporcao_aluno_nivel_2,

            CAST(
                proporcao_aluno_nivel_3 AS DOUBLE
            ) AS proporcao_aluno_nivel_3,

            CAST(
                proporcao_aluno_nivel_4 AS DOUBLE
            ) AS proporcao_aluno_nivel_4,

            CAST(
                proporcao_aluno_nivel_5 AS DOUBLE
            ) AS proporcao_aluno_nivel_5,

            CAST(
                proporcao_aluno_nivel_6 AS DOUBLE
            ) AS proporcao_aluno_nivel_6,

            CAST(
                proporcao_aluno_nivel_7 AS DOUBLE
            ) AS proporcao_aluno_nivel_7,

            CAST(
                proporcao_aluno_nivel_8 AS DOUBLE
            ) AS proporcao_aluno_nivel_8,

            'silver.municipio' AS fonte_gold,

            CURRENT_TIMESTAMP AS gerado_em_utc

        FROM read_parquet(
            '{caminho_municipio}',
            hive_partitioning=false
        )

        WHERE id_municipio IS NOT NULL
    """

    return salvar_gold(
        conexao=conexao,
        nome_tabela="indicador_alfabetizacao_municipio",
        consulta=consulta,
        data_processamento=data_processamento,
    )


def criar_metas_municipio(
    conexao: duckdb.DuckDBPyConnection,
    arquivo_meta_municipio: Path,
    data_processamento: str,
) -> Path:
    """Transforma as metas municipais para formato longitudinal."""

    caminho_meta = preparar_caminho_sql(
        arquivo_meta_municipio
    )

    consulta = f"""
        WITH base AS (
            SELECT
                *,

                CASE
                    WHEN TRY_CAST(rede AS INTEGER) IS NOT NULL
                        THEN TRY_CAST(rede AS INTEGER)

                    WHEN LOWER(
                        TRIM(CAST(rede AS VARCHAR))
                    ) LIKE 'total%'
                        THEN 0

                    WHEN LOWER(
                        TRIM(CAST(rede AS VARCHAR))
                    ) = 'federal'
                        THEN 1

                    WHEN LOWER(
                        TRIM(CAST(rede AS VARCHAR))
                    ) = 'estadual'
                        THEN 2

                    WHEN LOWER(
                        TRIM(CAST(rede AS VARCHAR))
                    ) = 'municipal'
                        THEN 3

                    WHEN LOWER(
                        TRIM(CAST(rede AS VARCHAR))
                    ) = 'privada'
                        THEN 4

                    ELSE NULL
                END AS codigo_rede_normalizado

            FROM read_parquet(
                '{caminho_meta}',
                hive_partitioning=false
            )
        ),

        metas_longas AS (
            SELECT
                CAST(
                    ano AS INTEGER
                ) AS ano_referencia_fonte,

                CAST(
                    id_municipio AS VARCHAR
                ) AS id_municipio,

                codigo_rede_normalizado AS codigo_rede,

                2024 AS ano_meta,

                CAST(
                    meta_alfabetizacao_2024 AS DOUBLE
                ) AS meta_alfabetizacao

            FROM base

            UNION ALL

            SELECT
                CAST(ano AS INTEGER),
                CAST(id_municipio AS VARCHAR),
                codigo_rede_normalizado,
                2025,
                CAST(
                    meta_alfabetizacao_2025 AS DOUBLE
                )

            FROM base

            UNION ALL

            SELECT
                CAST(ano AS INTEGER),
                CAST(id_municipio AS VARCHAR),
                codigo_rede_normalizado,
                2026,
                CAST(
                    meta_alfabetizacao_2026 AS DOUBLE
                )

            FROM base

            UNION ALL

            SELECT
                CAST(ano AS INTEGER),
                CAST(id_municipio AS VARCHAR),
                codigo_rede_normalizado,
                2027,
                CAST(
                    meta_alfabetizacao_2027 AS DOUBLE
                )

            FROM base

            UNION ALL

            SELECT
                CAST(ano AS INTEGER),
                CAST(id_municipio AS VARCHAR),
                codigo_rede_normalizado,
                2028,
                CAST(
                    meta_alfabetizacao_2028 AS DOUBLE
                )

            FROM base

            UNION ALL

            SELECT
                CAST(ano AS INTEGER),
                CAST(id_municipio AS VARCHAR),
                codigo_rede_normalizado,
                2029,
                CAST(
                    meta_alfabetizacao_2029 AS DOUBLE
                )

            FROM base

            UNION ALL

            SELECT
                CAST(ano AS INTEGER),
                CAST(id_municipio AS VARCHAR),
                codigo_rede_normalizado,
                2030,
                CAST(
                    meta_alfabetizacao_2030 AS DOUBLE
                )

            FROM base
        ),

        metas_deduplicadas AS (
            SELECT
                *,

                ROW_NUMBER() OVER (
                    PARTITION BY
                        id_municipio,
                        codigo_rede,
                        ano_meta

                    ORDER BY
                        ano_referencia_fonte DESC
                ) AS numero_linha

            FROM metas_longas

            WHERE
                meta_alfabetizacao IS NOT NULL
                AND codigo_rede IS NOT NULL
                AND id_municipio IS NOT NULL
        )

        SELECT
            id_municipio,

            SUBSTR(
                id_municipio,
                1,
                2
            ) AS id_uf,

            codigo_rede,

            CASE codigo_rede
                WHEN 0 THEN 'Total'
                WHEN 1 THEN 'Federal'
                WHEN 2 THEN 'Estadual'
                WHEN 3 THEN 'Municipal'
                WHEN 4 THEN 'Privada'
                ELSE 'Não identificado'
            END AS rede,

            ano_meta,

            ROUND(
                meta_alfabetizacao,
                2
            ) AS meta_alfabetizacao,

            ano_referencia_fonte,

            'silver.meta_alfabetizacao_municipio'
                AS fonte_gold,

            CURRENT_TIMESTAMP AS gerado_em_utc

        FROM metas_deduplicadas

        WHERE numero_linha = 1
    """

    return salvar_gold(
        conexao=conexao,
        nome_tabela="metas_alfabetizacao_municipio",
        consulta=consulta,
        data_processamento=data_processamento,
    )


def criar_comparacao_meta_resultado(
    conexao: duckdb.DuckDBPyConnection,
    arquivo_indicador: Path,
    arquivo_metas: Path,
    data_processamento: str,
) -> Path:
    """Compara o resultado municipal com a meta do mesmo ano."""

    caminho_indicador = preparar_caminho_sql(
        arquivo_indicador
    )

    caminho_metas = preparar_caminho_sql(
        arquivo_metas
    )

    consulta = f"""
        SELECT
            indicador.ano,
            indicador.id_municipio,
            indicador.id_uf,
            indicador.codigo_rede,
            indicador.rede,
            indicador.taxa_alfabetizacao,
            metas.meta_alfabetizacao,

            ROUND(
                indicador.taxa_alfabetizacao
                - metas.meta_alfabetizacao,
                2
            ) AS diferenca_meta_pp,

            CASE
                WHEN indicador.taxa_alfabetizacao IS NULL
                    THEN 'Sem resultado'

                WHEN metas.meta_alfabetizacao IS NULL
                    THEN 'Sem meta'

                WHEN indicador.taxa_alfabetizacao
                    >= metas.meta_alfabetizacao
                    THEN 'Meta atingida'

                ELSE 'Abaixo da meta'
            END AS status_meta,

            CURRENT_TIMESTAMP AS gerado_em_utc

        FROM read_parquet(
            '{caminho_indicador}',
            hive_partitioning=false
        ) AS indicador

        INNER JOIN read_parquet(
            '{caminho_metas}',
            hive_partitioning=false
        ) AS metas

            ON indicador.id_municipio = metas.id_municipio
            AND indicador.codigo_rede = metas.codigo_rede
            AND indicador.ano = metas.ano_meta
    """

    return salvar_gold(
        conexao=conexao,
        nome_tabela="comparacao_meta_resultado_municipio",
        consulta=consulta,
        data_processamento=data_processamento,
    )


def criar_evolucao_municipio(
    conexao: duckdb.DuckDBPyConnection,
    arquivo_indicador: Path,
    data_processamento: str,
) -> Path:
    """Compara a taxa municipal de alfabetização entre 2023 e 2024."""

    caminho_indicador = preparar_caminho_sql(
        arquivo_indicador
    )

    consulta = f"""
        WITH resultados AS (
            SELECT
                id_municipio,
                id_uf,
                codigo_rede,
                rede,

                MAX(
                    CASE
                        WHEN ano = 2023
                        THEN taxa_alfabetizacao
                    END
                ) AS taxa_alfabetizacao_2023,

                MAX(
                    CASE
                        WHEN ano = 2024
                        THEN taxa_alfabetizacao
                    END
                ) AS taxa_alfabetizacao_2024

            FROM read_parquet(
                '{caminho_indicador}',
                hive_partitioning=false
            )

            GROUP BY
                id_municipio,
                id_uf,
                codigo_rede,
                rede
        )

        SELECT
            id_municipio,
            id_uf,
            codigo_rede,
            rede,
            taxa_alfabetizacao_2023,
            taxa_alfabetizacao_2024,

            ROUND(
                taxa_alfabetizacao_2024
                - taxa_alfabetizacao_2023,
                2
            ) AS variacao_2023_2024_pp,

            CASE
                WHEN taxa_alfabetizacao_2023 IS NULL
                    OR taxa_alfabetizacao_2024 IS NULL
                    THEN 'Sem comparação'

                WHEN taxa_alfabetizacao_2024
                    > taxa_alfabetizacao_2023
                    THEN 'Evolução positiva'

                WHEN taxa_alfabetizacao_2024
                    < taxa_alfabetizacao_2023
                    THEN 'Queda'

                ELSE 'Estável'
            END AS tendencia,

            CURRENT_TIMESTAMP AS gerado_em_utc

        FROM resultados
    """

    return salvar_gold(
        conexao=conexao,
        nome_tabela="evolucao_alfabetizacao_municipio",
        consulta=consulta,
        data_processamento=data_processamento,
    )


def main() -> None:
    """Executa a construção da camada Gold municipal."""

    data_processamento = datetime.now(
        timezone.utc
    ).strftime("%Y-%m-%d")

    print("\nIniciando criação da camada Gold\n")

    arquivo_municipio = localizar_arquivo_mais_recente(
        "municipio"
    )

    arquivo_meta_municipio = localizar_arquivo_mais_recente(
        "meta_alfabetizacao_municipio"
    )

    conexao = duckdb.connect(
        database=":memory:"
    )

    try:
        arquivo_indicador = criar_indicador_municipio(
            conexao=conexao,
            arquivo_municipio=arquivo_municipio,
            data_processamento=data_processamento,
        )

        arquivo_metas = criar_metas_municipio(
            conexao=conexao,
            arquivo_meta_municipio=arquivo_meta_municipio,
            data_processamento=data_processamento,
        )

        criar_comparacao_meta_resultado(
            conexao=conexao,
            arquivo_indicador=arquivo_indicador,
            arquivo_metas=arquivo_metas,
            data_processamento=data_processamento,
        )

        criar_evolucao_municipio(
            conexao=conexao,
            arquivo_indicador=arquivo_indicador,
            data_processamento=data_processamento,
        )

    finally:
        conexao.close()

    print("\nCamada Gold criada com sucesso.")


if __name__ == "__main__":
    main()
