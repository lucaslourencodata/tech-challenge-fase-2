import csv
from datetime import datetime, timezone
from pathlib import Path
import sys

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[2]

GOLD_PATH = PROJECT_ROOT / "data" / "gold"

REPORT_PATH = (
    PROJECT_ROOT
    / "docs"
    / "relatorio_qualidade_gold.csv"
)

TABELAS_GOLD = [
    "indicador_alfabetizacao_municipio",
    "metas_alfabetizacao_municipio",
    "comparacao_meta_resultado_municipio",
    "evolucao_alfabetizacao_municipio",
]


def localizar_arquivo_mais_recente(tabela: str) -> Path:
    """Localiza o arquivo mais recente de uma tabela Gold."""

    pasta_tabela = GOLD_PATH / tabela

    pastas_processamento = sorted(
        pasta_tabela.glob("data_processamento=*"),
        reverse=True,
    )

    if not pastas_processamento:
        raise FileNotFoundError(
            f"Nenhum processamento encontrado para {tabela}."
        )

    arquivo = pastas_processamento[0] / "dados.parquet"

    if not arquivo.exists():
        raise FileNotFoundError(
            f"Arquivo não encontrado: {arquivo}"
        )

    return arquivo


def criar_relacao_parquet(arquivo: Path) -> str:
    """Cria uma expressão SQL para consultar um arquivo Parquet."""

    caminho = arquivo.as_posix().replace("'", "''")

    return (
        "read_parquet("
        f"'{caminho}', "
        "hive_partitioning=false"
        ")"
    )


def executar_valor(
    conexao: duckdb.DuckDBPyConnection,
    consulta: str,
) -> int:
    """Executa uma consulta e retorna o primeiro valor."""

    resultado = conexao.execute(consulta).fetchone()

    if resultado is None:
        return 0

    return int(resultado[0])


def adicionar_resultado(
    resultados: list[dict[str, object]],
    verificacao: str,
    tabela: str,
    valor_encontrado: int,
    valor_esperado: int,
    detalhe: str,
    status_forcado: str | None = None,
) -> None:
    """Adiciona uma verificação ao relatório."""

    if status_forcado is not None:
        status = status_forcado
    else:
        status = (
            "APROVADO"
            if valor_encontrado == valor_esperado
            else "REPROVADO"
        )

    resultados.append(
        {
            "verificacao": verificacao,
            "tabela": tabela,
            "status": status,
            "valor_encontrado": valor_encontrado,
            "valor_esperado": valor_esperado,
            "detalhe": detalhe,
        }
    )

    simbolos = {
        "APROVADO": "OK",
        "REPROVADO": "ERRO",
        "AVISO": "AVISO",
    }

    print(
        f"[{simbolos.get(status, status)}] "
        f"{verificacao} | "
        f"{tabela} | "
        f"encontrado={valor_encontrado:,} | "
        f"esperado={valor_esperado:,}"
    )


def validar_quantidade_minima(
    conexao: duckdb.DuckDBPyConnection,
    tabela: str,
    relacao: str,
    resultados: list[dict[str, object]],
) -> None:
    """Verifica se a tabela Gold possui registros."""

    quantidade = executar_valor(
        conexao,
        f"SELECT COUNT(*) FROM {relacao}",
    )

    status = (
        "APROVADO"
        if quantidade > 0
        else "REPROVADO"
    )

    adicionar_resultado(
        resultados=resultados,
        verificacao="Tabela não vazia",
        tabela=tabela,
        valor_encontrado=quantidade,
        valor_esperado=1,
        detalhe="A tabela Gold deve possuir pelo menos um registro.",
        status_forcado=status,
    )


def validar_duplicidades(
    conexao: duckdb.DuckDBPyConnection,
    tabela: str,
    relacao: str,
    resultados: list[dict[str, object]],
) -> None:
    """Verifica duplicidades nas chaves analíticas."""

    chaves_por_tabela = {
        "indicador_alfabetizacao_municipio": [
            "ano",
            "id_municipio",
            "codigo_rede",
            "serie",
        ],
        "metas_alfabetizacao_municipio": [
            "id_municipio",
            "codigo_rede",
            "ano_meta",
        ],
        "comparacao_meta_resultado_municipio": [
            "ano",
            "id_municipio",
            "codigo_rede",
        ],
        "evolucao_alfabetizacao_municipio": [
            "id_municipio",
            "codigo_rede",
        ],
    }

    chaves = chaves_por_tabela[tabela]
    colunas = ", ".join(chaves)

    consulta = f"""
        SELECT COUNT(*)
        FROM (
            SELECT
                {colunas},
                COUNT(*) AS quantidade
            FROM {relacao}
            GROUP BY {colunas}
            HAVING COUNT(*) > 1
        )
    """

    quantidade_duplicidades = executar_valor(
        conexao,
        consulta,
    )

    adicionar_resultado(
        resultados=resultados,
        verificacao="Duplicidades na chave analítica",
        tabela=tabela,
        valor_encontrado=quantidade_duplicidades,
        valor_esperado=0,
        detalhe=(
            "Não deve existir mais de um registro para a mesma "
            "combinação de chaves."
        ),
    )


def validar_chaves_nulas(
    conexao: duckdb.DuckDBPyConnection,
    tabela: str,
    relacao: str,
    resultados: list[dict[str, object]],
) -> None:
    """Verifica valores nulos nas chaves principais."""

    chaves_por_tabela = {
        "indicador_alfabetizacao_municipio": [
            "ano",
            "id_municipio",
            "codigo_rede",
        ],
        "metas_alfabetizacao_municipio": [
            "id_municipio",
            "codigo_rede",
            "ano_meta",
        ],
        "comparacao_meta_resultado_municipio": [
            "ano",
            "id_municipio",
            "codigo_rede",
        ],
        "evolucao_alfabetizacao_municipio": [
            "id_municipio",
            "codigo_rede",
        ],
    }

    for coluna in chaves_por_tabela[tabela]:
        quantidade_nulos = executar_valor(
            conexao,
            f"""
                SELECT COUNT(*)
                FROM {relacao}
                WHERE
                    {coluna} IS NULL
                    OR TRIM(
                        CAST({coluna} AS VARCHAR)
                    ) = ''
            """,
        )

        adicionar_resultado(
            resultados=resultados,
            verificacao=f"Nulos na chave {coluna}",
            tabela=tabela,
            valor_encontrado=quantidade_nulos,
            valor_esperado=0,
            detalhe=f"A coluna {coluna} faz parte da chave analítica.",
        )


def validar_faixa_percentual(
    conexao: duckdb.DuckDBPyConnection,
    tabela: str,
    relacao: str,
    coluna: str,
    resultados: list[dict[str, object]],
) -> None:
    """Valida se um percentual está entre zero e cem."""

    quantidade_invalidos = executar_valor(
        conexao,
        f"""
            SELECT COUNT(*)
            FROM {relacao}
            WHERE
                {coluna} IS NOT NULL
                AND (
                    {coluna} < 0
                    OR {coluna} > 100
                )
        """,
    )

    adicionar_resultado(
        resultados=resultados,
        verificacao=f"Faixa válida de {coluna}",
        tabela=tabela,
        valor_encontrado=quantidade_invalidos,
        valor_esperado=0,
        detalhe=f"A coluna {coluna} deve estar entre 0 e 100.",
    )


def validar_comparacao_meta_resultado(
    conexao: duckdb.DuckDBPyConnection,
    relacao_indicador: str,
    relacao_metas: str,
    relacao_comparacao: str,
    resultados: list[dict[str, object]],
) -> None:
    """Valida a comparação entre resultados e metas compatíveis."""

    quantidade_esperada = executar_valor(
        conexao,
        f"""
            SELECT COUNT(*)
            FROM {relacao_indicador} AS indicador

            INNER JOIN {relacao_metas} AS metas
                ON indicador.id_municipio = metas.id_municipio
                AND indicador.codigo_rede = metas.codigo_rede
                AND indicador.ano = metas.ano_meta
        """,
    )

    quantidade_comparacao = executar_valor(
        conexao,
        f"SELECT COUNT(*) FROM {relacao_comparacao}",
    )

    adicionar_resultado(
        resultados=resultados,
        verificacao="Reconciliação da comparação",
        tabela="comparacao_meta_resultado_municipio",
        valor_encontrado=quantidade_comparacao,
        valor_esperado=quantidade_esperada,
        detalhe=(
            "A tabela deve conter todas as combinações compatíveis "
            "entre indicador e meta."
        ),
    )

    status_invalidos = executar_valor(
        conexao,
        f"""
            SELECT COUNT(*)
            FROM {relacao_comparacao}
            WHERE
                status_meta NOT IN (
                    'Meta atingida',
                    'Abaixo da meta',
                    'Sem resultado'
                )
                OR status_meta IS NULL
        """,
    )

    adicionar_resultado(
        resultados=resultados,
        verificacao="Domínio do status da meta",
        tabela="comparacao_meta_resultado_municipio",
        valor_encontrado=status_invalidos,
        valor_esperado=0,
        detalhe=(
            "O status deve representar uma comparação válida "
            "entre indicador e meta."
        ),
    )

    metas_nulas = executar_valor(
        conexao,
        f"""
            SELECT COUNT(*)
            FROM {relacao_comparacao}
            WHERE meta_alfabetizacao IS NULL
        """,
    )

    adicionar_resultado(
        resultados=resultados,
        verificacao="Metas nulas na comparação",
        tabela="comparacao_meta_resultado_municipio",
        valor_encontrado=metas_nulas,
        valor_esperado=0,
        detalhe=(
            "Todos os registros da comparação devem possuir "
            "uma meta correspondente."
        ),
    )


def validar_evolucao(
    conexao: duckdb.DuckDBPyConnection,
    relacao: str,
    resultados: list[dict[str, object]],
) -> None:
    """Valida o domínio de tendência da evolução municipal."""

    tendencias_invalidas = executar_valor(
        conexao,
        f"""
            SELECT COUNT(*)
            FROM {relacao}
            WHERE
                tendencia NOT IN (
                    'Evolução positiva',
                    'Queda',
                    'Estável',
                    'Sem comparação'
                )
                OR tendencia IS NULL
        """,
    )

    adicionar_resultado(
        resultados=resultados,
        verificacao="Domínio da tendência",
        tabela="evolucao_alfabetizacao_municipio",
        valor_encontrado=tendencias_invalidas,
        valor_esperado=0,
        detalhe="A tendência deve pertencer ao domínio definido.",
    )

    sem_comparacao = executar_valor(
        conexao,
        f"""
            SELECT COUNT(*)
            FROM {relacao}
            WHERE tendencia = 'Sem comparação'
        """,
    )

    status = (
        "AVISO"
        if sem_comparacao > 0
        else "APROVADO"
    )

    adicionar_resultado(
        resultados=resultados,
        verificacao="Municípios sem comparação temporal",
        tabela="evolucao_alfabetizacao_municipio",
        valor_encontrado=sem_comparacao,
        valor_esperado=0,
        detalhe=(
            "Alguns registros não possuem resultado disponível "
            "nos dois anos analisados."
        ),
        status_forcado=status,
    )


def salvar_relatorio(
    resultados: list[dict[str, object]],
) -> None:
    """Salva as verificações em arquivo CSV."""

    REPORT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    campos = [
        "verificacao",
        "tabela",
        "status",
        "valor_encontrado",
        "valor_esperado",
        "detalhe",
    ]

    with REPORT_PATH.open(
        mode="w",
        encoding="utf-8-sig",
        newline="",
    ) as arquivo:
        escritor = csv.DictWriter(
            arquivo,
            fieldnames=campos,
        )

        escritor.writeheader()
        escritor.writerows(resultados)


def main() -> None:
    """Executa as validações da camada Gold."""

    print("\nIniciando validação da camada Gold\n")

    inicio = datetime.now(timezone.utc)

    conexao = duckdb.connect(
        database=":memory:"
    )

    resultados: list[dict[str, object]] = []
    relacoes: dict[str, str] = {}

    try:
        for tabela in TABELAS_GOLD:
            arquivo = localizar_arquivo_mais_recente(
                tabela
            )

            relacao = criar_relacao_parquet(
                arquivo
            )

            relacoes[tabela] = relacao

            validar_quantidade_minima(
                conexao=conexao,
                tabela=tabela,
                relacao=relacao,
                resultados=resultados,
            )

            validar_duplicidades(
                conexao=conexao,
                tabela=tabela,
                relacao=relacao,
                resultados=resultados,
            )

            validar_chaves_nulas(
                conexao=conexao,
                tabela=tabela,
                relacao=relacao,
                resultados=resultados,
            )

        validar_faixa_percentual(
            conexao=conexao,
            tabela="indicador_alfabetizacao_municipio",
            relacao=relacoes[
                "indicador_alfabetizacao_municipio"
            ],
            coluna="taxa_alfabetizacao",
            resultados=resultados,
        )

        validar_faixa_percentual(
            conexao=conexao,
            tabela="metas_alfabetizacao_municipio",
            relacao=relacoes[
                "metas_alfabetizacao_municipio"
            ],
            coluna="meta_alfabetizacao",
            resultados=resultados,
        )

        validar_comparacao_meta_resultado(
            conexao=conexao,
            relacao_indicador=relacoes[
                "indicador_alfabetizacao_municipio"
            ],
            relacao_metas=relacoes[
                "metas_alfabetizacao_municipio"
            ],
            relacao_comparacao=relacoes[
                "comparacao_meta_resultado_municipio"
            ],
            resultados=resultados,
        )

        validar_evolucao(
            conexao=conexao,
            relacao=relacoes[
                "evolucao_alfabetizacao_municipio"
            ],
            resultados=resultados,
        )

    finally:
        conexao.close()

    salvar_relatorio(resultados)

    quantidade_aprovada = sum(
        resultado["status"] == "APROVADO"
        for resultado in resultados
    )

    quantidade_avisos = sum(
        resultado["status"] == "AVISO"
        for resultado in resultados
    )

    quantidade_reprovada = sum(
        resultado["status"] == "REPROVADO"
        for resultado in resultados
    )

    duracao = datetime.now(timezone.utc) - inicio

    print("\nValidação Gold concluída.")
    print(f"Verificações executadas: {len(resultados)}")
    print(f"Verificações aprovadas: {quantidade_aprovada}")
    print(f"Avisos encontrados: {quantidade_avisos}")
    print(f"Verificações reprovadas: {quantidade_reprovada}")
    print(f"Duração: {duracao}")
    print(f"Relatório: {REPORT_PATH}")

    if quantidade_reprovada > 0:
        print(
            "\nExistem erros de qualidade na camada Gold."
        )
        sys.exit(1)

    if quantidade_avisos > 0:
        print(
            "\nA camada Gold foi aprovada com avisos."
        )
        return

    print(
        "\nTodos os testes da camada Gold foram aprovados."
    )


if __name__ == "__main__":
    main()