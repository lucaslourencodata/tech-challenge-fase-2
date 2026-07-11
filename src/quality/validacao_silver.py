import csv
from datetime import datetime, timezone
from pathlib import Path
import sys

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_PATH = PROJECT_ROOT / "data" / "bronze" / "batch"
SILVER_PATH = PROJECT_ROOT / "data" / "silver"
REPORT_PATH = PROJECT_ROOT / "docs" / "relatorio_qualidade_silver.csv"

TABELAS = [
    "uf",
    "municipio",
    "meta_alfabetizacao_brasil",
    "meta_alfabetizacao_uf",
    "meta_alfabetizacao_municipio",
    "alunos",
]

QUANTIDADE_ESPERADA_POR_ANO = {
    2023: 1_747_439,
    2024: 2_120_560,
}


def localizar_pasta_mais_recente(
    pasta_base: Path,
    padrao: str,
) -> Path:
    """Retorna a pasta de processamento mais recente."""

    pastas = sorted(
        pasta_base.glob(padrao),
        reverse=True,
    )

    if not pastas:
        raise FileNotFoundError(
            f"Nenhuma pasta encontrada em: {pasta_base}"
        )

    return pastas[0]


def localizar_arquivos_bronze(tabela: str) -> list[Path]:
    """Localiza os arquivos mais recentes da camada Bronze."""

    if tabela != "alunos":
        pasta = localizar_pasta_mais_recente(
            BRONZE_PATH / tabela,
            "data_ingestao=*",
        )

        arquivos = sorted(pasta.glob("*.parquet"))

    else:
        arquivos = []

        pasta_alunos = BRONZE_PATH / "alunos"

        for pasta_ano in sorted(pasta_alunos.glob("ano=*")):
            pasta_ingestao = localizar_pasta_mais_recente(
                pasta_ano,
                "data_ingestao=*",
            )

            arquivos.extend(
                sorted(pasta_ingestao.glob("*.parquet"))
            )

    if not arquivos:
        raise FileNotFoundError(
            f"Nenhum arquivo Bronze encontrado para {tabela}."
        )

    return arquivos


def localizar_arquivos_silver(tabela: str) -> list[Path]:
    """Localiza os arquivos mais recentes da camada Silver."""

    if tabela != "alunos":
        pasta = localizar_pasta_mais_recente(
            SILVER_PATH / tabela,
            "data_processamento=*",
        )

        arquivos = sorted(pasta.glob("*.parquet"))

    else:
        arquivos = []

        pasta_alunos = SILVER_PATH / "alunos"

        for pasta_ano in sorted(pasta_alunos.glob("ano=*")):
            pasta_processamento = localizar_pasta_mais_recente(
                pasta_ano,
                "data_processamento=*",
            )

            arquivos.extend(
                sorted(pasta_processamento.glob("*.parquet"))
            )

    if not arquivos:
        raise FileNotFoundError(
            f"Nenhum arquivo Silver encontrado para {tabela}."
        )

    return arquivos


def criar_relacao_parquet(arquivos: list[Path]) -> str:
    """Cria uma expressão SQL para leitura dos arquivos Parquet."""

    caminhos = []

    for arquivo in arquivos:
        caminho = arquivo.as_posix().replace("'", "''")
        caminhos.append(f"'{caminho}'")

    lista_arquivos = ", ".join(caminhos)

    return (
        "read_parquet("
        f"[{lista_arquivos}], "
        "union_by_name=true, "
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
    """Adiciona uma validação ao relatório."""

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
        f"[{simbolos.get(status, status)}] {verificacao} | "
        f"{tabela} | "
        f"encontrado={valor_encontrado:,} | "
        f"esperado={valor_esperado:,}"
    )


def validar_quantidade_bronze_silver(
    conexao: duckdb.DuckDBPyConnection,
    tabela: str,
    relacao_bronze: str,
    relacao_silver: str,
    resultados: list[dict[str, object]],
) -> None:
    """Compara a quantidade de linhas da Bronze e da Silver."""

    quantidade_bronze = executar_valor(
        conexao,
        f"SELECT COUNT(*) FROM {relacao_bronze}",
    )

    quantidade_silver = executar_valor(
        conexao,
        f"SELECT COUNT(*) FROM {relacao_silver}",
    )

    adicionar_resultado(
        resultados=resultados,
        verificacao="Reconciliação Bronze x Silver",
        tabela=tabela,
        valor_encontrado=quantidade_silver,
        valor_esperado=quantidade_bronze,
        detalhe="A quantidade de linhas deve ser preservada.",
    )


def validar_duplicidades(
    conexao: duckdb.DuckDBPyConnection,
    tabela: str,
    relacao_silver: str,
    resultados: list[dict[str, object]],
) -> None:
    """Valida duplicidades exatas ou por chave do aluno."""

    if tabela == "alunos":
        consulta = f"""
            SELECT COUNT(*)
            FROM (
                SELECT
                    ano,
                    id_aluno,
                    COUNT(*) AS quantidade
                FROM {relacao_silver}
                WHERE id_aluno IS NOT NULL
                GROUP BY
                    ano,
                    id_aluno
                HAVING COUNT(*) > 1
            )
        """

        detalhe = (
            "Não deve existir repetição da chave ano + id_aluno."
        )

    else:
        consulta = f"""
            SELECT
                (
                    SELECT COUNT(*)
                    FROM {relacao_silver}
                )
                -
                (
                    SELECT COUNT(*)
                    FROM (
                        SELECT DISTINCT *
                        FROM {relacao_silver}
                    )
                )
        """

        detalhe = "Não devem existir registros exatamente duplicados."

    quantidade_duplicidades = executar_valor(
        conexao,
        consulta,
    )

    adicionar_resultado(
        resultados=resultados,
        verificacao="Duplicidades",
        tabela=tabela,
        valor_encontrado=quantidade_duplicidades,
        valor_esperado=0,
        detalhe=detalhe,
    )


def validar_nulos_alunos(
    conexao: duckdb.DuckDBPyConnection,
    relacao_alunos: str,
    resultados: list[dict[str, object]],
) -> None:
    """Valida campos obrigatórios da tabela de alunos."""

    colunas_obrigatorias = [
        "ano",
        "id_municipio",
        "id_aluno",
        "presenca",
    ]

    for coluna in colunas_obrigatorias:
        consulta = f"""
            SELECT COUNT(*)
            FROM {relacao_alunos}
            WHERE
                {coluna} IS NULL
                OR TRIM(CAST({coluna} AS VARCHAR)) = ''
        """

        quantidade_nulos = executar_valor(
            conexao,
            consulta,
        )

        adicionar_resultado(
            resultados=resultados,
            verificacao=f"Nulos na coluna {coluna}",
            tabela="alunos",
            valor_encontrado=quantidade_nulos,
            valor_esperado=0,
            detalhe=f"A coluna {coluna} é obrigatória.",
        )


def validar_anos_alunos(
    conexao: duckdb.DuckDBPyConnection,
    relacao_alunos: str,
    resultados: list[dict[str, object]],
) -> None:
    """Valida anos e quantidades conhecidas da fonte."""

    anos_invalidos = executar_valor(
        conexao,
        f"""
            SELECT COUNT(*)
            FROM {relacao_alunos}
            WHERE ano NOT IN (2023, 2024)
               OR ano IS NULL
        """,
    )

    adicionar_resultado(
        resultados=resultados,
        verificacao="Domínio de anos",
        tabela="alunos",
        valor_encontrado=anos_invalidos,
        valor_esperado=0,
        detalhe="Somente os anos 2023 e 2024 são esperados.",
    )

    for ano, quantidade_esperada in QUANTIDADE_ESPERADA_POR_ANO.items():
        quantidade_encontrada = executar_valor(
            conexao,
            f"""
                SELECT COUNT(*)
                FROM {relacao_alunos}
                WHERE ano = {ano}
            """,
        )

        adicionar_resultado(
            resultados=resultados,
            verificacao=f"Quantidade de alunos em {ano}",
            tabela="alunos",
            valor_encontrado=quantidade_encontrada,
            valor_esperado=quantidade_esperada,
            detalhe="Comparação com a quantidade da tabela de origem.",
        )


def validar_integridade_municipios(
    conexao: duckdb.DuckDBPyConnection,
    relacao_alunos: str,
    relacao_municipio: str,
    relacao_meta_municipio: str,
    resultados: list[dict[str, object]],
) -> None:
    """Analisa a presença dos municípios entre as tabelas."""

    alunos_sem_resultado_municipal = executar_valor(
        conexao,
        f"""
            SELECT COUNT(*)
            FROM (
                SELECT DISTINCT id_municipio
                FROM {relacao_alunos}
                WHERE id_municipio IS NOT NULL
            ) AS alunos
            LEFT JOIN (
                SELECT DISTINCT id_municipio
                FROM {relacao_municipio}
                WHERE id_municipio IS NOT NULL
            ) AS municipios
                USING (id_municipio)
            WHERE municipios.id_municipio IS NULL
        """,
    )

    status_alunos = (
        "AVISO"
        if alunos_sem_resultado_municipal > 0
        else None
    )

    adicionar_resultado(
        resultados=resultados,
        verificacao="Municípios sem resultado agregado",
        tabela="alunos",
        valor_encontrado=alunos_sem_resultado_municipal,
        valor_esperado=0,
        detalhe=(
            "A tabela municipio contém resultados agregados, não um "
            "cadastro completo. Municípios presentes em alunos podem não "
            "ter resultado municipal publicado. O caso deve ser registrado "
            "como aviso, sem excluir os alunos."
        ),
        status_forcado=status_alunos,
    )

    metas_sem_municipio = executar_valor(
        conexao,
        f"""
            SELECT COUNT(*)
            FROM (
                SELECT DISTINCT id_municipio
                FROM {relacao_meta_municipio}
                WHERE id_municipio IS NOT NULL
            ) AS metas
            LEFT JOIN (
                SELECT DISTINCT id_municipio
                FROM {relacao_municipio}
                WHERE id_municipio IS NOT NULL
            ) AS municipios
                USING (id_municipio)
            WHERE municipios.id_municipio IS NULL
        """,
    )

    adicionar_resultado(
        resultados=resultados,
        verificacao="Metas sem resultado municipal",
        tabela="meta_alfabetizacao_municipio",
        valor_encontrado=metas_sem_municipio,
        valor_esperado=0,
        detalhe=(
            "Municípios com meta devem possuir resultado agregado "
            "na tabela municipio."
        ),
    )


def salvar_relatorio(
    resultados: list[dict[str, object]],
) -> None:
    """Salva o relatório das validações em CSV."""

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
    """Executa as validações de qualidade da camada Silver."""

    print("\nIniciando validação da camada Silver\n")

    inicio = datetime.now(timezone.utc)
    conexao = duckdb.connect(database=":memory:")
    resultados: list[dict[str, object]] = []
    relacoes_silver: dict[str, str] = {}

    try:
        for tabela in TABELAS:
            arquivos_bronze = localizar_arquivos_bronze(tabela)
            arquivos_silver = localizar_arquivos_silver(tabela)

            relacao_bronze = criar_relacao_parquet(
                arquivos_bronze
            )

            relacao_silver = criar_relacao_parquet(
                arquivos_silver
            )

            relacoes_silver[tabela] = relacao_silver

            validar_quantidade_bronze_silver(
                conexao=conexao,
                tabela=tabela,
                relacao_bronze=relacao_bronze,
                relacao_silver=relacao_silver,
                resultados=resultados,
            )

            validar_duplicidades(
                conexao=conexao,
                tabela=tabela,
                relacao_silver=relacao_silver,
                resultados=resultados,
            )

        validar_nulos_alunos(
            conexao=conexao,
            relacao_alunos=relacoes_silver["alunos"],
            resultados=resultados,
        )

        validar_anos_alunos(
            conexao=conexao,
            relacao_alunos=relacoes_silver["alunos"],
            resultados=resultados,
        )

        validar_integridade_municipios(
            conexao=conexao,
            relacao_alunos=relacoes_silver["alunos"],
            relacao_municipio=relacoes_silver["municipio"],
            relacao_meta_municipio=(
                relacoes_silver["meta_alfabetizacao_municipio"]
            ),
            resultados=resultados,
        )

    finally:
        conexao.close()

    salvar_relatorio(resultados)

    quantidade_reprovada = sum(
        resultado["status"] == "REPROVADO"
        for resultado in resultados
    )

    quantidade_avisos = sum(
        resultado["status"] == "AVISO"
        for resultado in resultados
    )

    quantidade_aprovada = sum(
        resultado["status"] == "APROVADO"
        for resultado in resultados
    )

    duracao = datetime.now(timezone.utc) - inicio

    print("\nValidação concluída.")
    print(f"Verificações executadas: {len(resultados)}")
    print(f"Verificações aprovadas: {quantidade_aprovada}")
    print(f"Avisos encontrados: {quantidade_avisos}")
    print(f"Verificações reprovadas: {quantidade_reprovada}")
    print(f"Duração: {duracao}")
    print(f"Relatório: {REPORT_PATH}")

    if quantidade_reprovada > 0:
        print(
            "\nAtenção: existem verificações reprovadas "
            "que precisam ser corrigidas."
        )
        sys.exit(1)

    if quantidade_avisos > 0:
        print(
            "\nA pipeline foi aprovada com avisos. "
            "Consulte o relatório de qualidade."
        )
        return

    print("\nTodos os testes de qualidade foram aprovados.")


if __name__ == "__main__":
    main()