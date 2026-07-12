import argparse
import csv
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow.parquet as parquet


PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_PATH = PROJECT_ROOT / "data"
LOGS_PATH = PROJECT_ROOT / "logs"
EXECUTIONS_PATH = LOGS_PATH / "execucoes"
SUMMARY_CSV_PATH = LOGS_PATH / "resumo_execucoes.csv"
HISTORY_JSONL_PATH = LOGS_PATH / "historico_execucoes.jsonl"
ALERTS_PATH = LOGS_PATH / "alertas.log"


ETAPAS = {
    "ingestao_referencias": {
        "descricao": "Ingestão batch das tabelas de referência",
        "script": "src/ingestion/ingestao_batch.py",
        "argumentos": [],
    },
    "ingestao_alunos": {
        "descricao": "Ingestão batch dos microdados de alunos",
        "script": "src/ingestion/ingestao_alunos.py",
        "argumentos": [],
    },
    "transformacao_referencias": {
        "descricao": "Transformação Bronze para Silver das referências",
        "script": "src/transformation/transformacao_referencias.py",
        "argumentos": [],
    },
    "transformacao_alunos": {
        "descricao": "Transformação Bronze para Silver dos alunos",
        "script": "src/transformation/transformacao_alunos.py",
        "argumentos": [],
    },
    "validacao_silver": {
        "descricao": "Validação de qualidade da camada Silver",
        "script": "src/quality/validacao_silver.py",
        "argumentos": [],
    },
    "criacao_gold": {
        "descricao": "Criação das tabelas analíticas da camada Gold",
        "script": "src/transformation/criar_camada_gold.py",
        "argumentos": [],
    },
    "validacao_gold": {
        "descricao": "Validação de qualidade da camada Gold",
        "script": "src/quality/validacao_gold.py",
        "argumentos": [],
    },
    "produtor_streaming": {
        "descricao": "Produção de eventos simulados",
        "script": "src/streaming/produtor_eventos.py",
        "argumentos": [
            "--quantidade",
            "10",
            "--intervalo",
            "0.2",
        ],
    },
    "consumidor_streaming": {
        "descricao": "Consumo e tratamento dos eventos simulados",
        "script": "src/streaming/consumidor_eventos.py",
        "argumentos": [],
    },
    "criacao_datasets_cloud": {
        "descricao": "Criação dos datasets no BigQuery",
        "script": "src/cloud/criar_datasets_bigquery.py",
        "argumentos": [],
    },
    "carga_bronze_cloud": {
        "descricao": "Carga da camada Bronze no BigQuery",
        "script": "src/cloud/carregar_bronze_bigquery.py",
        "argumentos": [],
    },
    "carga_silver_cloud": {
        "descricao": "Carga da camada Silver no BigQuery",
        "script": "src/cloud/carregar_silver_bigquery.py",
        "argumentos": [],
    },
    "carga_gold_cloud": {
        "descricao": "Carga da camada Gold no BigQuery",
        "script": "src/cloud/carregar_gold_bigquery.py",
        "argumentos": [],
    },
}


PERFIS = {
    "qualidade": [
        "validacao_silver",
        "validacao_gold",
    ],
    "local": [
        "transformacao_referencias",
        "transformacao_alunos",
        "validacao_silver",
        "criacao_gold",
        "validacao_gold",
    ],
    "streaming": [
        "produtor_streaming",
        "consumidor_streaming",
    ],
    "cloud-gold": [
        "criacao_datasets_cloud",
        "carga_gold_cloud",
    ],
    "completo": [
        "ingestao_referencias",
        "ingestao_alunos",
        "transformacao_referencias",
        "transformacao_alunos",
        "validacao_silver",
        "criacao_gold",
        "validacao_gold",
        "produtor_streaming",
        "consumidor_streaming",
        "criacao_datasets_cloud",
        "carga_bronze_cloud",
        "carga_silver_cloud",
        "carga_gold_cloud",
    ],
}


def formatar_bytes(quantidade: int) -> str:
    """Converte uma quantidade de bytes para uma unidade legível."""

    unidades = [
        "B",
        "KB",
        "MB",
        "GB",
        "TB",
    ]

    valor = float(quantidade)

    for unidade in unidades:
        if valor < 1024 or unidade == unidades[-1]:
            return f"{valor:.2f} {unidade}"

        valor /= 1024

    return f"{quantidade} B"


def contar_linhas_arquivo(arquivo: Path) -> int:
    """Conta registros de formatos conhecidos sem carregar tudo na memória."""

    try:
        if arquivo.suffix.lower() == ".parquet":
            return parquet.ParquetFile(
                arquivo
            ).metadata.num_rows

        if arquivo.suffix.lower() == ".jsonl":
            with arquivo.open(
                mode="r",
                encoding="utf-8",
            ) as conteudo:
                return sum(
                    1
                    for linha in conteudo
                    if linha.strip()
                )

        if arquivo.suffix.lower() == ".csv":
            with arquivo.open(
                mode="r",
                encoding="utf-8-sig",
                errors="replace",
            ) as conteudo:
                quantidade = sum(1 for _ in conteudo)

            return max(0, quantidade - 1)

    except Exception:
        return 0

    return 0


def coletar_metricas_camada(
    camada: str,
) -> dict[str, int]:
    """Coleta arquivos, tamanho e registros de uma camada."""

    pasta = DATA_PATH / camada

    if not pasta.exists():
        return {
            "arquivos": 0,
            "bytes": 0,
            "registros": 0,
        }

    arquivos = [
        arquivo
        for arquivo in pasta.rglob("*")
        if arquivo.is_file()
    ]

    total_bytes = sum(
        arquivo.stat().st_size
        for arquivo in arquivos
    )

    total_registros = sum(
        contar_linhas_arquivo(arquivo)
        for arquivo in arquivos
    )

    return {
        "arquivos": len(arquivos),
        "bytes": total_bytes,
        "registros": total_registros,
    }


def coletar_metricas_dados() -> dict[str, dict[str, int]]:
    """Coleta métricas das camadas Bronze, Silver e Gold."""

    return {
        camada: coletar_metricas_camada(camada)
        for camada in [
            "bronze",
            "silver",
            "gold",
        ]
    }


def salvar_log_etapa(
    pasta_execucao: Path,
    nome_etapa: str,
    comando: list[str],
    stdout: str,
    stderr: str,
    returncode: int,
) -> Path:
    """Salva a saída completa de uma etapa."""

    arquivo_log = (
        pasta_execucao
        / f"{nome_etapa}.log"
    )

    conteudo = [
        f"ETAPA: {nome_etapa}",
        f"COMANDO: {' '.join(comando)}",
        f"CODIGO_RETORNO: {returncode}",
        "",
        "===== STDOUT =====",
        stdout or "(sem saída)",
        "",
        "===== STDERR =====",
        stderr or "(sem erros)",
        "",
    ]

    arquivo_log.write_text(
        "\n".join(conteudo),
        encoding="utf-8",
    )

    return arquivo_log


def registrar_alerta(
    run_id: str,
    etapa: str,
    mensagem: str,
) -> None:
    """Registra uma falha operacional no arquivo de alertas."""

    LOGS_PATH.mkdir(
        parents=True,
        exist_ok=True,
    )

    momento = datetime.now(
        timezone.utc
    ).isoformat()

    with ALERTS_PATH.open(
        mode="a",
        encoding="utf-8",
    ) as arquivo:
        arquivo.write(
            f"{momento} | "
            f"execucao={run_id} | "
            f"etapa={etapa} | "
            f"{mensagem}\n"
        )


def executar_etapa(
    run_id: str,
    nome_etapa: str,
    pasta_execucao: Path,
) -> dict[str, Any]:
    """Executa um script da pipeline e registra suas métricas."""

    configuracao = ETAPAS[nome_etapa]

    script = PROJECT_ROOT / configuracao["script"]

    if not script.exists():
        raise FileNotFoundError(
            f"Script não encontrado: {script}"
        )

    comando = [
        sys.executable,
        str(script),
        *configuracao["argumentos"],
    ]

    inicio = datetime.now(
        timezone.utc
    )

    inicio_cronometro = time.perf_counter()

    print(
        f"\n[INÍCIO] {nome_etapa}"
    )
    print(
        f"Descrição: {configuracao['descricao']}"
    )

    ambiente = os.environ.copy()
    ambiente["PYTHONUTF8"] = "1"

    processo = subprocess.run(
        comando,
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=ambiente,
        check=False,
    )

    duracao_segundos = round(
        time.perf_counter()
        - inicio_cronometro,
        3,
    )

    fim = datetime.now(
        timezone.utc
    )

    status = (
        "SUCESSO"
        if processo.returncode == 0
        else "FALHA"
    )

    arquivo_log = salvar_log_etapa(
        pasta_execucao=pasta_execucao,
        nome_etapa=nome_etapa,
        comando=comando,
        stdout=processo.stdout,
        stderr=processo.stderr,
        returncode=processo.returncode,
    )

    print(
        f"[{status}] {nome_etapa} | "
        f"duração={duracao_segundos:.3f}s"
    )
    print(
        f"Log: {arquivo_log}"
    )

    if processo.returncode != 0:
        mensagem = (
            processo.stderr.strip()
            or processo.stdout.strip()
            or "Falha sem mensagem retornada."
        )

        registrar_alerta(
            run_id=run_id,
            etapa=nome_etapa,
            mensagem=mensagem.replace(
                "\n",
                " ",
            )[:1000],
        )

    return {
        "etapa": nome_etapa,
        "descricao": configuracao["descricao"],
        "status": status,
        "codigo_retorno": processo.returncode,
        "inicio_utc": inicio.isoformat(),
        "fim_utc": fim.isoformat(),
        "duracao_segundos": duracao_segundos,
        "arquivo_log": str(arquivo_log),
    }


def salvar_historico_jsonl(
    resultado_execucao: dict[str, Any],
) -> None:
    """Acrescenta a execução ao histórico em JSON Lines."""

    LOGS_PATH.mkdir(
        parents=True,
        exist_ok=True,
    )

    with HISTORY_JSONL_PATH.open(
        mode="a",
        encoding="utf-8",
    ) as arquivo:
        arquivo.write(
            json.dumps(
                resultado_execucao,
                ensure_ascii=False,
            )
        )

        arquivo.write("\n")


def salvar_resumo_csv(
    resultado_execucao: dict[str, Any],
) -> None:
    """Acrescenta um resumo da execução ao arquivo CSV."""

    LOGS_PATH.mkdir(
        parents=True,
        exist_ok=True,
    )

    campos = [
        "run_id",
        "perfil",
        "status",
        "inicio_utc",
        "fim_utc",
        "duracao_segundos",
        "etapas_executadas",
        "etapas_com_sucesso",
        "etapas_com_falha",
        "bronze_registros",
        "silver_registros",
        "gold_registros",
        "bronze_bytes",
        "silver_bytes",
        "gold_bytes",
    ]

    arquivo_existe = (
        SUMMARY_CSV_PATH.exists()
    )

    metricas = resultado_execucao[
        "metricas_depois"
    ]

    linha = {
        "run_id": resultado_execucao["run_id"],
        "perfil": resultado_execucao["perfil"],
        "status": resultado_execucao["status"],
        "inicio_utc": resultado_execucao["inicio_utc"],
        "fim_utc": resultado_execucao["fim_utc"],
        "duracao_segundos": (
            resultado_execucao["duracao_segundos"]
        ),
        "etapas_executadas": (
            resultado_execucao["etapas_executadas"]
        ),
        "etapas_com_sucesso": (
            resultado_execucao["etapas_com_sucesso"]
        ),
        "etapas_com_falha": (
            resultado_execucao["etapas_com_falha"]
        ),
        "bronze_registros": (
            metricas["bronze"]["registros"]
        ),
        "silver_registros": (
            metricas["silver"]["registros"]
        ),
        "gold_registros": (
            metricas["gold"]["registros"]
        ),
        "bronze_bytes": (
            metricas["bronze"]["bytes"]
        ),
        "silver_bytes": (
            metricas["silver"]["bytes"]
        ),
        "gold_bytes": (
            metricas["gold"]["bytes"]
        ),
    }

    with SUMMARY_CSV_PATH.open(
        mode="a",
        encoding="utf-8-sig",
        newline="",
    ) as arquivo:
        escritor = csv.DictWriter(
            arquivo,
            fieldnames=campos,
        )

        if not arquivo_existe:
            escritor.writeheader()

        escritor.writerow(linha)


def exibir_metricas(
    titulo: str,
    metricas: dict[str, dict[str, int]],
) -> None:
    """Exibe métricas de volume das camadas."""

    print(f"\n{titulo}")

    for camada, valores in metricas.items():
        print(
            f"- {camada.capitalize()}: "
            f"{valores['arquivos']:,} arquivos | "
            f"{valores['registros']:,} registros | "
            f"{formatar_bytes(valores['bytes'])}"
        )


def executar_pipeline(
    perfil: str,
    continuar_em_erro: bool,
) -> int:
    """Executa um perfil da pipeline com observabilidade."""

    inicio = datetime.now(
        timezone.utc
    )

    run_id = inicio.strftime(
        "%Y%m%dT%H%M%S%fZ"
    )

    pasta_execucao = (
        EXECUTIONS_PATH
        / run_id
    )

    pasta_execucao.mkdir(
        parents=True,
        exist_ok=True,
    )

    etapas = PERFIS[perfil]

    print(
        "\n=========================================="
    )
    print(
        "EXECUÇÃO MONITORADA DA PIPELINE"
    )
    print(
        "=========================================="
    )
    print(f"ID da execução: {run_id}")
    print(f"Perfil: {perfil}")
    print(f"Quantidade de etapas: {len(etapas)}")

    metricas_antes = coletar_metricas_dados()

    exibir_metricas(
        "Métricas antes da execução:",
        metricas_antes,
    )

    resultados_etapas: list[dict[str, Any]] = []

    inicio_cronometro = time.perf_counter()

    for nome_etapa in etapas:
        resultado = executar_etapa(
            run_id=run_id,
            nome_etapa=nome_etapa,
            pasta_execucao=pasta_execucao,
        )

        resultados_etapas.append(
            resultado
        )

        if (
            resultado["status"] == "FALHA"
            and not continuar_em_erro
        ):
            print(
                "\nA execução foi interrompida após "
                "a primeira falha."
            )
            break

    duracao_total = round(
        time.perf_counter()
        - inicio_cronometro,
        3,
    )

    fim = datetime.now(
        timezone.utc
    )

    metricas_depois = coletar_metricas_dados()

    etapas_com_sucesso = sum(
        etapa["status"] == "SUCESSO"
        for etapa in resultados_etapas
    )

    etapas_com_falha = sum(
        etapa["status"] == "FALHA"
        for etapa in resultados_etapas
    )

    status_execucao = (
        "SUCESSO"
        if etapas_com_falha == 0
        else "FALHA"
    )

    resultado_execucao = {
        "run_id": run_id,
        "perfil": perfil,
        "status": status_execucao,
        "inicio_utc": inicio.isoformat(),
        "fim_utc": fim.isoformat(),
        "duracao_segundos": duracao_total,
        "etapas_planejadas": len(etapas),
        "etapas_executadas": len(resultados_etapas),
        "etapas_com_sucesso": etapas_com_sucesso,
        "etapas_com_falha": etapas_com_falha,
        "metricas_antes": metricas_antes,
        "metricas_depois": metricas_depois,
        "resultados_etapas": resultados_etapas,
    }

    salvar_historico_jsonl(
        resultado_execucao
    )

    salvar_resumo_csv(
        resultado_execucao
    )

    arquivo_resultado = (
        pasta_execucao
        / "resultado_execucao.json"
    )

    arquivo_resultado.write_text(
        json.dumps(
            resultado_execucao,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    exibir_metricas(
        "Métricas depois da execução:",
        metricas_depois,
    )

    print(
        "\n=========================================="
    )
    print("RESUMO OPERACIONAL")
    print(
        "=========================================="
    )
    print(f"Status: {status_execucao}")
    print(
        f"Etapas executadas: "
        f"{len(resultados_etapas)}"
    )
    print(
        f"Etapas com sucesso: "
        f"{etapas_com_sucesso}"
    )
    print(
        f"Etapas com falha: "
        f"{etapas_com_falha}"
    )
    print(
        f"Duração total: "
        f"{duracao_total:.3f} segundos"
    )
    print(
        f"Resultado detalhado: "
        f"{arquivo_resultado}"
    )
    print(
        f"Resumo CSV: "
        f"{SUMMARY_CSV_PATH}"
    )

    return (
        0
        if status_execucao == "SUCESSO"
        else 1
    )


def obter_argumentos() -> argparse.Namespace:
    """Obtém os argumentos informados no terminal."""

    parser = argparse.ArgumentParser(
        description=(
            "Executa e monitora as etapas da pipeline "
            "de alfabetização."
        )
    )

    parser.add_argument(
        "--perfil",
        choices=sorted(PERFIS.keys()),
        default="qualidade",
        help=(
            "Conjunto de etapas que será executado. "
            "O padrão é qualidade."
        ),
    )

    parser.add_argument(
        "--continuar-em-erro",
        action="store_true",
        help=(
            "Continua executando as próximas etapas "
            "mesmo após uma falha."
        ),
    )

    return parser.parse_args()


def main() -> None:
    """Executa a pipeline monitorada."""

    argumentos = obter_argumentos()

    codigo_saida = executar_pipeline(
        perfil=argumentos.perfil,
        continuar_em_erro=(
            argumentos.continuar_em_erro
        ),
    )

    sys.exit(codigo_saida)


if __name__ == "__main__":
    main()
