import argparse
import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

GOLD_PATH = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "indicador_alfabetizacao_municipio"
)

STREAMING_PATH = (
    PROJECT_ROOT
    / "data"
    / "bronze"
    / "streaming"
    / "eventos"
)


def localizar_indicador_mais_recente() -> Path:
    """Localiza o arquivo Gold mais recente do indicador municipal."""

    pastas_processamento = sorted(
        GOLD_PATH.glob("data_processamento=*"),
        reverse=True,
    )

    if not pastas_processamento:
        raise FileNotFoundError(
            "Nenhuma tabela Gold de indicador municipal foi encontrada."
        )

    arquivo = pastas_processamento[0] / "dados.parquet"

    if not arquivo.exists():
        raise FileNotFoundError(
            f"Arquivo não encontrado: {arquivo}"
        )

    return arquivo


def carregar_base_eventos() -> pd.DataFrame:
    """Carrega os registros que serão usados na simulação."""

    arquivo = localizar_indicador_mais_recente()

    dataframe = pd.read_parquet(
        arquivo,
        columns=[
            "ano",
            "id_municipio",
            "id_uf",
            "codigo_rede",
            "rede",
            "taxa_alfabetizacao",
        ],
    )

    dataframe = dataframe.dropna(
        subset=[
            "ano",
            "id_municipio",
            "codigo_rede",
            "taxa_alfabetizacao",
        ]
    ).reset_index(drop=True)

    if dataframe.empty:
        raise ValueError(
            "A tabela Gold não possui registros válidos para simulação."
        )

    return dataframe


def gerar_evento(registro: pd.Series) -> dict[str, object]:
    """Gera um evento simulado de atualização do indicador."""

    taxa_anterior = float(registro["taxa_alfabetizacao"])

    variacao = round(
        random.uniform(-2.0, 2.0),
        2,
    )

    taxa_atualizada = round(
        min(
            100.0,
            max(
                0.0,
                taxa_anterior + variacao,
            ),
        ),
        2,
    )

    variacao_real = round(
        taxa_atualizada - taxa_anterior,
        2,
    )

    return {
        "event_id": str(uuid4()),
        "event_type": "indicador_alfabetizacao_atualizado",
        "event_timestamp_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "schema_version": "1.0",
        "source": "simulador_python",
        "payload": {
            "ano": int(registro["ano"]),
            "id_municipio": str(registro["id_municipio"]),
            "id_uf": str(registro["id_uf"]),
            "codigo_rede": int(registro["codigo_rede"]),
            "rede": str(registro["rede"]),
            "taxa_alfabetizacao_anterior": taxa_anterior,
            "taxa_alfabetizacao_atualizada": taxa_atualizada,
            "variacao_pp": variacao_real,
        },
    }


def salvar_evento(
    evento: dict[str, object],
    arquivo_destino: Path,
) -> None:
    """Acrescenta um evento ao arquivo JSON Lines."""

    with arquivo_destino.open(
        mode="a",
        encoding="utf-8",
    ) as arquivo:
        arquivo.write(
            json.dumps(
                evento,
                ensure_ascii=False,
            )
        )

        arquivo.write("\n")
        arquivo.flush()


def executar_produtor(
    quantidade: int,
    intervalo: float,
) -> None:
    """Produz eventos simulados em intervalos regulares."""

    if quantidade <= 0:
        raise ValueError(
            "A quantidade de eventos deve ser maior que zero."
        )

    if intervalo < 0:
        raise ValueError(
            "O intervalo não pode ser negativo."
        )

    dataframe = carregar_base_eventos()

    STREAMING_PATH.mkdir(
        parents=True,
        exist_ok=True,
    )

    data_arquivo = datetime.now(
        timezone.utc
    ).strftime("%Y-%m-%d")

    arquivo_destino = (
        STREAMING_PATH
        / f"eventos_{data_arquivo}.jsonl"
    )

    print("\nIniciando produtor de eventos")
    print(f"Quantidade: {quantidade}")
    print(f"Intervalo: {intervalo} segundo(s)")
    print(f"Destino: {arquivo_destino}\n")

    for numero_evento in range(
        1,
        quantidade + 1,
    ):
        indice = random.randrange(
            len(dataframe)
        )

        registro = dataframe.iloc[indice]
        evento = gerar_evento(registro)

        salvar_evento(
            evento=evento,
            arquivo_destino=arquivo_destino,
        )

        payload = evento["payload"]

        print(
            f"[EVENTO {numero_evento:03d}/{quantidade:03d}] "
            f"município={payload['id_municipio']} | "
            f"rede={payload['rede']} | "
            f"taxa={payload['taxa_alfabetizacao_atualizada']}%"
        )

        if numero_evento < quantidade:
            time.sleep(intervalo)

    print(
        "\nProdução de eventos concluída com sucesso."
    )


def obter_argumentos() -> argparse.Namespace:
    """Obtém os parâmetros informados no terminal."""

    parser = argparse.ArgumentParser(
        description=(
            "Simula eventos de atualização do indicador "
            "de alfabetização."
        )
    )

    parser.add_argument(
        "--quantidade",
        type=int,
        default=10,
        help="Quantidade de eventos que serão produzidos.",
    )

    parser.add_argument(
        "--intervalo",
        type=float,
        default=1.0,
        help="Intervalo em segundos entre os eventos.",
    )

    return parser.parse_args()


def main() -> None:
    """Executa o produtor de eventos."""

    argumentos = obter_argumentos()

    executar_produtor(
        quantidade=argumentos.quantidade,
        intervalo=argumentos.intervalo,
    )


if __name__ == "__main__":
    main()