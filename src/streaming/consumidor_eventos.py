import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_STREAMING_PATH = (
    PROJECT_ROOT
    / "data"
    / "bronze"
    / "streaming"
    / "eventos"
)

SILVER_STREAMING_PATH = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "streaming"
    / "indicador_alfabetizacao_atualizado"
)

TIPO_EVENTO_ESPERADO = "indicador_alfabetizacao_atualizado"
VERSAO_ESQUEMA_ESPERADA = "1.0"

CODIGOS_REDE_VALIDOS = {
    0,
    1,
    2,
    3,
    4,
    5,
    6,
}


def localizar_arquivos_eventos() -> list[Path]:
    """Localiza os arquivos JSONL disponíveis na camada Bronze."""

    arquivos = sorted(
        BRONZE_STREAMING_PATH.glob("eventos_*.jsonl")
    )

    if not arquivos:
        raise FileNotFoundError(
            "Nenhum arquivo de eventos foi encontrado em "
            f"{BRONZE_STREAMING_PATH}."
        )

    return arquivos


def carregar_ids_processados() -> set[str]:
    """Carrega os IDs que já foram gravados na camada Silver."""

    arquivos_processados = sorted(
        SILVER_STREAMING_PATH.glob(
            "data_processamento=*/*.parquet"
        )
    )

    ids_processados: set[str] = set()

    for arquivo in arquivos_processados:
        dataframe = pd.read_parquet(
            arquivo,
            columns=["event_id"],
        )

        ids_processados.update(
            dataframe["event_id"]
            .dropna()
            .astype(str)
            .tolist()
        )

    return ids_processados


def validar_texto(
    valor: Any,
    campo: str,
) -> str:
    """Valida um campo obrigatório de texto."""

    if valor is None:
        raise ValueError(
            f"O campo obrigatório '{campo}' está ausente."
        )

    texto = str(valor).strip()

    if not texto:
        raise ValueError(
            f"O campo obrigatório '{campo}' está vazio."
        )

    return texto


def validar_percentual(
    valor: Any,
    campo: str,
) -> float:
    """Valida se um percentual está entre zero e cem."""

    try:
        percentual = float(valor)
    except (TypeError, ValueError) as erro:
        raise ValueError(
            f"O campo '{campo}' não possui valor numérico válido."
        ) from erro

    if percentual < 0 or percentual > 100:
        raise ValueError(
            f"O campo '{campo}' deve estar entre 0 e 100."
        )

    return round(percentual, 2)


def transformar_evento(
    evento: dict[str, Any],
    arquivo_origem: Path,
    numero_linha: int,
) -> dict[str, Any]:
    """Valida e transforma um evento em registro tabular."""

    event_id = validar_texto(
        evento.get("event_id"),
        "event_id",
    )

    event_type = validar_texto(
        evento.get("event_type"),
        "event_type",
    )

    if event_type != TIPO_EVENTO_ESPERADO:
        raise ValueError(
            f"Tipo de evento inválido: {event_type}."
        )

    schema_version = validar_texto(
        evento.get("schema_version"),
        "schema_version",
    )

    if schema_version != VERSAO_ESQUEMA_ESPERADA:
        raise ValueError(
            f"Versão de esquema não suportada: {schema_version}."
        )

    event_timestamp = pd.to_datetime(
        evento.get("event_timestamp_utc"),
        utc=True,
        errors="coerce",
    )

    if pd.isna(event_timestamp):
        raise ValueError(
            "O campo 'event_timestamp_utc' é inválido."
        )

    payload = evento.get("payload")

    if not isinstance(payload, dict):
        raise ValueError(
            "O campo 'payload' deve ser um objeto JSON."
        )

    try:
        ano = int(payload.get("ano"))
        codigo_rede = int(payload.get("codigo_rede"))
    except (TypeError, ValueError) as erro:
        raise ValueError(
            "Os campos 'ano' e 'codigo_rede' devem ser inteiros."
        ) from erro

    if ano not in {2023, 2024}:
        raise ValueError(
            f"Ano inválido no evento: {ano}."
        )

    if codigo_rede not in CODIGOS_REDE_VALIDOS:
        raise ValueError(
            f"Código de rede inválido: {codigo_rede}."
        )

    id_municipio = validar_texto(
        payload.get("id_municipio"),
        "id_municipio",
    )

    id_uf = validar_texto(
        payload.get("id_uf"),
        "id_uf",
    )

    if not id_municipio.startswith(id_uf):
        raise ValueError(
            "O id_uf não corresponde ao início do id_municipio."
        )

    rede = validar_texto(
        payload.get("rede"),
        "rede",
    )

    taxa_anterior = validar_percentual(
        payload.get("taxa_alfabetizacao_anterior"),
        "taxa_alfabetizacao_anterior",
    )

    taxa_atualizada = validar_percentual(
        payload.get("taxa_alfabetizacao_atualizada"),
        "taxa_alfabetizacao_atualizada",
    )

    try:
        variacao_informada = round(
            float(payload.get("variacao_pp")),
            2,
        )
    except (TypeError, ValueError) as erro:
        raise ValueError(
            "O campo 'variacao_pp' não é válido."
        ) from erro

    variacao_calculada = round(
        taxa_atualizada - taxa_anterior,
        2,
    )

    if abs(
        variacao_informada - variacao_calculada
    ) > 0.01:
        raise ValueError(
            "A variação informada não corresponde à diferença "
            "entre a taxa anterior e a taxa atualizada."
        )

    return {
        "event_id": event_id,
        "event_type": event_type,
        "event_timestamp_utc": event_timestamp,
        "schema_version": schema_version,
        "source": validar_texto(
            evento.get("source"),
            "source",
        ),
        "ano": ano,
        "id_municipio": id_municipio,
        "id_uf": id_uf,
        "codigo_rede": codigo_rede,
        "rede": rede,
        "taxa_alfabetizacao_anterior": taxa_anterior,
        "taxa_alfabetizacao_atualizada": taxa_atualizada,
        "variacao_pp": variacao_calculada,
        "arquivo_origem": arquivo_origem.name,
        "numero_linha_origem": numero_linha,
        "processado_em_utc": datetime.now(
            timezone.utc
        ),
    }


def ler_eventos(
    arquivos: list[Path],
) -> tuple[list[dict[str, Any]], list[str]]:
    """Lê e valida todos os eventos encontrados."""

    registros_validos: list[dict[str, Any]] = []
    erros: list[str] = []

    for arquivo in arquivos:
        with arquivo.open(
            mode="r",
            encoding="utf-8",
        ) as conteudo:
            for numero_linha, linha in enumerate(
                conteudo,
                start=1,
            ):
                if not linha.strip():
                    continue

                try:
                    evento = json.loads(linha)

                    if not isinstance(evento, dict):
                        raise ValueError(
                            "A linha não representa um objeto JSON."
                        )

                    registro = transformar_evento(
                        evento=evento,
                        arquivo_origem=arquivo,
                        numero_linha=numero_linha,
                    )

                    registros_validos.append(registro)

                except (
                    json.JSONDecodeError,
                    ValueError,
                    TypeError,
                ) as erro:
                    erros.append(
                        f"{arquivo.name}, linha {numero_linha}: {erro}"
                    )

    return registros_validos, erros


def salvar_eventos_silver(
    dataframe: pd.DataFrame,
) -> Path:
    """Salva os novos eventos processados na camada Silver."""

    momento_processamento = datetime.now(
        timezone.utc
    )

    data_processamento = momento_processamento.strftime(
        "%Y-%m-%d"
    )

    identificador_arquivo = momento_processamento.strftime(
        "%Y%m%dT%H%M%S"
    )

    pasta_destino = (
        SILVER_STREAMING_PATH
        / f"data_processamento={data_processamento}"
    )

    pasta_destino.mkdir(
        parents=True,
        exist_ok=True,
    )

    arquivo_destino = (
        pasta_destino
        / f"eventos_{identificador_arquivo}.parquet"
    )

    dataframe.to_parquet(
        arquivo_destino,
        index=False,
        engine="pyarrow",
        compression="zstd",
    )

    return arquivo_destino


def main() -> None:
    """Executa o consumidor dos eventos de streaming."""

    print("\nIniciando consumidor de eventos\n")

    arquivos = localizar_arquivos_eventos()

    print(f"Arquivos Bronze encontrados: {len(arquivos)}")

    registros, erros = ler_eventos(
        arquivos
    )

    print(f"Eventos válidos encontrados: {len(registros)}")
    print(f"Eventos inválidos encontrados: {len(erros)}")

    if erros:
        print("\nErros de validação:")

        for erro in erros:
            print(f"- {erro}")

        raise ValueError(
            "Existem eventos inválidos na camada Bronze."
        )

    dataframe = pd.DataFrame(registros)

    if dataframe.empty:
        print("\nNenhum evento válido foi encontrado.")
        return

    quantidade_antes_deduplicacao = len(dataframe)

    dataframe = dataframe.drop_duplicates(
        subset=["event_id"],
        keep="last",
    )

    duplicados_no_lote = (
        quantidade_antes_deduplicacao
        - len(dataframe)
    )

    ids_processados = carregar_ids_processados()

    dataframe = dataframe[
        ~dataframe["event_id"].isin(ids_processados)
    ].copy()

    print(
        f"Duplicados removidos no lote: "
        f"{duplicados_no_lote}"
    )

    print(
        f"Eventos já processados ignorados: "
        f"{quantidade_antes_deduplicacao - duplicados_no_lote - len(dataframe)}"
    )

    if dataframe.empty:
        print(
            "\nNenhum evento novo precisa ser processado."
        )
        return

    dataframe = dataframe.sort_values(
        by="event_timestamp_utc"
    ).reset_index(drop=True)

    arquivo_destino = salvar_eventos_silver(
        dataframe
    )

    print(
        f"\nEventos salvos na Silver: {len(dataframe)}"
    )

    print(f"Destino: {arquivo_destino}")

    print(
        "\nConsumo dos eventos concluído com sucesso."
    )


if __name__ == "__main__":
    main()