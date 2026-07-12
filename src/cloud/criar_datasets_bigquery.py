from google.api_core.exceptions import GoogleAPIError, NotFound
from google.cloud import bigquery


PROJECT_ID = "tc-alfabetizacao-lucas"
LOCATION = "US"

DATASETS = {
    "tc_bronze": {
        "camada": "bronze",
        "descricao": (
            "Camada Bronze do Tech Challenge. "
            "Armazena dados brutos provenientes das fontes."
        ),
    },
    "tc_silver": {
        "camada": "silver",
        "descricao": (
            "Camada Silver do Tech Challenge. "
            "Armazena dados limpos, padronizados e validados."
        ),
    },
    "tc_gold": {
        "camada": "gold",
        "descricao": (
            "Camada Gold do Tech Challenge. "
            "Armazena dados analíticos preparados para consultas, "
            "dashboards e inteligência artificial."
        ),
    },
}


def criar_dataset(
    client: bigquery.Client,
    dataset_id: str,
    camada: str,
    descricao: str,
) -> None:
    """Cria um dataset no BigQuery caso ele ainda não exista."""

    identificador_completo = f"{PROJECT_ID}.{dataset_id}"

    try:
        dataset_existente = client.get_dataset(
            identificador_completo
        )

        print(
            f"[JÁ EXISTE] {dataset_existente.dataset_id} | "
            f"localização={dataset_existente.location}"
        )

        return

    except NotFound:
        pass

    dataset = bigquery.Dataset(
        identificador_completo
    )

    dataset.location = LOCATION
    dataset.description = descricao

    dataset.labels = {
        "projeto": "tech_challenge_fase_2",
        "camada": camada,
        "ambiente": "desenvolvimento",
    }

    try:
        dataset_criado = client.create_dataset(
            dataset,
            timeout=30,
        )

        print(
            f"[CRIADO] {dataset_criado.dataset_id} | "
            f"localização={dataset_criado.location}"
        )

    except GoogleAPIError as erro:
        raise RuntimeError(
            f"Não foi possível criar o dataset "
            f"{identificador_completo}: {erro}"
        ) from erro


def listar_datasets_criados(
    client: bigquery.Client,
) -> None:
    """Confirma os datasets do projeto relacionados ao trabalho."""

    print("\nDatasets da Arquitetura Medalhão:\n")

    datasets_encontrados = {
        dataset.dataset_id
        for dataset in client.list_datasets(
            project=PROJECT_ID
        )
    }

    for dataset_id in DATASETS:
        status = (
            "OK"
            if dataset_id in datasets_encontrados
            else "NÃO ENCONTRADO"
        )

        print(f"- {dataset_id}: {status}")


def main() -> None:
    """Cria os datasets Bronze, Silver e Gold no BigQuery."""

    print("\nCriando Arquitetura Medalhão no BigQuery\n")
    print(f"Projeto: {PROJECT_ID}")
    print(f"Localização: {LOCATION}\n")

    client = bigquery.Client(
        project=PROJECT_ID
    )

    for dataset_id, configuracao in DATASETS.items():
        criar_dataset(
            client=client,
            dataset_id=dataset_id,
            camada=configuracao["camada"],
            descricao=configuracao["descricao"],
        )

    listar_datasets_criados(
        client=client
    )

    print(
        "\nCriação dos datasets concluída com sucesso."
    )


if __name__ == "__main__":
    main()