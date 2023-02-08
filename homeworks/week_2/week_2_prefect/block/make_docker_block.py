from prefect.infrastructure import DockerContainer

# alternative creating blocks without UI interafce
docker_block = DockerContainer(
    image="prefecthq/prefect:zoom",  # insert here your image
    image_pull_policy="ALWAYS",
    auto_remove=True
)

docker_block.save("zoomcamp", overwrite=True)
