from prefect.infrastructure.docker import DockerContainer

# alternative to creating docker block in the UI
docker_block = DockerContainer(
  image="shahmir98/prefect:dataeng", # insert image here that has been pushed to DockerHub, if not defaults to Prefect base image
  image_pull_policy="ALWAYS",
  auto_remove=True,
  network_mode="bridge"
)

docker_block.save("dataeng", overwrite=True)