from prefect.infrastructure.docker import DockerContainer
from prefect.deployments import Deployment
from etl_web_to_gcs import etl_parent_flow

docker_container_block = DockerContainer.load("dataeng")

docker_dep = Deployment.build_from_flow(
  flow=etl_parent_flow,
  name="docker-flow",
  infrastructure=docker_container_block
)

if __name__ == "__main__":
  docker_dep.apply()