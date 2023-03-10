from prefect.filesystems import GitHub
from prefect.deployments import Deployment
from prefect_github.repository import GitHubRepository

# github_repository_block: GitHubRepository = GitHubRepository.load("dataeng-week2")

# github_repository_block.get_directory(from_path="week2", local_path="./week2/github_cloned")
# deployment: Deployment = Deployment.build_from_flow(
#   flow=github_block.get_flow(),
#   name="github-flow"
# )

if __name__ == "__main__":
  # Alternative to making a block on the UI, can make a block here
  gh_block = GitHub(
    name="dataeng-week2", repository="https://github.com/Light2Dark/data-engineering-zoomcamp" 
  )
  gh_block.save("dataeng-week2", overwrite=True)

  # deployment.apply()
  # github_block.get_directory(from_path="week2", local_path="./week2/github_cloned")
  # prefect deployment apply prefect-github-deployment.yaml
  # prefect deployment run parent_flow/github-flow -p "months=[11], colours=['green'], years=[2020]"
  # prefect deployment build week2/etl_web_to_gcs.py:etl_parent_flow \
  # -n github-flow \
  # -q test \
  # -sb github/dataeng-week2 \
  # -a
  ## -o prefect-github-deployment
  # prefect agent start -q test
  
  
