from prefect.filesystems import GitHub

if __name__ == "__main__":
  github_block = GitHub.load("dataeng-week2")
  github_block.get_directory(from_path="week2", local_path="./github_cloned")