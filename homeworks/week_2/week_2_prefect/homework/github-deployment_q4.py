from prefect.deployments import Deployment
from etl_web_to_gcs_q4 import check_size
from prefect.filesystems import GitHub

storage = GitHub.load("vulnerrags-github")

deployment = Deployment.build_from_flow(
     flow=check_size,
     name="github-example",
     storage=storage,
     entrypoint="homeworks/week_2/week_2_prefect/homework/etl_web_to_gcs_q4.py:check_size",
     parameters={"color": "green", "year": 2020, "month": 11}
)

if __name__ == "__main__":
    deployment.apply()