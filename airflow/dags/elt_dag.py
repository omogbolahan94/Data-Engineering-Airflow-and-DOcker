from datetime import datetime, timedelta
from airflow import DAG
from docker.types import Mount
import airflow.operators as ap 
from airflow.operators.python_operators import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.docker import DockerOperator
import subprocess

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}

def run_elt_script():
    # path where the script is pointed to in docker
    script_path = '/opt/airflow/elt/elt_script.py'
    result  = subprocess(['python', script_path], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Script failed with error {result.stderr}.")
    else:
        print(result.stdout)


# set up the dag object
dag = DAG(
    'elt_and_dbt', # dag name
    defaoult_args=default_args,
    description="An ELT workflow with DBT",
    start_date=datetime(2023,10,18),
    catchup=False
)

# defining task: we will comment out the elt script and dbt service in the docker compose file
# since we will be creating those services with airflow as a task

# task 1 is to run python script with PythonOperator
task1 = ap.python_operators.PythonOperator(
    task_id='run_elt_script',
    python_callable=run_elt_script,
    dag=dag
) 

# task 1 is to run python script with PythonOperator
# repeating the dbt service we have in docker copose in airflow
task2 = ap.docker.DockerOperator(
    task_id='run_dbt',
    image='ghcr.io/dbt-labs/dbt-postgres:1.4.7', # replace the image of the dbt service we have in docker container
    command=[
      "run",
      "--profiles-dir",
      "/root",
      "--project-dir",
      "/dbt"
    ],
    auto_remove=True, # remove the image once it has finished running
    docker_url='unix://var/run/docker.sock',
    network_mode='bridge',
    mounts=[
        Mount(source='users/user/document/elt-de-proj/custom_postgres', target='/dbt', type='bind'),
        Mount(source='users/user/.dbt', target='/root', type='bind')
    ], # replicate the volume of the dbt service in docker compose file
    dag=dag
) 

# order in which task should run: task 1 takes priority over task 2
task1 >> task2 

git_done = "New changes"
git_done2 = "changes"


