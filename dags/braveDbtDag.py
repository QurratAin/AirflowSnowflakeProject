from datetime import datetime
import os
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from dags.utils import get_env_var_astro
from dotenv import load_dotenv 

load_dotenv()

# Profile and connection configuration for DBT model
profile_config = ProfileConfig(profile_name="snowflake_profile",
                               target_name="dev",
                               profile_mapping=SnowflakeUserPasswordProfileMapping(
                                   conn_id="snowflake_default", 
                                    profile_args=get_env_var_astro())
                                )


dbt_snowflake_dag = DbtDag(project_config=ProjectConfig("/usr/local/airflow/dbt"),
                    operator_args={"install_deps": True},
                    profile_config=profile_config,
                    execution_config=ExecutionConfig(dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",),
                    schedule_interval="@daily",
                    start_date=datetime(2024, 11, 13),
                    catchup=False,
                    dag_id="dbt_dag",)
