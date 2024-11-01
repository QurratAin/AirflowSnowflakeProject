from datetime import datetime
import os
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from pathlib import Path


profile_config = ProfileConfig(profile_name="xxx_profile",
                               target_name="dev",
                               profile_mapping=SnowflakeUserPasswordProfileMapping(conn_id="snowflake_default", 
                                                    profile_args={
                                                        "database": "DATABASE",
                                                        "schema": "PROJECT",
                                                        "account": "xxx",
                                                        "warehouse": "xxx",
                                                        "region": "xxx",
                                                        "role": "xxxx",
                                                        "user": "xxxx"}
                                                    ))


dbt_snowflake_dag = DbtDag(project_config=ProjectConfig("/usr/local/airflow/dbt"),
                    operator_args={"install_deps": True},
                    profile_config=profile_config,
                    execution_config=ExecutionConfig(dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",),
                    schedule_interval="@daily",
                    start_date=datetime(2024, 11, 1),
                    catchup=False,
                    dag_id="dbt_dag",)
