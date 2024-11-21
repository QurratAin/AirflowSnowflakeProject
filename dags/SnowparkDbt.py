from datetime import datetime, timedelta
import os
from airflow.decorators import task, dag
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from snowflake.snowpark.functions import col, desc, count, when, date_trunc, sum, avg, asc
from snowflake.snowpark import Session    
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from dags.utils import get_env_var_astro
from dotenv import load_dotenv 

load_dotenv()
__snowflake_conn_id = "snowflake_default"

# Profile and connection configuration for DBT model
profile_config = ProfileConfig(profile_name="snowflake_profile",
                               target_name="dev",
                               profile_mapping=SnowflakeUserPasswordProfileMapping(
                                   conn_id=__snowflake_conn_id, 
                                    profile_args=get_env_var_astro())
                                )


@dag(dag_id="dbt_with_snowpark_analysis",
     start_date=datetime(2024, 11, 19),
     schedule=None,
     default_args = {
        "snowflake_conn_id": __snowflake_conn_id,
        "owner": "airflow",
        }, 
        )

def dbt_with_snowpark_analysis():

    # Create the dbt Task Group
    dbt_task_group = DbtTaskGroup(
    project_config=ProjectConfig("/usr/local/airflow/dbt"),
    operator_args={"install_deps": True},
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"
    ),
    group_id="dbt_task_group",
    )

    def session_maker():
        # Create the SnowflakeHook  
        hook = SnowflakeHook(snowflake_conn_id=__snowflake_conn_id) 
        conn = hook.get_connection(hook.snowflake_conn_id)
        
        # Create the Snowpark session using the connection details
        session = Session.builder.configs({
            "account": conn.extra_dejson.get('account'),
            "user": conn.login,
            "password": conn.password,
            "role": conn.extra_dejson.get('role'),
            "warehouse": conn.extra_dejson.get('warehouse'),
            "database": conn.extra_dejson.get('database'),
            "schema": conn.extra_dejson.get('schema')
        }).create()

        # Set the schema explicitly 
        session.sql("USE SCHEMA brave_database.project_test").collect()
        
        return session

    # Create the Snowpark analysis function
    @task
    def top_products():
        
        session = session_maker()
        # Find top 50 products that were viewed and but not purchased 
        top_products = ( 
            session.table('brave_database.project_test.fact_search_product_performance_qu')
            .filter((col("has_atc") == True) & (col("has_purchase") == False))
            .group_by("product_id")
            .agg(
                count(when(col("has_atc") == True, 1)).alias("total_potential_purchases")
            )
            .order_by( desc("total_potential_purchases"))
            .limit(50)
        )
        
        # Join with dim_product_qu to get product_name and product_category 
        prod_table = session.table('brave_database.project_test.dim_product_qu')
        top_products_with_details = ( 
            top_products.join(prod_table,
                            top_products.col("product_id") == prod_table.col("product_id"), how="inner")
                        .select(top_products.col("product_id"), 
                                prod_table.col("product_name"),
                                prod_table.col("product_category"),
                                top_products.col("total_potential_purchases"))
                        .order_by(desc("total_potential_purchases"), desc("product_category"))
        )

        top_products_with_details.write.mode("overwrite").save_as_table("TOP_PRODUCTS", table_type="transient")
        
        # Close the session 
        session.close()
        
        return "Data saved to Snowflake table TOP_PRODUCTS"

    @task
    def effective_marketing_channel():
        session = session_maker()
        # Load the DIM_CAMPAIGN_QU and FACT_CAMPAIGN_EFFECTIVENESS_QU tables 
        
        dim_campaign = session.table('brave_database.project_test.dim_campaign_qu') 
        fact_campaign = session.table('brave_database.project_test.fact_campaign_effectiveness_qu') 
        
        # Join the tables on CAMPAIGN_ID 
        campaign_effectiveness = ( 
            dim_campaign.join(fact_campaign, 
                              dim_campaign.col("CAMPAIGN_ID") == fact_campaign.col("CAMPAIGN_ID"), how="inner") ) 
        
        # Aggregate the data by medium and source 
        campaign_stats = ( campaign_effectiveness
                        .group_by(dim_campaign.col("MEDIUM"),
                                    dim_campaign.col("SOURCE"))
                        .agg( sum(fact_campaign.col("TOTAL_UNIT_SOLD")).alias("TOTAL_UNITS_SOLD"),
                               sum(fact_campaign.col("TOTAL_ATC")).alias("TOTAL_ATC")) 
                        .order_by(col("TOTAL_UNITS_SOLD").desc())
                        .limit(10) 
                        ) 
        
       
        # Save the DataFrame to a Snowflake table 
        campaign_stats.write.mode("overwrite").save_as_table("CAMPAIGN_STATS", table_type="transient")
        # Close the session 
        session.close()
        
        return "Data saved to Snowflake table CAMPAIGN_STATS"

    @task
    def stock_prediction():

        session = session_maker()
        # Load the inventory management table
        inv_mag = session.table('BRAVE_DATABASE.PROJECT_TEST.FACT_INVENTORY_MANAGEMENT_QU')

        # Filter to include only records with LAST_RESTOCK_DATE in 2024 
        inv_mag_filtered = inv_mag.filter( col("LAST_RESTOCK_DATE").between("2024-01-01", "2024-12-31") )
        
        # Calculate average monthly demand for the past year
        monthly_sales = (
            inv_mag_filtered
        .select(
                col("PRODUCT_ID"),
                col("SALES_VOLUME"),
                date_trunc('month', col("LAST_RESTOCK_DATE")).alias("MONTH")
            )
            .group_by(col("PRODUCT_ID"), col("MONTH"))
            .agg(
                sum(col("SALES_VOLUME")).alias("MONTHLY_SALES")
            )
        )

        
        # Calculate the average monthly demand
        average_monthly_demand = (
            monthly_sales
            .group_by(col("PRODUCT_ID"))
            .agg(
                avg(col("MONTHLY_SALES")).alias("AVERAGE_MONTHLY_DEMAND")
            )
        )

        # Join the inventory management table with the average monthly demand
        inventory_with_demand = (
            inv_mag_filtered
            .join(
                average_monthly_demand,
                inv_mag_filtered.col("PRODUCT_ID") == average_monthly_demand.col("PRODUCT_ID"),
                how="inner"
            )
            .select(           
                inv_mag_filtered.col("WAREHOUSE_ID"),
                inv_mag_filtered.col("INVENTORY_ID"),
                inv_mag_filtered.col("PRODUCT_ID"),
                inv_mag_filtered.col("STOCK_LEVEL"),
                inv_mag_filtered.col("REORDER_LEVEL"),
                average_monthly_demand.col("AVERAGE_MONTHLY_DEMAND"),         
                (average_monthly_demand.col("AVERAGE_MONTHLY_DEMAND") * 2).alias("RECOMMENDED_REORDER_LEVEL"),
                ((average_monthly_demand.col("AVERAGE_MONTHLY_DEMAND") * 2) - inv_mag_filtered.col("STOCK_LEVEL")).alias("ADJUSTMENT_NEEDED")
            )
        )

        # Filter to show products that need restocking
        inventory_adjustment = inventory_with_demand.filter(col("ADJUSTMENT_NEEDED") > 0).order_by(asc(col("warehouse_id")))

        inventory_adjustment.write.mode("overwrite").save_as_table("INVENTORY_ADJUSTMENT", table_type="transient")

        # Close the session 
        session.close()
        
        return "Data saved to Snowflake table INVENTORY_ADJUSTMENT"

    products = top_products()
    marketing_channel = effective_marketing_channel()
    stocks = stock_prediction()

    # Set task dependencies
    dbt_task_group >> products
    dbt_task_group >> marketing_channel
    dbt_task_group >> stocks

dbt_with_snowpark_analysis = dbt_with_snowpark_analysis()    
