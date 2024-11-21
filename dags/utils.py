from dotenv import load_dotenv 
import os

def get_env_var_astro() -> dict:
    """
    Returns a dictionary of the connection parameters using the SnowSQL CLI
    environment variables.
    """
    load_dotenv()
    
    try:
        return {
        "user": os.getenv("SNOW_USER"),
        "password": os.getenv("SNOW_PWD"),
        "account": os.getenv("SNOW_ACCOUNT"),
        "role": os.getenv("SNOW_ROLE"),
        "warehouse": os.getenv("SNOW_WAREHOUSE"),
        "database": os.getenv("SNOW_DATABASE"),
        "schema": os.getenv("SNOW_SCHEMA"),
        "region": os.getenv("SNOW_REGION"),
        }
    except KeyError as exc:
        raise KeyError(
            "ERROR: Environment variable for Snowflake Connection not found. "
            + "Please set the SNOWSQL_* environment variables"
        ) from exc    
