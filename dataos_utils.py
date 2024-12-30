import os

import google.auth
from google.auth.transport.requests import Request


def get_access_token(service_account_file_path: str, scopes=None):
    """
    Retrieves an access token from Google Cloud Platform using service account credentials.

    Args:
        service_account_file_path: Path to the service account JSON key file.
        scopes: List of OAuth scopes required for your application.

    Returns:
        The access token as a string.
    """

    if scopes is None or len(scopes) < 1:
        scopes = ["https://www.googleapis.com/auth/cloud-platform"]
    credentials, name = google.auth.load_credentials_from_file(
        service_account_file_path, scopes=scopes)

    request = Request()
    credentials.refresh(request)  # Forces token refresh if needed
    return credentials


def get_iceberg_destination_config():
    """
        returns dictionary of iceberg config from env vars and config/secret files
    """
    # config_from_file = dlt.config.get("destination.iceberg.config", {})
    return {
        "cloud_provider": get_env_var("DESTINATION__ICEBERG__CONFIG__CLOUD_PROVIDER"),
        "catalog": get_env_var("DESTINATION__ICEBERG__CONFIG__CATALOG"),
        "warehouse_path": get_env_var("DESTINATION__ICEBERG__CONFIG__WAREHOUSE_PATH"),
        "metastore_url": get_env_var("DESTINATION__ICEBERG__CONFIG__METASTORE_URL"),
        "table": get_env_var("DESTINATION__ICEBERG__CONFIG__TABLE", "dlt_default_table"),
        "namespace": get_env_var("DESTINATION__ICEBERG__CONFIG__NAMESPACE", "dlt_default_namespace"),
        # **config_from_file
    }


def get_iceberg_credentials(cloud_provider):
    """
        returns dictionary of iceberg secrets from env vars and config/secret files
    """
    # secret_from_file = dlt.secrets.get(f"{cloud_provider}.credentials")
    secrets_from_env = {}
    if cloud_provider == "gcs":
        secrets_from_env = {
            "project-is": get_env_var("GCS__CREDENTIALS__PROJECT_ID"),
            "secret_file_path": get_env_var("GCS__CREDENTIALS__SECRET_FILE_PATH"),
        }
    elif cloud_provider == "abfss":
        secrets_from_env = {
            "azureendpointsuffix": get_env_var("ABFSS__CREDENTIALS__AZUREENDPOINTSUFFIX"),
            "azurestorageaccountname": get_env_var("ABFSS__CREDENTIALS__AZURESTORAGEACCOUNTNAME"),
            "azurestorageaccountkey": get_env_var("ABFSS__CREDENTIALS__AZURESTORAGEACCOUNTKEY"),
        }
    elif cloud_provider == "s3":
        secrets_from_env = {
            "aws_access_key_id": get_env_var("S3__CREDENTIALS__AWS_ACCESS_KEY_ID"),
            "aws_secret_access_key": get_env_var("S3__CREDENTIALS__AWS_SECRET_ACCESS_KEY"),
            "region": get_env_var("S3__CREDENTIALS__REGION", "ap-south-1"),
        }
    return {**secrets_from_env}


def get_env_var(key: str, default: str = None) -> str:
    """
    Fetches an environment variable safely. Raises an exception if the variable is not set and no default is provided.
    """
    value = os.getenv(key, default)
    if value is None:
        raise ValueError(f"Environment variable '{key}' is required but not set.")
    return value
