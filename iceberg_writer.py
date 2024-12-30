import datetime

import pyarrow
from pyarrow import Table as PyArrowTable
from abc import ABC, abstractmethod
from dataos_utils import get_access_token
from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table


class Writer(ABC):
    """
    Abstract base class for all writers.
    Provides a blueprint for implementing specific data writers.
    """

    @abstractmethod
    def write(self, input_data, destination, mode="append"):
        """
        Abstract method to write data to a specified destination.
        Args:
            input_data: Data to be written. The format may vary depending on the writer.
            destination: Target destination for the output.
            mode: Write mode ('append', 'overwrite', etc.).
        """
        pass

    @abstractmethod
    def validate_data(self, input_data):
        """
        Abstract method to validate the data before writing.
        Args:
            input_data: Data to validate.
        Returns:
            bool: True if the data is valid, otherwise raises an error.
        """
        pass

    @staticmethod
    def create_writer(connection):
        """
        Dynamically determine and create the appropriate writer based on the connection.
        Args:
            connection (dict): Connection details that determine the writer type.
        Returns:
            Writer: An instance of the appropriate writer class.
        """
        if connection.get("cloud_provider") in ["s3", "abfss", "gcs"]:
            return IcebergWriter(connection)
        else:
            raise ValueError("Unsupported cloud provider. Supported providers are 's3', 'abfss' and 'gcs'.")


class IcebergWriter(Writer):
    """
    Concrete writer class for Iceberg format.
    """

    def __init__(self, connection):
        """
        Initialize the IcebergWriter with a connection configuration.
        Args:
            connection (dict): Dictionary containing connection details.
        """
        cloud_provider = connection.get("cloud_provider")
        if cloud_provider == "s3":
            warehouse_url = connection.get("warehouse_path")
            self.catalog = load_catalog(
                name="iceberg_catalog",
                **{
                    "uri": connection.get("metastore_url", ""),
                    "s3.access-key-id": connection.get("aws_access_key_id", ""),
                    "s3.secret-access-key": connection.get("aws_secret_access_key", ""),
                    "s3.region": connection.get("region", "ap-south-1")
                }
            )
        elif cloud_provider == "abfss":
            self.catalog = load_catalog(
                name="iceberg_catalog",
                **{
                    "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
                    "adls.account-name": connection.get("azurestorageaccountname", ""),
                    "adls.account-key": connection.get("azurestorageaccountkey", ""),
                    "uri": connection.get("metastore_url", "")
                }
            )
        elif cloud_provider == "gcs":
            secret_file_path = connection.get("secret_file_path", "")
            scopes = connection.get("scopes", None)
            access_token = get_access_token(service_account_file_path=secret_file_path, scopes=scopes)
            token_expiry = int(datetime.datetime.now().timestamp()) + 10000
            self.catalog = load_catalog(
                name="iceberg_catalog",
                **{
                    "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
                    "uri": connection.get("metastore_url", ""),
                    "gcs.project-id": connection.get("project-id", ""),
                    "gcs.default-bucket-location": connection.get("warehouse_path", ""),
                    "gcs.oauth2.token-expires-at": token_expiry,
                    "gcs.oauth2.token": access_token
                }
            )
        else:
            raise ValueError("Unsupported cloud provider. Supported providers are 's3', 'abfss', and 'gcp'.")

    def write(self, input_data: PyArrowTable, destination, mode="append"):
        """
        Write data to an Iceberg table.
        Args:
            input_data: Data to write to icebergs.
            destination: The Iceberg table identifier (e.g., "namespace.table_name").
            mode: Write mode ('append', 'overwrite').
        """
        self.validate_data(input_data)
        iceberg_table: Table = self.create_table_from_pyarrow(input_data, destination)
        if mode == "append":
            iceberg_table.append(input_data)
        elif mode == "overwrite":
            iceberg_table.overwrite(input_data)
        else:
            raise ValueError(f"Unsupported write mode: {mode}")
        print(f"Data successfully written to Iceberg table '{destination}' in {mode} mode.")

    def validate_data(self, input_data: PyArrowTable):
        """
        Validate the data to ensure it's compatible with Iceberg.
        Args:
            input_data: The data to validate.
        Raises:
            ValueError: If data is not valid.
        """
        if not isinstance(input_data, PyArrowTable):
            raise ValueError("Input data must be a PyArrowTable.")
        if len(input_data) == 0:
            raise ValueError("Input data cannot be empty.")
        print("Input data is valid for Iceberg.")
        return True

    def create_table_from_pyarrow(self, pyarrow_table: pyarrow.Table, destination: str) -> Table:
        """
        Create an Iceberg table from a PyArrow Table schema.
        """
        # Check if the table exists, and create it if it doesn't
        try:
            table = self.catalog.create_table_if_not_exists(destination, pyarrow_table.schema)
            return table
        except Exception:
            print(f"Exception while creating table '{destination}'.")


# Factory Method (Optional for Scalability)
class WriterFactory:
    """
    Factory class to create writers dynamically.
    """

    @staticmethod
    def get_writer(writer_type, **kwargs):
        """
        Factory method to get a writer instance.
        Args:
            writer_type (str): The type of writer ('iceberg', 'json', etc.).
            kwargs: Additional parameters for writer initialization.
        Returns:
            Writer: An instance of the requested writer.
        """
        if writer_type == "iceberg":
            return IcebergWriter(**kwargs)
        else:
            raise ValueError(f"Unsupported writer type: {writer_type}")
