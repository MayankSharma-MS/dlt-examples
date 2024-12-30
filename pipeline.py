import os

import dlt
from dlt.common.pipeline import LoadInfo
from dlt.pipeline.pipeline import Pipeline
from typing import Iterable
from pyarrow import Table as PyArrowTable
from iceberg_writer import IcebergWriter
from dataos_utils import get_iceberg_credentials, get_iceberg_destination_config

try:
    from .mongodb import mongodb, mongodb_collection
except ImportError:
    from mongodb import mongodb, mongodb_collection


@dlt.destination(
    name="iceberg",
    loader_file_format="parquet",
    batch_size=1,
    naming_convention="snake_case"
)
def iceberg_insert(
        resources: Iterable,
        data_dict: dict
) -> None:
    """
    Custom destination for Iceberg table integration with DLT.
    """
    destination_config = get_iceberg_destination_config()
    cloud_provider = destination_config.get("cloud_provider", "")
    destination_credentials = get_iceberg_credentials(cloud_provider)
    iceberg_connection = {**destination_config, **destination_credentials}
    iceberg_writer = IcebergWriter(connection=iceberg_connection)
    table_name = iceberg_connection.get("table") if iceberg_connection.get("table") != "dlt_default_table" \
        else data_dict.get("name", "")
    namespace = iceberg_connection.get("namespace")
    full_table_path = f"{namespace}.{table_name}"
    pyarrow_table = PyArrowTable.from_batches(batches=[resources])
    iceberg_writer.write(pyarrow_table, destination=full_table_path, mode="overwrite")
    print(f"Data successfully written to Iceberg table {full_table_path}!")


def load_select_collection_db(pipeline: Pipeline = None) -> LoadInfo:
    """Use the MongoDB source to reflect an entire database schema and load select tables from it."""
    if pipeline is None:
        pipeline = dlt.pipeline(
            pipeline_name="mongo_to_postgres",
            destination=iceberg_insert,
            dataset_name="mongo_select",
            loader_file_format="parquet"
        )
    collection_name = os.getenv('SOURCES__MONGODB__COLLECTION_NAMES').split(",")
    # Load the MongoDB data source
    mongodb_source = mongodb(collection_names=collection_name)
    # Run the pipeline
    info = pipeline.run(mongodb_source, loader_file_format="parquet")
    return info


if __name__ == "__main__":
    load_info = load_select_collection_db()
