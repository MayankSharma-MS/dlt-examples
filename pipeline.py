import dlt
from dlt.sources.sql_database import sql_database



def load_tables_family_and_genome():
    # Create a dlt source that will load tables "family" and "genome"
    source = sql_database().with_resources("employees", "departments")

    # Create a dlt pipeline object
    pipeline = dlt.pipeline(
        pipeline_name="mysql_to_duckdb_pipeline",  # Custom name for the pipeline
        destination="duckdb",  # dlt destination to which the data will be loaded
        dataset_name="mysql_to_duckdb_pipeline_data",  # Custom name for the dataset created in the destination
    )

    # Run the pipeline
    load_info = pipeline.run(source)

    # Pretty print load information
    print(load_info)


if __name__ == '__main__':
    load_tables_family_and_genome()
