from google.cloud import bigquery


def clear_data_analytical_from_file(
    client: bigquery.Client,
    dataset_id: str,
    table_id: str,
    file: str,
):
    table_ref = f"{client.project}.{dataset_id}.{table_id}"

    query = f"""
        delete from `{table_ref}` where file = '{file}'
    """

    query_job = client.query(query)  # API request

    results = query_job.result()  # Waits for job to complete

    print(results)


def upload_csv_to_bigquery(
    client: bigquery.Client, csv_path: str, dataset_id: str, table_id: str
):
    """
    Faz upload de um arquivo CSV para a tabela 'analytical' no dataset 'realpark', projeto 'realpark-dev' no BigQuery.
    """
    table_ref = f"{client.project}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Skip header row if present
        autodetect=False,  # Automatically detect schema
    )

    with open(csv_path, "rb") as source_file:
        job = client.load_table_from_file(
            source_file, table_ref, job_config=job_config
        )

    job.result()  # Waits for the job to complete
