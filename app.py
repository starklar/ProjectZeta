from google.cloud import storage, bigquery

client = bigquery.Client()

dataset_id = "{}.project_zeta".format(client.project)
dataset_location = "northamerica-northeast2"
bucket_name = "project-zeta-bucket"
source_file_name = "pokemon_data.csv"
destination_blob_name = "pokemon_data"
table_id = dataset_id + ".pokemon_data"
uri = "gs://" + bucket_name + "/" + destination_blob_name



def init_dataset() -> None:
    #Set up dataset

    dataset = bigquery.Dataset(dataset_id)

    dataset.location = dataset_location

    dataset = client.create_dataset(dataset, timeout=30)
    print("Created dataset {}.{}".format(client.project, dataset.dataset_id))

    #Upload data file to bucket

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    generation_match_precondition = 0

    blob.upload_from_filename(source_file_name, if_generation_match=generation_match_precondition)

    #Set up table to hold data

    job_config = bigquery.LoadJobConfig(
        schema = [
            bigquery.SchemaField("Number", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("Name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("Form", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Type 1", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("Type 2", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("HP", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("Attack", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("Defence", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("Sp Attack", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("Sp Defense", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("Speed", "INTEGER", mode="REQUIRED"),
        ]
    )

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )

    load_job.result()

    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))


def small_query() -> None:
    results = client.query_and_wait(
        """
        SELECT
          Name, Speed
        FROM `project_zeta.pokemon_data`
        WHERE `Type 1` LIKE 'Electric'
        ORDER BY Speed DESC
        LIMIT 10
        """
    )

    for row in results:
        print("{} : {} Speed".format(row.Name, row.Speed))



if __name__ == "__main__":
    print("HEE HO!")
    init_dataset()
    small_query()

