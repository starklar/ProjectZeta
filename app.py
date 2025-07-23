from google.cloud import storage, bigquery


client = bigquery.Client()

dataset_id = "{}.project_zeta".format(client.project)
dataset_location = "northamerica-northeast2"
bucket_name = "project-zeta-bucket"
source_file_name = "pokemon_gen_6_data_1.csv"
destination_blob_name = "pokemon_data"
table_id = dataset_id + "." + destination_blob_name
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
            bigquery.SchemaField("Type 1", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("Type 2", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("HP", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("Attack", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("Defence", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("Sp Attack", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("Sp Defence", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("Speed", "INTEGER", mode="REQUIRED"),
        ]
    )

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )

    load_job.result()

    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))


def send_query(query_request) -> None:
    results = client.query_and_wait(query_request)

    for row in results:
        print(row)


def merge(file_name) -> None:
    temp_blob_name = "pokemon_data_temp"
    temp_table_id = dataset_id + "." + temp_blob_name
    temp_uri = "gs://" + bucket_name + "/" + temp_blob_name

    #Upload new data to bucket

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(temp_blob_name)

    generation_match_precondition = 0

    blob.upload_from_filename(file_name, if_generation_match=generation_match_precondition)

    #Load data from bucket into temp table

    job_config = bigquery.LoadJobConfig(
        schema = [
            bigquery.SchemaField("Number", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("Name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("Type 1", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("Type 2", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("HP", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("Attack", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("Defence", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("Sp Attack", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("Sp Defence", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("Speed", "INTEGER", mode="REQUIRED"),
        ]
    )

    load_job = client.load_table_from_uri(
        temp_uri, temp_table_id, job_config=job_config
    )

    load_job.result()

    #Merge data from the two tables

    merge_query = "MERGE INTO " + table_id + " T "
    merge_query += "USING " + temp_table_id + " S "
    merge_query += "ON T.Name = S.Name "
    merge_query += "WHEN MATCHED THEN "
    merge_query += "UPDATE SET `Type 1` = S.`Type 1`, `Type 2` = S.`Type 2`, "
    merge_query += "Attack = S.Attack, Defence = S.Defence, `Sp Attack` = S.`Sp Attack`, "
    merge_query += "`Sp Defence` = S.`Sp Defence`, Speed = S.Speed "
    merge_query += "WHEN NOT MATCHED THEN "
    merge_query += "INSERT (`Type 1`, `Type 2`, HP, Attack, Defence, `Sp Attack`, `Sp Defence`, Speed) "
    merge_query += "VALUES (`Type 1`, `Type 2`, HP, Attack, Defence, `Sp Attack`, `Sp Defence`, Speed)"
    
    results = client.query_and_wait(merge_query)

    for row in results:
        print(row)

    #Delete uploaded data to bucket

    blob.reload()
    generation_match_precondition = blob.generation

    blob.delete(if_generation_match=generation_match_precondition)
    print("Temp blob '{}' deleted.".format(temp_blob_name))

    #Delete temp table

    client.delete_table(temp_table_id, not_found_ok=True)  # Make an API request.
    print("Deleted temp table '{}'.".format(temp_table_id))


if __name__ == "__main__":
    print("Project Zeta! Start!")

    initialize_command = input("Would you first like to initialize the data for this program? (Enter \"yes\" to init, anything else to continue):")
    
    if initialize_command == "yes":
        print("Initializating data...")
        init_dataset()
    else:
        print("Skipping initialization...")

    while True:
        print("Now...")
        command = input("Enter your command: ")
        
        #Commands: query, merge, quit
        if command == "query":
            #Send queries to the database
            new_query = input("Enter your query here: ")
            #Ex. query: SELECT Name, Attack FROM `project_zeta.pokemon_data` WHERE `Type 1` = 'Ghost' OR `Type 2` = 'Ghost' ORDER BY Attack DESC
            send_query(new_query)
        elif command == "merge":
            #Ex. file: pokemon_gen_6_data_2.csv
            file_name = input("Enter the file name: ")
            merge(file_name)
        elif command == "quit":
            print("Project Zeta: Out. PEACE!")
            exit()
        else:
            print("ERROR: Unrecognized command. Please choose either: query, merge, or quit")