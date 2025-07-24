from google.cloud import storage, bigquery


client = bigquery.Client()

dataset_id = f"{client.project}.project_zeta"
dataset_location = "northamerica-northeast2"
bucket_name = "project-zeta-bucket"
source_file_name = "pokemon_gen_6_data_1.csv"
destination_blob_name = "pokemon_data"
table_id = dataset_id + "." + destination_blob_name
uri = "gs://" + bucket_name + "/" + destination_blob_name

table_columns = ["Number","Name","`Type 1`","`Type 2`","HP","Attack","Defence","`Sp Attack`","`Sp Defence`","Speed"]
types = ["Normal", "Fire", "Water", "Grass", "Flying", "Fighting", "Poison", "Electric", "Ground", "Rock", "Psychic", "Ice", "Bug", "Ghost", "Dragon", "Dark", "Steel", "Fairy"]



def set_up_dataset(id) -> None:
    dataset = bigquery.Dataset(id)

    dataset.location = dataset_location

    dataset = client.create_dataset(dataset, timeout=30)
    print(f"Created dataset {client.project}.{id}")


def upload_file_to_bucket(blob_name, file_name) -> None:
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    generation_match_precondition = 0

    blob.upload_from_filename(file_name, if_generation_match=generation_match_precondition)


def load_table_from_bucket(bucket_uri, id) -> None:
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
        bucket_uri, id, job_config=job_config
    )

    load_job.result()

    destination_table = client.get_table(id)
    print(f"Loaded {destination_table.num_rows} rows for {id}.")


def init_dataset() -> None:
    #Set up dataset
    set_up_dataset(dataset_id)

    #Upload data file to bucket
    upload_file_to_bucket(destination_blob_name, source_file_name)

    #Set up table to hold data
    load_table_from_bucket(uri, table_id)

    print("Initialization complete")


def send_query(query_request) -> None:
    print("Sending query: " + query_request)

    results = client.query_and_wait(query_request)

    for row in results:
        print(row.values())


#Select queries for a sorted of a given stat for a given type or type combo
def select_best_of_query(columns, type_1, type_2, stat) -> None:
    for c in columns.split(","):
        if c not in table_columns:
            print("Invalid columns given")
            return

    if type_1 not in types:
        print("Type 1 is not a valid type")
        return
    
    if type_2 != "" and type_2 not in types:
        print("Type 2 is not a valid type")
        return
    
    if stat not in table_columns:
        print("Given stat is not valid")
        return

    query = f"SELECT {columns} FROM `{table_id}` WHERE `Type 1` = '{type_1}'"

    if type_2 == "":
        query += " OR `Type 2` = '" + type_1
    else:
        query += " AND `Type 2` = '" + type_2
    
    query += f"' ORDER BY {stat} DESC"
    
    send_query(query)


def bst_range(min, max, type) -> None:
    type_query = ""
    
    if type == "":
        pass
    elif type in types:
        type_query += f"(T.`Type 1` = '{type}' OR T.`Type 2` = '{type}') AND "
    else:
        print("Type 1 is not a valid type")
        return

    math = f"T.{table_columns[4]}"
    
    i = 5
    while i < len(table_columns):
        math += f" + T.{table_columns[i]}"
        i += 1

    query = f"SELECT * FROM `{table_id}` AS T WHERE {type_query} {math} BETWEEN {min} AND {max}"
    
    send_query(query)


def total_by_type() -> None:
    query = f"SELECT `Type 1`, `Type 2`, COUNT(*) FROM `{table_id}` GROUP BY `Type 1`, `Type 2`"
    send_query(query)


def merge(file_name) -> None:
    #Ex. file: pokemon_gen_6_data_2.csv
    
    temp_blob_name = "pokemon_data_temp"
    temp_table_id = dataset_id + "." + temp_blob_name
    temp_uri = "gs://" + bucket_name + "/" + temp_blob_name

    #Upload new data to bucket
    upload_file_to_bucket(temp_blob_name, file_name)

    #Load data from bucket into temp table
    load_table_from_bucket(temp_uri, temp_table_id)

    #Merge data from the two tables
    merge_query = f"""
    MERGE INTO {table_id} T 
    USING {temp_table_id} S 
    ON T.Name = S.Name
    WHEN MATCHED 
    THEN UPDATE 
        SET `Type 1` = S.`Type 1`, `Type 2` = S.`Type 2`, Attack = S.Attack, Defence = S.Defence, `Sp Attack` = S.`Sp Attack`, `Sp Defence` = S.`Sp Defence`, Speed = S.Speed 
    WHEN NOT MATCHED 
    THEN 
        INSERT (Number, Name, `Type 1`, `Type 2`, HP, Attack, Defence, `Sp Attack`, `Sp Defence`, Speed)
        VALUES (Number, Name, `Type 1`, `Type 2`, HP, Attack, Defence, `Sp Attack`, `Sp Defence`, Speed)
    """
    
    send_query(merge_query)

    #Delete uploaded data to bucket
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(temp_blob_name)

    blob.reload()
    generation_match_precondition = blob.generation

    blob.delete(if_generation_match=generation_match_precondition)
    print(f"Temp blob '{temp_blob_name}' deleted.")

    #Delete temp table

    client.delete_table(temp_table_id, not_found_ok=True)  # Make an API request.
    print(f"Deleted temp table '{temp_table_id}'.")


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
        
        if command == "best of":
            cols = input("Enter desired columns here (seperate by ','): ")
            t1 = input("Enter desired type here: ")
            t2 = input("Optionally, enter desired secondary type here (leave blank to just search by one type): ")
            s = input("Enter stat to sort by: ")

            select_best_of_query(cols, t1, t2, s)
        elif command == "bst range":
            min = input("Enter min BST: ")
            max = input("Enter max BST: ")
            t = input("Optionally, enter desired type here (leave blank to just search by range): ")
            
            bst_range(min, max, t)
        elif command == "total by type":
            total_by_type()
        elif command == "merge":
            #Ex. file: pokemon_gen_6_data_2.csv
            file_name = input("Enter the file name: ")
            merge(file_name)
        elif command == "quit":
            print("Project Zeta: Out. PEACE!")
            exit()
        else:
            print("ERROR: Unrecognized command. Please choose either: best of, bst range, total by type, merge, or quit")