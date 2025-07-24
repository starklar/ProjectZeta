from prefect import flow
from prefect_gcp.bigquery import GcpCredentials, BigQueryWarehouse, bigquery_load_cloud_storage
from prefect_gcp.cloud_storage import cloud_storage_upload_blob_from_file

@flow
def bigquery_flow() -> None:
    gcp_creds = GcpCredentials.load("creds")

    with BigQueryWarehouse(gcp_credentials=gcp_creds) as warehouse:
        warehouse.execute(
            "CREATE TABLE `excellent-nomad-466619-e1.project_zeta.temp_table` (Number int,Name string,`Type 1` string,`Type 2` string,HP int,Attack int,Defence int,`Sp Attack` int,`Sp Defence` int,Speed int)"
        )
        
        path = "./pokemon_gen_2_data.csv"

        blob = cloud_storage_upload_blob_from_file(
        path, "project-zeta-bucket", "temp_blob", gcp_creds)

        '''
        bigquery_load_cloud_storage(
            dataset="excellent-nomad-466619-e1.project_zeta",
            table="excellent-nomad-466619-e1.project_zeta.temp_table",
            uri="gs://project-zeta-bucket/temp_blob",
            gcp_credentials=gcp_creds
        )
        '''
        
        warehouse.execute(
            """
            INSERT INTO excellent-nomad-466619-e1.project_zeta.temp_table
                (Number, Name, `Type 1`, `Type 2`, HP, Attack, Defence, `Sp Attack`, `Sp Defence`, Speed)
            VALUES (1000,"Gholdengo","Steel","Ghost",87,60,95,133,91,84)
            """
        )
        
        warehouse.execute(
            """
            MERGE INTO excellent-nomad-466619-e1.project_zeta.pokemon_data T
            USING excellent-nomad-466619-e1.project_zeta.temp_table S
            ON T.Name = S.Name
            WHEN MATCHED 
            THEN UPDATE 
                SET `Type 1` = S.`Type 1`, `Type 2` = S.`Type 2`, Attack = S.Attack, Defence = S.Defence, `Sp Attack` = S.`Sp Attack`, `Sp Defence` = S.`Sp Defence`, Speed = S.Speed 
            WHEN NOT MATCHED 
            THEN 
                INSERT (Number, Name, `Type 1`, `Type 2`, HP, Attack, Defence, `Sp Attack`, `Sp Defence`, Speed)
                VALUES (Number, Name, `Type 1`, `Type 2`, HP, Attack, Defence, `Sp Attack`, `Sp Defence`, Speed)
            """
        )


if __name__ == "__main__":
    bigquery_flow()