from prefect import flow
from prefect_gcp.bigquery import GcpCredentials, BigQueryWarehouse, bigquery_load_file

@flow
def bigquery_flow() -> None:
    gcp_credentials = GcpCredentials.load("AIzaSyB0dsSFW4PhPi4qkvNjb5u_7MJQwgTw4CI")

    with BigQueryWarehouse(gcp_credentials=gcp_credentials) as warehouse:
        warehouse.execute(
            "CREATE TABLE `excellent-nomad-466619-e1.project_zeta.temp_table' (Number INTEGER,Name STRING,`Type 1` STRING,`Type 2` STRING,HP INTEGER,Attack INTEGER,Defence INTEGER,`Sp Attack` INTEGER,`Sp Defence` INTEGER,Speed INTEGER);"
        )

        bigquery_load_file(
            dataset="excellent-nomad-466619-e1.project_zeta",
            table="temp_table",
            path="./pokemon_gen_2_data.csv",
            gcp_credentials=gcp_credentials
        )

        warehouse.execute(
            "SELECT * FROM `excellent-nomad-466619-e1.project_zeta.temp_table' EXCEPT SELECT * FROM `excellent-nomad-466619-e1.project_zeta.pokemon_data`;"
        )
        warehouse.execute(
            """
            MERGE INTO `excellent-nomad-466619-e1.project_zeta.pokemon_data` T 
            USING `excellent-nomad-466619-e1.project_zeta.temp_table` S 
            ON T.Name = S.Name 
            WHEN MATCHED THEN
                UPDATE SET `Type 1` = S.`Type 1`, `Type 2` = S.`Type 2`, Attack = S.Attack,
                Defence = S.Defence, `Sp Attack` = S.`Sp Attack`,
                Sp Defence` = S.`Sp Defence`, Speed = S.Speed
            WHEN NOT MATCHED THEN
                INSERT (Number, Name, `Type 1`, `Type 2`, HP, Attack, Defence, `Sp Attack`, `Sp Defence`, Speed) 
                VALUES (Number, Name, `Type 1`, `Type 2`, HP, Attack, Defence, `Sp Attack`, `Sp Defence`, Speed)
            """
        )


if __name__ == "__main__":
    bigquery_flow()