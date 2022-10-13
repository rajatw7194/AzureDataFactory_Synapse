delta_tables = [
    {'bronze_table':'bronze_payments', 'silver_table':'silver_paymentsq'},
    {'bronze_table':'bronze_riders', 'silver_table':'silver_ridersq'},
    {'bronze_table':'bronze_stations', 'silver_table':'silver_stationsq'},
    {'bronze_table':'bronze_trips', 'silver_table':'silver_tripsq'}]

spark.sql(f"""
    CREATE TABLE {delta_tables[0]['silver_table']} (payment_id VARCHAR(40), payment_date VARCHAR(40), amount VARCHAR(40), rider_id VARCHAR(40))
    USING DELTA LOCATION '/delta/{delta_tables[0]['silver_table']}'""")

spark.sql(f"""
    CREATE TABLE {delta_tables[1]['silver_table']} (rider_id VARCHAR(40), firstname VARCHAR(40), lastname VARCHAR(40), address VARCHAR(200), birthday VARCHAR(40), account_start_date VARCHAR(40), account_end_date VARCHAR(40), is_member VARCHAR(20))
    USING DELTA LOCATION '/delta/{delta_tables[1]['silver_table']}'""")

spark.sql(f"""
    CREATE TABLE {delta_tables[2]['silver_table']} (station_id VARCHAR(60), name VARCHAR(100), latitude VARCHAR(60), longitude VARCHAR(60))
    USING DELTA LOCATION '/delta/{delta_tables[2]['silver_table']}'""")

spark.sql(f"""
    CREATE TABLE {delta_tables[3]['silver_table']} (trip_id VARCHAR(60), rideable_type VARCHAR(100), trip_start_date VARCHAR(60), trip_end_date VARCHAR(60), start_station_id VARCHAR(60), end_station_id VARCHAR(60), rider_id VARCHAR(60))
    USING DELTA LOCATION '/delta/{delta_tables[3]['silver_table']}'""")
    
def create_bronze_table(source_table_location: str, target_table_name: str): 
    (spark.read.format("delta").load(f"/delta/{source_table_location}").toDF(*spark.read.table(target_table_name).columns)
        .write.format("delta").mode("overwrite").saveAsTable(target_table_name))

for file in delta_tables:
    create_bronze_table(file['bronze_table'], file['silver_table'])
