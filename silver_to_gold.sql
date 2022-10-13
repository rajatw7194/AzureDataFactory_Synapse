delta_tables = [
    {'silver_table':'silver_paymentsq', 'gold_table':'gold_payments'},
    {'silver_table':'silver_ridersq', 'gold_table':'gold_riders'},
    {'silver_table':'silver_stationsq', 'gold_table':'gold_stations'},
    {'silver_table':'silver_tripsq', 'gold_table':'gold_trips'},
    {'gold_table':'Gold_Date'}, 
    {'gold_table':'Gold_Time'}]

spark.sql(f"""
CREATE TABLE gold_payments (
    payment_id INT, payment_date_key INT, amount FLOAT,  rider_id INT)
USING DELTA LOCATION '/delta/gold_payments'
""")

spark.sql(f"""
CREATE TABLE gold_riders (
    rider_id INT, firstname VARCHAR(40), lastname VARCHAR(40), birthday DATE, member BOOLEAN, address VARCHAR(200))
USING DELTA LOCATION '/delta/gold_riders'
""")

spark.sql(f"""
CREATE TABLE gold_stations (
    station_id VARCHAR(60), station_name VARCHAR(100), latitude FLOAT, longitude FLOAT)
USING DELTA LOCATION '/delta/gold_stations'
""")

spark.sql(f"""
CREATE TABLE gold_trips (
    trip_id VARCHAR(60), trip_duration INT, start_time_key INT, end_time_key INT, start_date_key INT, end_date_key INT, start_station_key VARCHAR(60), end_station_key VARCHAR(60), rider_id INT, rider_age INT)
USING DELTA LOCATION '/delta/gold_trips'
""")

spark.sql(f"""
CREATE TABLE Gold_Date (
    date_id INT, day INT, dayweek VARCHAR(50), month INT, quarter INT, year INT)
USING DELTA LOCATION '/delta/Gold_Date'
""")

spark.sql(f"""
CREATE TABLE Gold_Time (
    time_id INT, second INT, minute INT, hour INT)
USING DELTA LOCATION '/delta/Gold_Time'
""")

df_payments = spark.read.table("silver_paymentsq")
df_riders = spark.read.table("silver_ridersq") 

(
    df_payments.alias("p")
    .join(df_riders.alias("r"), ['rider_id'])
    .selectExpr("CAST(p.payment_id AS INT) AS payment_id", 
                "CAST(r.rider_id AS INT) AS rider_id", 
                "CAST(UNIX_TIMESTAMP(CAST (p.payment_date AS DATE)) AS INTEGER) AS payment_date_key", 
                "CAST(p.amount AS FLOAT) AS amount")
    .distinct()
    .write.format("delta")
    .mode("overwrite")
    .saveAsTable("gold_payments")
)

df_stations = spark.read.table("silver_stationsq")

(
    df_stations.alias("s")
    .selectExpr("s.station_id AS station_id", 
                "s.name AS station_name", 
                "CAST(s.latitude AS FLOAT) AS latitude", 
                "CAST(s.longitude AS FLOAT) AS longitude")
    .distinct()
    .write.format("delta")
    .mode("overwrite")
    .saveAsTable("gold_stations")
)

df_riders = spark.read.table("silver_ridersq") 
df_trips = spark.read.table("silver_tripsq")

(
    df_trips.alias("t")
    .join(df_riders.alias("r"), ['rider_id'])
    .selectExpr(
                "t.trip_id AS trip_id",
                "CAST(CAST(to_timestamp(CONCAT(trip_end_date, ':00'), 'dd/MM/yyyy HH:mm:ss') AS LONG) - CAST(to_timestamp(CONCAT(trip_start_date, ':00'), 'dd/MM/yyyy HH:mm:ss') AS LONG) AS INTEGER) AS trip_duration",
                "(hour(to_timestamp(CONCAT(trip_start_date, ':00'), 'dd/MM/yyyy HH:mm:ss')) * 3600 + minute(to_timestamp(CONCAT(trip_start_date,':00'), 'dd/MM/yyyy HH:mm:ss')) * 60) +    second(to_timestamp(CONCAT(trip_start_date, ':00'), 'dd/MM/yyyy HH:mm:ss')) AS start_time_key",
                "(hour(to_timestamp(CONCAT(trip_end_date, ':00'), 'dd/MM/yyyy HH:mm:ss')) * 3600 + minute(to_timestamp(CONCAT(trip_end_date,':00'), 'dd/MM/yyyy HH:mm:ss')) * 60) +   second(to_timestamp(CONCAT(trip_end_date, ':00'), 'dd/MM/yyyy HH:mm:ss')) AS end_time_key",
                "CAST(UNIX_TIMESTAMP(to_date(CONCAT(trip_start_date, ':00'), 'dd/MM/yyyy HH:mm:ss')) AS INTEGER) AS start_date_key",
                "CAST(UNIX_TIMESTAMP(to_date(CONCAT(trip_end_date, ':00'), 'dd/MM/yyyy HH:mm:ss')) AS INTEGER) AS end_date_key",     
                "t.start_station_id AS start_station_key", "t.end_station_id AS end_station_key", "CAST(r.rider_id AS INTEGER) AS rider_id",
                "CAST(floor(datediff(current_date(), to_date(birthday))/365) AS INTEGER) AS rider_age")
    .distinct()
    .write.format("delta")
    .mode("overwrite")
    .saveAsTable("gold_trips")
)

from pyspark.sql.functions import to_date, explode, sequence

begin_date = '2000-01-01'
end_date = '2030-01-01'

df_date = (
  spark.sql(f"select explode(sequence(to_date('{begin_date}'), to_date('{end_date}'), interval 1 day)) as calendarDate")
)
df_date = df_date.selectExpr("CAST(UNIX_TIMESTAMP(calendarDate) AS INTEGER) AS date_id", 
                   "DAY(calendarDate) AS day", "CAST(DAYOFWEEK(calendarDate) AS STRING) AS dayweek", "MONTH(calendarDate) AS month",
                   "QUARTER(calendarDate) AS quarter","YEAR(calendarDate) AS year")
df_date.write.format("delta").mode("overwrite").saveAsTable("gold_date")

from datetime import datetime
from pyspark.sql.functions import to_date, explode, sequence

begin_date = datetime(2022, 10, 11, 0, 0, 0)
end_date = datetime(2022, 10, 11, 20, 28, 0)

df_time = (
  spark.sql(f"select explode(sequence(to_timestamp('{begin_date}'), to_timestamp('{end_date}'), interval 1 second)) as calendarDate")
)

df_time = df_time.selectExpr("""hour(calendarDate) * 3600 + minute(calendarDate) * 60 + second(calendarDate) AS time_id""",
                   "second(calendarDate) AS second",  "minute(calendarDate) AS minute", "hour(calendarDate) AS hour")
df_time.write.format("delta").mode("overwrite").saveAsTable("gold_time")
