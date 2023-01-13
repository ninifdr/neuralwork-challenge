import pandas as pd
import sqlite3
import datetime
from dateutil import parser
from pandasql import sqldf
from utils import get_coords

CHUNKSIZE = 1000000
PRECISSION = 1

# Connection with sqlite3 database
conn = sqlite3.connect("../database/trips.db")
cursor = conn.cursor()

# Start processing instance
start_processing_query = """INSERT INTO trips_processing (
        start_datetime
    )   values ("{}")
    """.format(
    datetime.datetime.now()
)
cursor.execute(start_processing_query)
conn.commit()

processing_id = int(
    cursor.execute("SELECT MAX(processing_id) from trips_processing").fetchone()[0]
)

# Read data in chunks
for df in pd.read_csv("../data/trips.csv", chunksize=1000000):

    trips_df = pd.read_sql_query("SELECT * from trips", conn)

    # Transform coords string into float with 2 decimals (in order tu classify):
    # Origin
    df["origin_lng"] = df["origin_coord"].apply(
        lambda coords: get_coords(coords, 0, PRECISSION)
    )
    df["origin_lat"] = df["origin_coord"].apply(
        lambda coords: get_coords(coords, 1, PRECISSION)
    )

    # Destination
    df["destination_lng"] = df["destination_coord"].apply(
        lambda coords: get_coords(coords, 0, PRECISSION)
    )
    df["destination_lat"] = df["destination_coord"].apply(
        lambda coords: get_coords(coords, 1, PRECISSION)
    )

    # Get hour from datetime
    df["hour"] = df["datetime"].apply(lambda datetime: parser.parse(datetime).hour)

    grouped_df = sqldf(
        """
        SELECT region, origin_lat, origin_lng, destination_lat, destination_lng, hour, count(*) as trips_qty, min(datetime) as min_datetime 
        FROM df
        GROUP BY region, origin_lat, origin_lng, destination_lat, destination_lng, hour
        ORDER BY trips_qty desc
    """
    )

    join_df = sqldf(
        """
        SELECT 
        DF.region,
        DF.origin_lat,
        DF. origin_lng,
        DF.destination_lat,
        DF.destination_lng,
        DF.hour,
        CASE WHEN T.region IS NULL THEN DF.trips_qty ELSE T.trips_qty + DF.trips_qty END AS trips_qty,
        DF.min_datetime,
        T.region as trips_region
        FROM grouped_df as DF
        LEFT JOIN trips_df as T ON 
            DF.region = T.region AND
            DF.hour = T.hour AND
            DF.origin_lat = T.origin_lat AND
            DF.origin_lng = T.origin_lng AND
            DF.destination_lng = T.destination_lng AND
            DF.destination_lat = T.destination_lat
    """
    )

    # Insert everything that didn't match in the previous query
    insert_df = sqldf(
        """
        SELECT region, origin_lat, origin_lng, destination_lat, destination_lng, hour, trips_qty, min_datetime
        FROM join_df
        WHERE trips_region is NULL

    """
    ).reset_index(drop=True)

    insert_df.to_sql("trips", conn, if_exists="append", index=False)

    update_df = sqldf(
        """
        SELECT region, origin_lat, origin_lng, destination_lat, destination_lng, hour, trips_qty
        FROM join_df
        WHERE trips_region is not NULL

    """
    ).reset_index(drop=True)

    update_df.to_sql("temp_update_trips", conn, if_exists="replace", index=False)

    update_query = """
       UPDATE trips 
       SET trips_qty = temp_update_trips.trips_qty 
       FROM temp_update_trips  
       WHERE temp_update_trips.region = trips.region AND 
       temp_update_trips.hour = trips.hour AND 
       temp_update_trips.origin_lat = trips.origin_lat AND
       temp_update_trips.origin_lng = trips.origin_lng AND
       temp_update_trips.destination_lng = trips.destination_lng AND
       temp_update_trips.destination_lat = trips.destination_lat
    """
    cursor.execute(update_query)

# Start processing instance
end_processing_query = """INSERT INTO trips_processing (
        start_datetime
    )   values ("{}")
    """.format(
    datetime.datetime.now()
)
end_processing_query = """UPDATE trips_processing
    SET end_datetime = "{}"
    WHERE processing_id = {}
    """.format(
    datetime.datetime.now(), processing_id
)
cursor.execute(end_processing_query)
conn.commit()
conn.close()
