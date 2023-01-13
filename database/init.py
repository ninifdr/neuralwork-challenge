import sqlite3

conn = sqlite3.connect("trips.db")
cursor = conn.cursor()

# Create trips table
cursor.execute(
    """CREATE TABLE trips (
        region TEXT, 
        origin_lat REAL, 
        origin_lng REAL, 
        destination_lat REAL, 
        destination_lng REAL, 
        hour INTEGER, 
        trips_qty INTEGER,
        min_datetime TEXT,
        PRIMARY KEY (region, origin_lat, origin_lng, destination_lat, destination_lng, hour))
    """
)

cursor.execute(
    """CREATE TABLE trips_processing (
        processing_id INTEGER PRIMARY KEY AUTOINCREMENT,
        start_datetime TEXT,
        end_datetime TEXT
    )
    """
)

conn.commit()
conn.close()
