from flask import Flask, jsonify, request
from dateutil import parser
from functools import reduce
import datetime
import sqlite3

app = Flask(__name__)

# define la ruta para la consulta de viajes
@app.route("/trips", methods=["GET"])
def get_trips():

    NOW = datetime.datetime.now()

    # Get Params
    region = request.args.get("region")
    max_lat = request.args.get("max_lat")
    min_lat = request.args.get("min_lat")
    max_lng = request.args.get("max_lng")
    min_lng = request.args.get("min_lng")

    # Connect to database
    conn = sqlite3.connect("../database/trips.db")
    c = conn.cursor()

    # Query to get trips
    query = """
    SELECT trips_qty, min_datetime
    FROM trips
    WHERE
    region = "{}" AND
    origin_lat between {} AND {} AND
    origin_lng between {} AND {} AND
    destination_lat between {} AND {} AND
    destination_lng between {} AND {}
    """.format(
        region, min_lat, max_lat, min_lng, max_lng, min_lat, max_lat, min_lng, max_lng
    )

    # Execute query
    c.execute(query)
    rows = c.fetchall()

    # Close Database connection
    conn.close()

    avg_weekly_trips = reduce(
        lambda x, y: x + (int(y[0]) / ((NOW - parser.parse(y[1])).days / 7)), rows, 0
    )

    # devuelve los resultados como una lista de diccionarios
    return jsonify(
        {
            "avg_weekly_trips": avg_weekly_trips,
            "status": "200",
        }
    )


@app.route("/data_status", methods=["GET"])
def get_data_status():

    conn = sqlite3.connect("../database/trips.db")
    c = conn.cursor()

    query = """
    SELECT
    start_datetime
    FROM
    trips_processing
    WHERE
    end_datetime IS NULL
    """
    c.execute(query)
    rows = c.fetchone()

    if rows:
        response = {
            "Data": "Data being ingested since {}".format(rows[0]),
            "Status": "200",
        }
    else:
        response = {"Data": "No data being ingested", "Status": "200"}

    return jsonify(response)


# define el puerto y el host para iniciar el servidor
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
