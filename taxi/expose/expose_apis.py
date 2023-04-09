from fastapi import FastAPI, Response
from fastapi.responses import HTMLResponse
import polars as pl
import io
import json
from typing import Optional
from pretty_html_table import build_table

app = FastAPI()

# Create a Postgres engine and connect to the Docker container
conn = "postgresql://postgres:password@localhost:2000/postgres"
query = f"SELECT 1"
df = pl.read_sql(query, conn)

@app.get("/")
async def welcome():
    return HTMLResponse(content="Y'ello", status_code=200)

# Endpoint to retrieve the dataset in CSV and JSON format
@app.get("/data/{format}")
def get_data(format: str):
    try:
        # Convert the data to the desired format and set the response headers
        if format == "csv":
        # Query the database and retrieve the data as a Polars DataFrame
            query = f"SELECT * FROM TRIPS"
            df = pl.read_sql(query, conn)
            csv_string = df.write_csv()
            response = Response(content=csv_string, media_type="text/csv")
            response.headers["Content-Disposition"] = "attachment; filename=taxi_trips.csv"
            return response
        elif format == "json":
        # Query the database and retrieve the data as a Polars DataFrame
            query = f"SELECT * FROM TRIPS"
            df = pl.read_sql(query, conn)
            json_string = df.write_json(row_oriented=True)
            response = Response(content=json_string, media_type="application/json")
            response.headers["Content-Disposition"] = "attachment; filename=taxi_trips.json"
            return response
        else:
            raise Exception(f"Invalid format: {format}. Valid formats are csv and json.")
    except Exception as e:
        return  HTMLResponse(content= str(e), status_code=400)

@app.get("/custom_query/{format}/{query}")
def get_stats(format: str, query: str):
    # Query the database and retrieve the data as a Polars DataFrame
    query = query.replace(';','')
    try:
        if format == 'display':
            query += ' limit 7' if not 'limit' in query.lower() else ''
            df = pl.read_sql(query, conn)
            df = df.to_pandas()
            return HTMLResponse(content=build_table(df, 'blue_light'), status_code=200)
        elif format == 'csv':
            df = pl.read_sql(query, conn)
            csv_string = df.write_csv()
            response = Response(content=csv_string, media_type="text/csv")
            response.headers["Content-Disposition"] = "attachment; filename=custom_sql.csv"
            return response
        else:
            raise Exception(f'Unknown format "{format}"')
    except Exception as e:
            error_str = str(e) + f"........Query Executed: {query}"
            return  HTMLResponse(content= error_str, status_code=400)
