from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from management.postgresModel import *
from management.csv_upload import load_csv_to_db
import json

app = FastAPI()

@app.post("/create-schema/", tags = ["DDL"])
def create_schema_endpoint(schema: SchemaParameters):
    """   
    Create a new schema in the specified PostgreSQL database.

    This endpoint accepts a JSON payload containing the database name and schema name.
    It invokes the internal `create_schema` function to create the schema if it does not exist.
    """
    return create_schema(schema)


@app.post("/create-table/", tags = ["DDL"])
def create_table_endpoint(table_definition: TableDefinition):
    """    
    Create a new table within an existing schema of a PostgreSQL database.

    This endpoint expects a JSON payload with the database name, schema name, table name,
    and a dictionary specifying the columns and their SQL data types. It calls the `create_table`
    function to perform the creation.
    """
    return create_table(table_definition)

@app.post("/upload-csv/", tags = ["Uploading"])
async def upload_csv(
    file: UploadFile = File(...), 
    table_params: str = Form(...)
):  
    """
    Upload a CSV file and import its data into a specified database table.

    This endpoint accepts a multipart/form-data request containing:
        - A CSV file.
        - A JSON string representing table parameters (database, schema, and table name).
    
    The function parses the JSON into a TableParameters object and delegates CSV processing to
    the `load_csv_to_db` function.
    """
        
    # Parse JSON parameters into a TableParameters object.
    try:
        params_dict = json.loads(table_params)
        table_params_obj = TableParameters(**params_dict)
    except (json.JSONDecodeError, ValueError) as e:
        raise HTTPException(status_code=400, detail=f"Error al interpretar los parámetros: {str(e)}")

    return await load_csv_to_db(file, table_params_obj)

@app.get("/reports/", response_model=List[Dict[str, str]], tags = ["Reporting"])
async def get_views(db_name: str):
    """    
    Retrieve a list of non-system views from the specified PostgreSQL database.

    This endpoint fetches all user-defined views (excluding those in system schemas) and returns
    their schema and name in a structured list.
    """
    return fetch_views(db_name)