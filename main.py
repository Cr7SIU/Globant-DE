from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from management.postgresModel import *
from management.csv_upload import load_csv_to_db
import json

app = FastAPI()

@app.post("/create-schema/", tags = ["DDL"])
def create_schema_endpoint(schema: SchemaParameters):
    """Crea el esquema dentro de una base de datos."""
    return create_schema(schema)


@app.post("/create-table/", tags = ["DDL"])
def create_table_endpoint(table_definition: TableDefinition):
    """Crea una tabla en el esquema de la base de datos."""
    return create_table(table_definition)

@app.post("/upload-csv/", tags = ["Uploading"])
async def upload_csv(
    file: UploadFile = File(...), 
    table_params: str = Form(...)
):
    # Convertir JSON recibido a la clase TableParameters
    try:
        params_dict = json.loads(table_params)
        table_params_obj = TableParameters(**params_dict)
    except (json.JSONDecodeError, ValueError) as e:
        raise HTTPException(status_code=400, detail=f"Error al interpretar los parámetros: {str(e)}")

    # Lógica para cargar el archivo CSV a la base de datos
    return await load_csv_to_db(file, table_params_obj)

@app.get("/reports/", response_model=List[Dict[str, str]], tags = ["Reporting"])
async def get_views(db_name: str):
    """Retorna las vistas de la base de datos especificada."""
    return fetch_views(db_name)