
import csv
import io
import pandas as pd
from io import StringIO
from fastapi import UploadFile, HTTPException, File
from sqlalchemy import Table, MetaData, create_engine
from sqlalchemy.orm import sessionmaker
from .postgresModel import connect_to_db, get_columns_from_table, TableParameters
from pydantic import BaseModel
from psycopg2 import sql


def validate_csv_columns(csv_file, table_parameters: TableParameters):
    """Validate that the CSV file's columns exactly match those of the target database table."""
    try:
        contents = csv_file.file.read().decode('utf-8')
        csv_data = StringIO(contents)
        df = pd.read_csv(csv_data)

        table_columns = get_columns_from_table(table_parameters)

        if table_columns is None:
            return {"error": "Unable to retrieve table columns."}

        # Las columnas deben coincidir exactamente (sin importar el orden)
        csv_columns = df.columns.tolist()

        if set(csv_columns) != set(table_columns):
            return {"error": f"CSV columns do not match table columns.\nCSV: {csv_columns}\nTable: {table_columns}"}

        return {"message": "Las columnas del archivo CSV coinciden con las de la tabla."}

    except Exception as e:
        print(f"Error al validar las columnas del CSV: {e}")
        return {"error": str(e)}

# Database configuration for SQLAlchemy (if needed elsewhere)
DATABASE_URL = "postgresql://globant:globant1239@localhost:5432/globant_challenge"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


async def load_csv_to_db(file: UploadFile, table_params: TableParameters):
    try:
        contents = await file.read()
        csv_file = StringIO(contents.decode("utf-8"))
        reader = csv.reader(csv_file)

        conn = connect_to_db(table_params.db_name)
        cursor = conn.cursor()
        
        columns_query = f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = %s 
            AND table_name = %s
        """
        cursor.execute(columns_query, (table_params.schema_name, table_params.table_name))
        columns = [row[0] for row in cursor.fetchall()]
        
        columns_placeholder = ', '.join(columns)
        placeholders = ', '.join([f"%s" for _ in columns])
        insert_query = f"INSERT INTO {table_params.schema_name}.{table_params.table_name} ({columns_placeholder}) VALUES ({placeholders})"

        for row in reader:
            row = [None if cell == '' or cell.lower() in ['na', 'n/a'] else cell for cell in row]
            
            if len(row) != len(columns):
                raise HTTPException(status_code=400, detail="Número de columnas en el CSV no coincide con el número de columnas en la tabla")
            
            cursor.execute(insert_query, row)

        conn.commit()
        cursor.close()
        conn.close()

        return {"message": "CSV file successfully loaded into the database."}

    except Exception as e:
        # Manejo de errores
        print(f"Error loading CSV file: {e}")
        raise HTTPException(status_code=500, detail=f"Error loading CSV file: {str(e)}")


# def new_upload_file(file: UploadFile = File(...)):
#     if not file.filename.endswith('.csv'):
#         raise HTTPException(status_code=400, detail="Only CSV.")
    
#     contents = await file.read()
    
#     csv_data = contents.decode('utf-8').splitlines()
#     csv_reader = csv.reader(csv_data)
    
#     db = SessionLocal()
    
#     for row in csv_reader:
#         item = Item()