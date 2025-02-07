
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
    """Verifica si las columnas del archivo CSV coinciden con las de la tabla en la base de datos."""
    try:
        # Leer el archivo CSV desde el cliente
        contents = csv_file.file.read().decode('utf-8')  # Decodificar el archivo a string
        csv_data = StringIO(contents)  # Usamos StringIO para tratarlo como un archivo
        df = pd.read_csv(csv_data)

        # Obtener las columnas de la tabla en la base de datos
        table_columns = get_columns_from_table(table_parameters)

        # Verificar si las columnas del CSV coinciden con las de la tabla
        if table_columns is None:
            return {"error": "No se pudo obtener las columnas de la tabla."}

        # Las columnas deben coincidir exactamente (sin importar el orden)
        csv_columns = df.columns.tolist()

        if set(csv_columns) != set(table_columns):
            return {"error": f"Las columnas del archivo CSV no coinciden con las de la tabla en la base de datos.\nCSV: {csv_columns}\nTabla: {table_columns}"}

        # Si las columnas coinciden
        return {"message": "Las columnas del archivo CSV coinciden con las de la tabla."}

    except Exception as e:
        print(f"Error al validar las columnas del CSV: {e}")
        return {"error": str(e)}

# Configuración de la base de datos (asumimos que ya está definida)
DATABASE_URL = "postgresql://globant:globant1239@localhost:5432/globant_challenge"  # Cambia esto según tu configuración
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


async def load_csv_to_db(file: UploadFile, table_params: TableParameters):
    
    # table_params = TableParameters(db_name= 'globant_challenge', schema_name= 'test_on_api', 
    #                             table_name= 'hired_employees')

    try:
        # Leemos el archivo CSV
        contents = await file.read()
        csv_file = StringIO(contents.decode("utf-8"))
        reader = csv.reader(csv_file)

        # Conectamos a la base de datos
        conn = connect_to_db(table_params.db_name)
        cursor = conn.cursor()

        # Obtener los nombres de las columnas de la tabla en la base de datos
        columns_query = f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = %s 
            AND table_name = %s
        """
        cursor.execute(columns_query, (table_params.schema_name, table_params.table_name))
        columns = [row[0] for row in cursor.fetchall()]
        
        # Construir la consulta SQL dinámica para insertar en la tabla
        columns_placeholder = ', '.join(columns)
        placeholders = ', '.join([f"%s" for _ in columns])
        insert_query = f"INSERT INTO {table_params.schema_name}.{table_params.table_name} ({columns_placeholder}) VALUES ({placeholders})"

        # Iterar sobre las filas del CSV y hacer el insert
        for row in reader:
            # Convertir valores vacíos en NULL
            row = [None if cell == '' or cell.lower() in ['na', 'n/a'] else cell for cell in row]
            
            # Validar que la cantidad de columnas coincida
            if len(row) != len(columns):
                raise HTTPException(status_code=400, detail="Número de columnas en el CSV no coincide con el número de columnas en la tabla")
            
            # Ejecutar el insert
            cursor.execute(insert_query, row)

        # Confirmar los cambios
        conn.commit()
        cursor.close()
        conn.close()

        return {"message": "Archivo CSV cargado correctamente en la base de datos."}

    except Exception as e:
        # Manejo de errores
        print(f"Error al cargar el archivo CSV: {e}")
        raise HTTPException(status_code=500, detail=f"Error al cargar el archivo CSV: {str(e)}")


# def new_upload_file(file: UploadFile = File(...)):
#     if not file.filename.endswith('.csv'):
#         raise HTTPException(status_code=400, detail="Only CSV.")
    
#     contents = await file.read()
    
#     csv_data = contents.decode('utf-8').splitlines()
#     csv_reader = csv.reader(csv_data)
    
#     db = SessionLocal()
    
#     for row in csv_reader:
#         item = Item()