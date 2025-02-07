import psycopg2
from psycopg2 import sql
from fastapi import HTTPException
from pydantic import BaseModel
from typing import List, Dict

PG_USER = "globant"
PG_PASSWORD = "globant1239"
PG_HOST = "localhost"
PG_PORT = "5432"
#DEFAULT_DB = "globant_challenge"


class SchemaParameters(BaseModel):
    db_name: str
    schema_name: str
    
class TableParameters(SchemaParameters):
    table_name: str
    
class TableDefinition(TableParameters):
    columns: dict


def connect_to_db(db_name: str):
    """Conexión a una base de datos específica."""
    return psycopg2.connect(
        dbname=db_name, user=PG_USER, password=PG_PASSWORD, host=PG_HOST, port=PG_PORT
    )

def create_schema(schema_parameters: SchemaParameters):
    """Crear un esquema en la base de datos especificada."""
    
    db_name = schema_parameters.db_name
    schema_name = schema_parameters.schema_name
    
    try:
        with connect_to_db(db_name) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    sql.SQL("CREATE SCHEMA IF NOT EXISTS {};").format(sql.Identifier(schema_name))
                )
                conn.commit()
                return {"message": f"Esquema '{schema_name}' creado correctamente en la base de datos '{db_name}'."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def create_table(table_parameters: TableDefinition):
    """Crear una tabla en la base de datos y esquema especificados."""
    try:
        with connect_to_db(table_parameters.db_name) as conn:
            with conn.cursor() as cursor:
                create_table_query = f"CREATE TABLE IF NOT EXISTS {table_parameters.schema_name}.{table_parameters.table_name} ("
                column_definitions = ", ".join([f"{col} {col_type}" for col, col_type in table_parameters.columns.items()])
                create_table_query += column_definitions + ");"
                cursor.execute(create_table_query)
                conn.commit()
                return {"message": f"Tabla '{table_parameters.table_name}' creada correctamente en el esquema '{table_parameters.schema_name}'."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
def get_columns_from_table(table_parameters : TableParameters):
    """Obtiene los nombres de las columnas de una tabla específica en el esquema dado."""
    try:
        # Conectar a la base de datos
        conn = connect_to_db(table_parameters.db_name)
        cursor = conn.cursor()

        # Consulta SQL para obtener las columnas de la tabla
        cursor.execute(sql.SQL("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s
            AND table_name = %s;
        """), (table_parameters.schema_name, table_parameters.table_name))

        # Obtener las columnas
        columns = [column[0] for column in cursor.fetchall()]

        cursor.close()
        conn.close()

        return columns

    except Exception as e:
        print(f"Error al obtener las columnas de la tabla: {e}")
        return None
    
def fetch_views(db_name: str) -> List[Dict]:
    """Obtener todas las vistas en la base de datos especificada."""
    
    query = """
    SELECT
      table_schema,
      table_name
    FROM
      information_schema.views
    WHERE
      table_schema NOT IN ('pg_catalog', 'information_schema')
    ORDER BY
      table_schema,
      table_name;
    """
    
    try:
        # Conectar a la base de datos
        with connect_to_db(db_name) as conn:
            with conn.cursor() as cursor:
                # Ejecutar la consulta SQL
                cursor.execute(query)
                
                # Obtener las filas de resultados
                rows = cursor.fetchall()
                
                # Convertir las filas en una lista de diccionarios
                views = [{"table_schema": row[0], "table_name": row[1]} for row in rows]
                
                return views
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener las vistas: {str(e)}")