# FastAPI PostgreSQL CSV Uploader

## Overview

This project is a FastAPI-based application designed to manage PostgreSQL database schemas and tables, as well as to facilitate the import of CSV files into specified database tables. The application offers a set of endpoints for creating schemas, creating tables with dynamic column definitions, uploading CSV data into the database, and retrieving user-defined views for reporting purposes.

## Features

- **Schema Management**: Create new schemas in a PostgreSQL database.
- **Table Creation**: Dynamically create tables with user-defined columns.
- **CSV Data Import**: Validate and upload CSV files into the designated database table.
- **Reporting**: Retrieve a list of user-defined views from the database.
- **Professional Documentation**: Detailed inline and external documentation to facilitate maintenance and further development.

## Architecture

The application is organized into three main components:

1. **FastAPI Endpoints**  
   Provides RESTful API endpoints to interact with the PostgreSQL database.  
   - Endpoints include schema creation, table creation, CSV file upload, and reporting.

2. **PostgreSQL Model**  
   Contains the database connection logic and functions to execute Data Definition Language (DDL) commands such as creating schemas and tables, as well as querying metadata from the database.

3. **CSV Upload Utility**  
   Handles CSV file processing including validation against the database table's column definitions and insertion of CSV data into the database.

## Prerequisites

- **Python 3.7+**
- **PostgreSQL**

### Required Python Packages

- `fastapi`
- `uvicorn`
- `psycopg2`
- `pandas`
- `sqlalchemy`
- `pydantic`

## Installation

1. **Clone the Repository:**
   ```bash
   git clone https://github.com/yourusername/fastapi-postgres-csv-uploader.git
   cd fastapi-postgres-csv-uploader
