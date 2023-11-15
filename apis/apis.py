###########  api developed for storing and retrieving open source database credentials

from database_config.models import SessionLocal,get_db
from pydantic import BaseModel
from database_config.models import Connection as DBConnection, SessionLocal, get_db
from fastapi import APIRouter,Depends,HTTPException,Form,Body
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, MetaData,inspect
from database_config.models import Connection
from enum import Enum
from sqlalchemy.orm import sessionmaker
from typing import Optional

router = APIRouter()


class SupportedDatabases(str, Enum):
    mysql = "mysql"
    postgres = "postgresql"

class ConnectionCreate(BaseModel):
    source: SupportedDatabases
    host: str
    user: str
    password: str
    port: Optional[int] = None
    database: str

class ConnectionResponse(BaseModel):
    host: str
    user: str
    password: str
    port: int
    database: str

class SupportedJoins(str, Enum):
    inner = "INNER"
    left = "LEFT"
    right = "RIGHT"
    full = "FULL"
    self = "SELF"
    cross = "CROSS"

class JoinDetails(BaseModel):
    connection_id: int
    type: SupportedJoins
    new_table : str
    table1: Optional[str] = None
    table1_col: Optional[dict] = None
    table2: Optional[str] = None
    table2_col: Optional[dict] = None



import yaml

# API endpoint to create a new connection
@router.post("/connections/add", tags=["connections"])
async def add_connection(
    connection_data: ConnectionCreate = Body(...),
    db: Session = Depends(get_db)
):
    """
    Add a new connection.

    #### Select MySQL or PostgreSQL as source
    """
    connection_data_dict = connection_data.dict()
    connection_data_dict['source'] = connection_data.source.value  # Convert Enum to string
    
    new_connection = Connection(**connection_data_dict)
    db.add(new_connection)
    db.commit()
    db.refresh(new_connection)


    with open('/home/user/.dbt/profiles.yml', 'r') as file:  # Read the YAML file
        data = yaml.load(file, Loader=yaml.FullLoader)

    # Create a new entry with the connection details
    test_output = {
        "id_"+str(new_connection.id): {
            'type': new_connection.source,
            'threads': 1,
            'host': new_connection.host,
            'port': int(new_connection.port),
            'user': new_connection.user,
            'pass': new_connection.password,
            'dbname': new_connection.database,
            'schema': 'public',
        }
    }

    data['denil']['outputs'].update(test_output)  # Update the outputs section in the YAML data

    # Write the updated data structure back to the YAML file
    with open('/home/user/.dbt/profiles.yml', 'w') as file:
        yaml.dump(data, file, default_flow_style=False)

    return new_connection

















# API endpoint to fetch each source or connections 
@router.get("/connections/{source_id}", tags=['connections'])
def get_connection(source_id: int,db : Session = Depends(get_db)):
    db_source = db.query(Connection).filter(Connection.id == source_id).first()
    if db_source:
        return db_source
    else:
        return {"message": "connection not found"}













## API endpoint developed to get all saved sources
@router.get("/sources", tags=["connections"])
def list_supported_sources():
    return {"supporteddatabases":list(SupportedDatabases)}














# # API to Perform a Connection to Pull Metadata

@router.post("/connections/connect", tags=["any metadata"])
def perform_connection_to_source(source_info: ConnectionCreate):
    """
    Perform a connection to a data source and pull metadata.
    """
    # Use source_info to connect to the source and pull metadata.
    source_db_url = ""
    db = source_info.source
    connection_string = (
        source_info.user + ':' + source_info.password + '@' +
        source_info.host + ':' + source_info.port + '/' + source_info.database  
    )
    try:
        if db not in SupportedDatabases:
            return {"error": "Unsupported database type"}
        if db in ["mysql", "postgres"]:
            if db == 'mysql':
                source_db_url = "mysql+mysqlconnector://" + connection_string
            else:
                source_db_url = "postgresql://" + connection_string
            
           
            engine = create_engine(source_db_url)
            SessionLocal.configure(bind=engine)
            metadata = MetaData()
            metadata.reflect(bind=engine)
            metadata_str = str(metadata.tables.values())
            return {"source_info": source_info, "metadata": metadata_str}
    except Exception as e:
        return {"error": str(e)}










# API to Provide a List of Data Source Tables in a Connection
@router.get("/connections/{connection_id}/tables", tags=["display tables"])
async def fetch_tables(connection_id: int, db = Depends(get_db)):
    connection = db.query(Connection).filter(Connection.id == connection_id).first()
    if not connection:
        raise HTTPException(status_code=404, detail="Connection not found")

    if connection.source == "mysql":
        source_db_url = f"mysql+mysqlconnector://{connection.user}:{connection.password}@{connection.host}:{connection.port}/{connection.database}"
    elif connection.source == "postgresql":
        source_db_url = f"postgresql://{connection.user}:{connection.password}@{connection.host}:{connection.port}/{connection.database}"
    else:
        raise HTTPException(status_code=400, detail="Unsupported database source")

    engine = create_engine(source_db_url)
    inspector = inspect(engine)
    table_list = inspector.get_table_names()

    return {"tables": table_list}





















# API to Provide a List of Unique Identifiers for a Table in a Source
@router.get("/connections/{connection_id}/table/{table_name}/unique_identifiers", tags=["unique identifiers"])
async def unique_identifiers(connection_id: int, table_name: str, db = Depends(get_db)):
    connection = db.query(Connection).filter(Connection.id == connection_id).first()
    if not connection:
        raise HTTPException(status_code=404, detail="Connection not found")

    if connection.source == "mysql":
        source_db_url = f"mysql+mysqlconnector://{connection.user}:{connection.password}@{connection.host}:{connection.port}/{connection.database}"
    elif connection.source == "postgresql":
        source_db_url = f"postgresql://{connection.user}:{connection.password}@{connection.host}:{connection.port}/{connection.database}"
    else:
        raise HTTPException(status_code=400, detail="Unsupported database source")

    engine = create_engine(source_db_url)
    inspector = inspect(engine)

    try:
        unique_identifiers = inspector.get_unique_constraints(table_name)
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found in the database")

    return {"unique_identifiers": unique_identifiers}













from sqlalchemy import create_engine, text,select,Table
# API to Provide table values


@router.get("/connections/{connection_id}/table/{table_name}/table_values", tags=["table values"])
async def table_values(connection_id: int, table_name: str, db: Session = Depends(get_db)):
    connection = db.query(Connection).filter(Connection.id == connection_id).first()
    if not connection:
        raise HTTPException(status_code=404, detail="Connection not found")

    if connection.source == "mysql":
        source_db_url = f"mysql+mysqlconnector://{connection.user}:{connection.password}@{connection.host}:{connection.port}/{connection.database}"
    elif connection.source == "postgresql":
        source_db_url = f"postgresql://{connection.user}:{connection.password}@{connection.host}:{connection.port}/{connection.database}"
    else:
        raise HTTPException(status_code=400, detail="Unsupported database source")

    engine = create_engine(source_db_url)

    try:
        # Use the engine to connect
        with engine.connect() as connection:
            inspector = inspect(engine)
            
            # Fetch columns information
            columns = inspector.get_columns(table_name)
            column_names = [column["name"] for column in columns]

            # Build a dynamic select statement based on columns
            select_query = f"SELECT {', '.join(column_names)} FROM {table_name}"

            # Execute the query and fetch all rows
            result = connection.execute(text(select_query))
            rows = result.fetchall()

            # Convert rows to dictionaries
            table_values = [dict(zip(column_names, row)) for row in rows]

            # Check if any rows were returned
            if not table_values:
                raise HTTPException(status_code=404, detail=f"No data found in the table '{table_name}'")
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Error: {e}")

    return {"table_values": table_values}













# API to Fetch Metadata from a Source Connection's Tables(all table metadata in connection source)
@router.get("/connections/{connection_id}/metadata", tags=["source metadata"])
async def fetch_metadata(connection_id: int, db = Depends(get_db)):
    connection = db.query(Connection).filter(Connection.id == connection_id).first()
    if not connection:
        raise HTTPException(status_code=404, detail="Connection not found")

    if connection.source == "mysql":
        source_db_url = f"mysql+mysqlconnector://{connection.user}:{connection.password}@{connection.host}:{connection.port}/{connection.database}"
    elif connection.source == "postgresql":
        source_db_url = f"postgresql://{connection.user}:{connection.password}@{connection.host}:{connection.port}/{connection.database}"
    else:
        raise HTTPException(status_code=400, detail="Unsupported database source")
    
    engine = create_engine(source_db_url)
    metadata = MetaData()
    try:

        metadata = MetaData()
        metadata.reflect(bind=engine)
        metadata_str = str(metadata.tables.values())

        print("data from database")
        return metadata_str

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve metadata: {str(e)}")
















# # API to Provide a List of Different Sources 
@router.get("/connections",tags=["connections"])
def get_all_connetion(db: Session = Depends(get_db)):
    connections = db.query(Connection).all()
    return connections















# # API to Trigger a Join from Two Source Table Connections
# Create a route that accepts the details of the two source tables, join conditions, and output options.
# @app.post("/joins")
# def perform_join(joint_info: dict):
#     """
#     Perform a join operation between two source table connections.
#     """
#     # Implement logic to perform the join operation and return the result.
#    return {...}



import os
import subprocess

# API to Trigger a Join from Two Source Table Connections
@router.post("/joins", tags=["connection"])
def perform_join(joint_info: JoinDetails, db: Session = Depends(get_db)):
    """
    Perform a join operation between two source table connections.
    """
    # Implement logic to perform the join operation and return the result.
    connection_id = joint_info.connection_id
    join_type = joint_info.type
    new_table = joint_info.new_table
    table1 = joint_info.table1
    table1_col = joint_info.table1_col
    table2 = joint_info.table2
    table2_col = joint_info.table2_col

    connection = db.query(Connection).filter(Connection.id == connection_id).first()
    if not connection:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    with open('/home/user/.dbt/profiles.yml', 'r') as file: # Read the YAML file
        data = yaml.load(file, Loader=yaml.FullLoader)
    
    data['denil']['target'] = "id_"+str(connection.id)
    
    with open('/home/user/.dbt/profiles.yml', 'w') as file: # Write the updated data structure back to the YAML file
        yaml.dump(data, file, default_flow_style=False)


    # Create appropriate .sql file
    # Specify the directory and filename for the .sql file
    directory = "./dbt_pro/denil/models"
    filename = new_table + ".sql"
    sql_file_path = f"{directory}/{filename}"

    # Build the SQL query dynamically
    # changing view to table 
    view_to_table = "{{ config(materialized='table') }}"
    sql_query = f"{view_to_table}\n\tSELECT\n\t"
    # Generate SELECT clause for table1
    select_table1 = [f"{table1}.{col} AS {table1_col[col]}" for col in table1_col]
    sql_query += ",\n\t".join(select_table1)
    sql_query += f",\n\t"

    # Generate SELECT clause for table2
    select_table2 = [f"{table2}.{col} AS {table2_col[col]}" for col in table2_col]
    sql_query += ",\n\t".join(select_table2)

    # Add the FROM clause with the join type and tables
    sql_query += f"\nFROM {table1}\n{join_type} JOIN {table2}"


    # Open the .sql file for writing
    with open(sql_file_path, "w") as sql_file:
        # Write SQL queries to the file, one query per line
        sql_file.write(sql_query)

    # The .sql file is now created with your SQL queries

    project_location = "./dbt_pro/denil"

    # Use subprocess or another method to run dbt commands from the remote location
    result = subprocess.run(["dbt", "run", "--project-dir", project_location], capture_output=True, text=True)
    output = {"stdout": result.stdout, "stderr": result.stderr}

    os.remove(sql_file_path)
    return output














