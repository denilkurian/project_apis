from fastapi import FastAPI
from database_config.models import Base, engine
import apis.apis as apis
app = FastAPI()


Base.metadata.create_all(bind=engine)

app.include_router(apis.router)



# API to Get Connection Details
# @app.get("/connections/{connection_id}")
# def get_connection_details(connection_id: int):
#     """
#     Get connection details by providing a connection ID.
#     """
#     # Fetch and return connection details based on the connection_id.
#     return {"connection_id": connection_id, "details": {...}}


# # API to Perform a Connection to Pull Metadata
# @app.post("/connections/connect")
# def perform_connection_to_source(source_info: dict):
#     """
#     Perform a connection to a data source and pull metadata.
#     """
#     # Use source_info to connect to the source and pull metadata.
#     return {"source_info": source_info, "metadata": {...}}


# # API to Provide a List of Different Sources
# @app.get("/sources")
# def list_supported_sources():
#     """
#     Provide a list of supported data sources.
#     """
#     # Return a list of supported data sources.
    # return {...}


# # API to Provide a List of Unique Identifiers for a Table in a Source
# @app.get("/sources/{source}/tables/{table}/unique-identifiers")
# def get_unique_identifiers(source: str, table: str):
#     """
#     Get the unique identifiers for a table in a source.
#     """
#     # Implement logic to retrieve unique identifiers for the specified table.
#     return {...}


# # API to Fetch Metadata from a Source Connection's Tables
# @app.get("/connections/{connection_id}/tables/metadata")
# def get_source_connection_tables_metadata(connection_id: int):
#     """
#     Fetch metadata for tables in a source connection.
#     """
#     # Implement logic to retrieve table metadata for the specified connection.
#     return {...}


# # API to Provide a List of Data Source Tables in a Connection
# @app.get("/connections/{connection_id}/tables")
# def list_source_connection_tables(connection_id: int):
#     """
#     Provide a list of tables available in a source connection.
#     """
#     # Implement logic to list tables in the specified connection.
#     return {...}


# # API to Provide List of Connection Sources
# @app.get("/connections/sources")
# def list_connection_sources():
#     """
#     Provide a list of available connection sources.
#     """
#     # Return a list of available connection sources.
#     return {...}


# # API to Trigger a Join from Two Source Table Connections
# @app.post("/joins")
# def perform_join(joint_info: dict):
#     """
#     Perform a join operation between two source table connections.
#     """
#     # Implement logic to perform the join operation and return the result.
#     return {...}







# 1. Get Connection Details

# 	Create a route that accepts a connection ID as a parameter.
# 	Use this ID to fetch connection details (e.g., credentials) from your database or configuration.

# 2. Perform a Connection to Pull Metadata

# 	Create a route that accepts connection details (e.g., credentials) and source information.
# 	Use the provided details to establish a connection to the source (MySQL, PostgreSQL, flat files) and pull metadata.



# 3. Provide List of Different Sources

# 	Create a route to query your database or configuration to retrieve the list of supported data sources (e.g., MySQL, PostgreSQL, flat files).



# 4. Provide List of Unique Identifiers for a Table in a Source

# 	Create a route that accepts a source and table name as parameters.
# 	Use the source information to identify the unique identifiers for the specified table.


# 5. Fetch Metadata from a Source Connection's Tables

# 	Create a route that accepts a source connection ID.
# 	Use the connection details to fetch metadata (e.g., table names, column names) for all tables in the source.


# 6. Provide List of Data Source Tables in a Connection

# 	Create a route that accepts a source connection ID.
# 	Use the connection details to list all the tables available in that source.


# 7. Provide List of Connection Sources
# 	Create a route to retrieve a list of available connection sources.


# 8. Trigger an API to Perform a Join from Two Source Table Connections

# 	Create a route that accepts the details of the two source tables, join conditions, and output options.
# 	Use the provided information to perform the join operation, and return the result.
	


