README.txt

This folder contains:
- Trino configuration files, for inclusion in /etc/trino.
- The "catalog" subfolder, which contains:
  - Trino connection files, for inclusion in /etc/trino/catalog.

Notes:
- The Trino configuration files are for the dev environment, with a single Trino node.
  - We will add configuration files for other environments in the future.
- The Trino connection files depend on several environment variables which must be set at the time Trino is started.
- Trino connections return data in standard SQL structures: catalog.schema.table.column.
  - MongoDB connections are made to a cluster, which may have more than one database.
    - Trino:MongoDB mapping:
      - catalog:connection_name
      - schema:database_name
      - table:collection_name
  - Neo4j connections are made to a cluster, which may have more than one database.
    - Trino:Neo4j mapping:
      - catalog:connection_name
      - schema:database_name
      - table:"(node_lable)" or "[relationship_type]"
  - PostgresSQL connections are made to a database.
    - Trino:PostgresSQL mapping:
      - catalog:connection_name
      - schema:schema_name
      - table:table_name
- Trino connectors determine schema and table names automatically, except for the MongoDB connector.
  The MongoDB connector requires that explicit table structure information be available for each
  collection in a database that is to be queried. See the ${PROJECT_HOME}/mongodb folder for more
  information, aids for generating MongoDB schema documents, and schema documents.