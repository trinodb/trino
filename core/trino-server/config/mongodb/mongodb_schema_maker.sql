/*  mongodb_schema_maker.sql

    Generates a JSON document containing Trino schema inFormation for a MongoDB collection
    from the output of the MongoDB Compass "Share Schema as JSON" command.

    Instructions:
    - In MongoDB Compass:
      - Connect to a cluster.
      - In the DataBases tree, expand a dataBase, then click a collection.
      - In the collection menu bar, click "Schema".
      - In the work area, click "Analyze Schema".
      - In the application menu bar, click "Share Schema as JSON".
    - In this SQL query:
      - Replace collectionName (below) with the name of the collection. (Keep the single and double quotes!)
      - Replace collectionSchema (below) with the contents of your clipboard. (Keep the single quotes!)
      - Run the query via psql.
      - The query result is a JSON schema document for the collection with the structure expected by Trino.
    - Copy the results of the query to a file and save it in the appropriate folder (for the connection/database).

    Notes:
    - If the output of the MongoDB Compass "Share Schema as JSON" command contains single quotes, this SQL query
      will fail. The fix is to delete the line(s) with the single quotes.
    - The order of fields in the output is not predictable, so manual editing is usually required. Our ordering
      convention is:
      - _id
      - (the collection UUID)
      - (other fields in the order found in most documents in the database)
      - the following fields, if they exist:
        - tenantInfoUUID
        - keycloakRealm
        - belongOrgUUID
        - belongOrgType
        - version
        - createDate
        - updateDate
        - _class
*/
WITH collection AS (
  SELECT
    '"collectionName"'        AS name     /* Replace collectionName with the name of the collection */
  , 'collectionSchema'::json  AS jschema  /* Replace collectionSchema with the schema JSON exported by MongoDB Compass */
  )
SELECT
  jsonb_pretty(('
    { "table": '||(SELECT name FROM collection)||',
      "fields": ['||string_agg('
        { "name": '||field_name||',
          "type": '||field_type||'
        }', ',')||'
      ]
    }')::jsonb)
FROM (
  SELECT
      f.field_name
    , coalesce(bs.sql_type, '"varchar"') AS field_type
  FROM (
    SELECT
        field_name::varchar AS field_name
      , lower(CASE
              WHEN json_typeof(field_type) = 'array'
              THEN  CASE
                    WHEN field_type->>0 = 'Undefined'
                    THEN field_type->1
                    ELSE field_type->0
                    END
              ELSE field_type
              END::varchar) AS field_type
    FROM (
      SELECT
          json_extract_path(json_array_elements(json_extract_path(c.jschema, 'fields')), 'name') AS field_name
        , json_extract_path(json_array_elements(json_extract_path(c.jschema, 'fields')), 'type') AS field_type
      FROM collection c
      ) m
    ) f
  LEFT JOIN (
    VALUES
        ('"objectid"', '"varchar"')
      , ('"string"', '"varchar"')
      , ('"int32"', '"integer"')
      , ('"short"', '"integer"')
      , ('"int64"', '"bigint"')
      , ('"long"', '"bigint"')
      , ('"double"', '"double"')
      , ('"decimal128"', '"decimal(12,8)"')
      , ('"json"', '"json"')
      , ('"binary"', '"varbinary"')
      , ('"boolean"', '"boolean"')
      , ('"date"', '"timeStamp"')
      , ('"dbref"', '"row"')
      , ('"document"', '"json"')
      , ('"object"', '"json"')
    ) AS bs (bson_type, sql_type)
  ON bs.bson_type = f.field_type
  ) mapped;
