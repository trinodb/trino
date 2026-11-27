package trino
import rego.v1

default allow := true

batchColumnMasks contains {
  "index": i,
  "viewExpression": {
    "expression": "NULL"
  }
} if {
  some i
  column_resource := input.action.filterResources[i].column
  column_resource.catalogName == "sample_catalog"
  column_resource.schemaName == "sample_schema"
  column_resource.tableName == "restricted_table"
  column_resource.columnName == "user_phone"
}


batchColumnMasks contains {
  "index": i,
  "viewExpression": {
    "expression": "'****' || substring(user_name, -3)",
    "identity": "admin"
  }
} if {
  some i
  column_resource := input.action.filterResources[i].column
  column_resource.catalogName == "sample_catalog"
  column_resource.schemaName == "sample_schema"
  column_resource.tableName == "restricted_table"
  column_resource.columnName == "user_name"
}
