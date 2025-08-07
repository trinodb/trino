# Case Sensitivity Support in Iceberg Connector

## Overview

The Iceberg connector now supports case-sensitive table and schema name matching through the `iceberg.case-sensitive-name-matching` catalog property. This feature allows you to create and access tables and schemas with mixed-case names when using delimited identifiers.

## Configuration

Add the following property to your Iceberg catalog configuration:

```properties
iceberg.case-sensitive-name-matching=true
```

**Default value:** `false` (case-insensitive, backward compatible)

## Behavior

### Case-Insensitive Mode (Default)

When `iceberg.case-sensitive-name-matching=false`:

- All identifiers (delimited and non-delimited) are converted to lowercase
- `"MixedCase"`, `"UPPERCASE"`, and `mixedcase` all refer to the same object
- Maintains backward compatibility with existing deployments

### Case-Sensitive Mode

When `iceberg.case-sensitive-name-matching=true`:

- **Non-delimited identifiers** are still converted to lowercase (SQL standard behavior)
- **Delimited identifiers** preserve their exact case
- `"MixedCase"` and `"mixedcase"` refer to different objects
- `MixedCase` (non-delimited) becomes `mixedcase`

## Examples

### Schema Operations

```sql
-- Case-sensitive mode
CREATE SCHEMA "MixedCaseSchema";
CREATE SCHEMA "UPPERCASESCHEMA";
CREATE SCHEMA lowercaseschema;  -- Creates 'lowercaseschema'

-- These are three different schemas
SHOW SCHEMAS;
-- Results: MixedCaseSchema, UPPERCASESCHEMA, lowercaseschema
```

### Table Operations

```sql
-- Case-sensitive mode
CREATE TABLE "MySchema"."MyTable" (id bigint, name varchar);
CREATE TABLE myschema.mytable (id bigint, name varchar);

-- These are different tables:
-- - "MySchema"."MyTable" (preserves case)
-- - myschema.mytable (lowercase)

INSERT INTO "MySchema"."MyTable" VALUES (1, 'case-sensitive');
INSERT INTO myschema.mytable VALUES (2, 'case-insensitive');

SELECT * FROM "MySchema"."MyTable";  -- Returns: (1, 'case-sensitive')
SELECT * FROM myschema.mytable;      -- Returns: (2, 'case-insensitive')
```

### Column Names

```sql
CREATE TABLE test_table (
    "MixedCaseColumn" bigint,
    "UPPERCASECOLUMN" varchar,
    lowercasecolumn boolean
);

-- Case-sensitive mode:
SELECT "MixedCaseColumn" FROM test_table;  -- Works
SELECT "mixedcasecolumn" FROM test_table;  -- Fails - column not found

-- Case-insensitive mode:
SELECT "MixedCaseColumn" FROM test_table;  -- Works
SELECT "mixedcasecolumn" FROM test_table;  -- Works (same column)
```

## Migration Guide

### Enabling Case Sensitivity

1. **Test thoroughly** in a development environment first
2. **Backup your data** before making changes
3. **Update applications** that rely on case-insensitive behavior
4. **Review existing table/schema names** for potential conflicts

### Potential Issues

When enabling case sensitivity on existing catalogs:

- Existing lowercase names will continue to work
- Applications using mixed-case delimited identifiers may start creating separate objects
- Queries expecting case-insensitive behavior may fail

### Recommended Approach

1. **Start with new catalogs** when possible
2. **Use consistent naming conventions** (all lowercase or all delimited)
3. **Test queries** with both delimited and non-delimited identifiers
4. **Monitor for errors** after enabling the feature

## REST Catalog Compatibility

The case sensitivity feature is fully compatible with Iceberg REST catalogs. The connector will:

- Pass case-sensitive identifiers to the REST catalog as-is
- Handle case-insensitive matching through local caching when needed
- Maintain compatibility with existing REST catalog implementations

## Best Practices

1. **Choose one mode** and stick with it across your organization
2. **Use consistent naming conventions**:
   - All lowercase: `my_schema.my_table`
   - All delimited: `"MySchema"."MyTable"`
3. **Document your naming standards** for your team
4. **Test thoroughly** when migrating between modes
5. **Consider using non-delimited identifiers** for better portability

## Troubleshooting

### Common Issues

1. **Table not found errors** after enabling case sensitivity
   - Check if you're using the correct case for delimited identifiers
   - Verify the actual table name with `SHOW TABLES`

2. **Duplicate table errors** when creating tables
   - Different cases of the same name may create separate tables
   - Use `SHOW TABLES` to see existing table names

3. **Query failures** with mixed-case column names
   - Ensure column names match exactly in case-sensitive mode
   - Use `DESCRIBE table_name` to see actual column names

### Debugging

```sql
-- Check actual schema names
SHOW SCHEMAS;

-- Check actual table names
SHOW TABLES FROM schema_name;

-- Check actual column names
DESCRIBE schema_name.table_name;
```

## Performance Considerations

- Case-sensitive mode has minimal performance impact
- REST catalog caching is optimized for both modes
- No significant difference in query execution time

## Limitations

- Column names follow the same case sensitivity rules as table/schema names
- View names are subject to the same case sensitivity rules
- Materialized view names follow the same rules
- The feature applies to all schemas and tables in the catalog
