package io.trino.plugin.teradata;

import java.util.Map;

/**
 * Utility class for building SQL statements dynamically.
 */
public class SqlBuilder
{
    private SqlBuilder()
    {
    }

    /**
     * Builds a CREATE TABLE SQL string.
     * Quotes all column names with double-quotes.
     *
     * @param schema optional schema name; can be null or empty
     * @param tableName the table name
     * @param columns a map of column names to their SQL types (e.g. "BIGINT", "VARCHAR(256)")
     * @return a CREATE TABLE SQL string
     */
    public static String buildCreateTableSql(String schema, String tableName, Map<String, String> columns)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ");
        if (schema != null && !schema.isEmpty()) {
            sb.append(schema).append(".");
        }
        sb.append(tableName).append(" (\n");

        int i = 0;
        for (Map.Entry<String, String> entry : columns.entrySet()) {
            sb.append("    \"").append(entry.getKey()).append("\" ").append(entry.getValue());
            if (i < columns.size() - 1) {
                sb.append(",");
            }
            sb.append("\n");
            i++;
        }

        sb.append(")");
        return sb.toString();
    }
}
