package io.prestosql.plugin.sybase;

import static java.util.Objects.requireNonNull;
import static java.util.Locale.ENGLISH;

public class CaseSensitiveMappedSchemaTableName
{
    private final String schemaName;
    private final String schemaNameLower;
    private final String tableName;
    private final String tableNameLower;

    public CaseSensitiveMappedSchemaTableName(String schemaName, String tableName)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        if(schemaName.isEmpty()) {
            throw new IllegalArgumentException("schemaName is empty");
        }
        this.schemaNameLower = schemaName.toLowerCase(ENGLISH);

        this.tableName = requireNonNull(tableName, "tableName is null");
        if(tableName.isEmpty()) {
            throw new IllegalArgumentException("tableName is empty");
        }
        this.tableNameLower = tableName.toLowerCase(ENGLISH);
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getSchemaNameLower() {
        return schemaNameLower;
    }

    public String getTableName() {
        return tableName;
    }

    public String getTableNameLower() {
        return tableNameLower;
    }
}
