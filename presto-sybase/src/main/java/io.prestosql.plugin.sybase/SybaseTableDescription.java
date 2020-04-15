package io.prestosql.plugin.sybase;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

public class SybaseTableDescription {

    private final String schemaName;
    private final String tableName;
    private final String column;
    private final Number minValue;
    private final Number maxValue;
    private final Integer numberPartitions;

    public SybaseTableDescription(
        String schemaName,
        String tableName,
        String column,
        Number minValue,
        Number maxValue,
        Integer numberPartitions)
    {
        checkArgument(!isNullOrEmpty(schemaName), "schemaName is null or is empty");
        checkArgument(!isNullOrEmpty(tableName), "tableName is null or is empty");
        checkArgument(!isNullOrEmpty(column), "column is null or is empty");
        checkArgument(!isNullOrEmpty(String.valueOf(numberPartitions)), "numberPartitions is null or is empty");

        this.schemaName = schemaName;
        this.tableName = tableName;
        this.column = column;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.numberPartitions = numberPartitions;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumn() {
        return column;
    }

    public Integer getNumberPartitions() {
        return numberPartitions;
    }
}
