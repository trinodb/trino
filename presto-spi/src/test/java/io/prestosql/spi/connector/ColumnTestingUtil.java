package io.prestosql.spi.connector;

import io.prestosql.spi.type.VarcharType;

public class ColumnTestingUtil
{
    private ColumnTestingUtil() {}

    public static ColumnMetadata column(String columnName)
    {
        return new ColumnMetadata(columnName, VarcharType.VARCHAR);
    }
}
