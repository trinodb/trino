package io.trino.loki;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import static java.util.Objects.requireNonNull;

public record LokiColumnHandle(String columnName, Type columnType, int ordinalPosition)
        implements ColumnHandle
{
    public LokiColumnHandle
    {
        requireNonNull(columnName, "columnName is null");
        requireNonNull(columnType, "columnType is null");
    }

    public ColumnMetadata columnMetadata()
    {
        return new ColumnMetadata(columnName, columnType);
    }
}
