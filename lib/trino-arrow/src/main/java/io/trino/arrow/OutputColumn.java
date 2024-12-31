package io.trino.arrow;

import io.trino.spi.type.Type;

import static java.util.Objects.requireNonNull;

public record OutputColumn(int sourcePageChannel, String columnName, Type type)
{
    public OutputColumn
    {
        requireNonNull(columnName, "columnName is null");
        requireNonNull(type, "type is null");

        if (sourcePageChannel < 0) {
            throw new IllegalArgumentException("sourcePageChannel is negative");
        }
    }
}
