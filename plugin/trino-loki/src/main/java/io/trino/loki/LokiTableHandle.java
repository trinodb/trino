package io.trino.loki;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.trino.spi.connector.ConnectorTableHandle;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public record LokiTableHandle(String tableName) implements ConnectorTableHandle {
    public LokiTableHandle
    {
        requireNonNull(tableName, "tableName is null");
    }

}
