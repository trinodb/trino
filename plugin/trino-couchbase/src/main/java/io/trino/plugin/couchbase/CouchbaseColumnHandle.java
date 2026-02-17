package io.trino.plugin.couchbase;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.couchbase.types.CouchbaseType;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;

import java.util.List;

import static java.util.Objects.requireNonNull;

public record CouchbaseColumnHandle(
        List<String> path,
        Type type,
        CouchbaseType cbtype)
        implements ColumnHandle
{
    public CouchbaseColumnHandle {
        path = ImmutableList.copyOf(path);
        requireNonNull(type, "type is null");
        requireNonNull(cbtype, "cbtype is null");
    }

    public String name() {
        return Joiner.on(".").join(path);
    }
}
