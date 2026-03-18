package io.trino.plugin.couchbase;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;

import java.util.List;

public record CouchbaseColumnHandle(List<String> path, Type type)
        implements ColumnHandle
{
    public CouchbaseColumnHandle {
        path = ImmutableList.copyOf(path);
    }

    public String fullName() {
        return String.format("`%s`", Joiner.on("`.`").join(path));
    }

    public String name() {
        return path.getLast();
    }
}
