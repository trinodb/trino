package io.trino.loki;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSource;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;

import java.net.URI;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class LokiRecordSet implements RecordSet {

    private final List<LokiColumnHandle> columnHandles;
    private final List<Type> columnTypes;

    private final QueryResult result;

    public LokiRecordSet(LokiClient lokiClient, LokiSplit split, List<LokiColumnHandle> columnHandles)
    {
        requireNonNull(lokiClient, "lokiClient is null");
        requireNonNull(split, "split is null");

        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (LokiColumnHandle column : columnHandles) {
            types.add(column.columnType());
        }
        this.columnTypes = types.build();

        // Actually execute the query
        // TODO: lazily parse
        this.result = lokiClient.doQuery(); // TODO: pass split
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new LokiRecordCursor(columnHandles, result);
    }
}
