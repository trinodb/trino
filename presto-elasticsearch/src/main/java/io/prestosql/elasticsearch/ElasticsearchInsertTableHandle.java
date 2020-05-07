package io.prestosql.elasticsearch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.type.Type;

import java.util.List;

public class ElasticsearchInsertTableHandle
        implements ConnectorInsertTableHandle
{
    private final String schema;
    private final String index;
    private final List<String> columnNames;
    private final List<Type> columnTypes;

    @JsonCreator
    public ElasticsearchInsertTableHandle(
            @JsonProperty("schema") String schema,
            @JsonProperty("index") String index,
            @JsonProperty("columnNames") List<String> columnNames,
            @JsonProperty("columnTypes") List<Type> columnTypes)
    {
        this.schema = schema;
        this.index = index;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public String getIndex()
    {
        return index;
    }

    @JsonProperty
    public List<String> getColumnNames()
    {
        return columnNames;
    }

    @JsonProperty
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }
}
