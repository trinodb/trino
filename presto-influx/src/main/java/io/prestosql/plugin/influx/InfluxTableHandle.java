package io.prestosql.plugin.influx;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.SchemaTableName;

public class InfluxTableHandle extends SchemaTableName implements ConnectorTableHandle {

    @JsonCreator
    public InfluxTableHandle(@JsonProperty("schema") String schemaName,
                             @JsonProperty("table") String tableName) {
        super(schemaName, tableName);
    }
}
