package io.prestosql.plugin.influx;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.SchemaTableName;

public class InfluxColumnHandle extends InfluxColumn implements ColumnHandle {

    private final SchemaTableName schemaTableName;

    @JsonCreator
    public InfluxColumnHandle(@JsonProperty("retentionPolicy") String retentionPolicy,
                              @JsonProperty("measurement") String measurement,
                              @JsonProperty("influxName") String influxName,
                              @JsonProperty("influxType") String influxType,
                              @JsonProperty("kind") Kind kind) {
        super(influxName, influxType, kind);
        this.schemaTableName = new SchemaTableName(retentionPolicy, measurement);
    }

    public InfluxColumnHandle(String retentionPolicy,
                              String measurement,
                              InfluxColumn column) {
        this(retentionPolicy, measurement, column.getInfluxName(), column.getInfluxType(), column.getKind());
    }

    public SchemaTableName getSchemaTableName() {
        return schemaTableName;
    }

    @JsonProperty
    public String getRetentionPolicy() {
        return schemaTableName.getSchemaName();
    }

    @JsonProperty
    public String getMeasurement() {
        return schemaTableName.getTableName();
    }
}
