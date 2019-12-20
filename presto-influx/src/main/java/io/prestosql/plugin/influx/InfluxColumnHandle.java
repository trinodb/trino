package io.prestosql.plugin.influx;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.SchemaTableName;


public class InfluxColumnHandle extends InfluxColumn implements ColumnHandle {

    private final String retentionPolicy;
    private final String measurement;

    @JsonCreator
    public InfluxColumnHandle(@JsonProperty("retentionPolicy") String retentionPolicy,
                              @JsonProperty("measurement") String measurement,
                              @JsonProperty("influxName") String influxName,
                              @JsonProperty("influxType") String influxType,
                              @JsonProperty("kind") Kind kind) {
        super(influxName, influxType, kind);
        this.retentionPolicy = retentionPolicy;
        this.measurement = measurement;
    }

    public InfluxColumnHandle(String retentionPolicy,
                              String measurement,
                              InfluxColumn column) {
        this(retentionPolicy, measurement, column.getInfluxName(), column.getInfluxType(), column.getKind());
    }

    @JsonProperty
    public String getRetentionPolicy() {
        return retentionPolicy;
    }

    @JsonProperty
    public String getMeasurement() {
        return measurement;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
            .addValue(getRetentionPolicy())
            .addValue(getMeasurement())
            .toString();
    }
}
