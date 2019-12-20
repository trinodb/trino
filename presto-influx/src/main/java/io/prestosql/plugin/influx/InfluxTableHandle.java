package io.prestosql.plugin.influx;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.SchemaTableName;

import static java.util.Objects.requireNonNull;

public class InfluxTableHandle extends SchemaTableName implements ConnectorTableHandle {

    private final String retentionPolicy;
    private final String measurement;
    private final InfluxQL where;

    @JsonCreator
    public InfluxTableHandle(@JsonProperty("retentionPolicy") String retentionPolicy,
                             @JsonProperty("measurement") String measurement,
                             @JsonProperty("where") InfluxQL where) {
        super(retentionPolicy, measurement);
        this.retentionPolicy = requireNonNull(retentionPolicy, "retentionPolicy is null");
        this.measurement = requireNonNull(measurement, "measurement is null");
        this.where = requireNonNull(where, "where is null");
    }

    public InfluxTableHandle(String retentionPolicy, String measurement) {
        this(retentionPolicy, measurement, new InfluxQL());
    }

    @JsonProperty
    public String getRetentionPolicy() {
        return retentionPolicy;
    }

    @JsonProperty
    public String getMeasurement() {
        return measurement;
    }

    @JsonProperty
    public InfluxQL getWhere() {
        return where;
    }

    public InfluxQL getFromWhere() {
        InfluxQL from = new InfluxQL("FROM ")
            .addIdentifier(getRetentionPolicy()).append('.').addIdentifier(getMeasurement());
        if (!getWhere().isEmpty()) {
            from.append(' ').append(getWhere().toString());
        }
        return from;
    }

    @Override
    public String toString() {
        return getFromWhere().toString();
    }
}
