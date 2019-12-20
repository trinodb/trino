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
    private final Long limit;

    @JsonCreator
    public InfluxTableHandle(@JsonProperty("retentionPolicy") String retentionPolicy,
                             @JsonProperty("measurement") String measurement,
                             @JsonProperty("where") InfluxQL where,
                             @JsonProperty("limit") Long limit) {
        super(retentionPolicy, measurement);
        this.retentionPolicy = requireNonNull(retentionPolicy, "retentionPolicy is null");
        this.measurement = requireNonNull(measurement, "measurement is null");
        this.where = requireNonNull(where, "where is null");
        this.limit = limit;
    }

    public InfluxTableHandle(String retentionPolicy, String measurement) {
        this(retentionPolicy, measurement, new InfluxQL(), null);
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

    @JsonProperty
    public Long getLimit() {
        return limit;
    }

    public InfluxQL getFromWhere() {
        InfluxQL from = new InfluxQL("FROM ")
            .addIdentifier(getRetentionPolicy()).append('.').addIdentifier(getMeasurement());
        if (!getWhere().isEmpty()) {
            from.append(' ').append(getWhere().toString());
        }
        if (getLimit() != null) {
            from.append(" LIMIT ").append(getLimit());
        }
        return from;
    }

    @Override
    public String toString() {
        return getFromWhere().toString();
    }
}
