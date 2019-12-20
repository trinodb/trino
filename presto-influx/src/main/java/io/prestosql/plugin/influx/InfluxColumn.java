package io.prestosql.plugin.influx;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.type.*;

public class InfluxColumn extends ColumnMetadata {

    // map InfluxDB types to Presto types
    private final static ImmutableMap<String, Type> TYPES_MAPPING = new ImmutableMap.Builder<String, Type>()
        .put("string", VarcharType.VARCHAR)
        .put("boolean", BooleanType.BOOLEAN)
        .put("integer", BigintType.BIGINT)
        .put("float", DoubleType.DOUBLE)
        .put("time", TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE)
        .build();
    public static final InfluxColumn TIME = new InfluxColumn("time", "time", Kind.TIME);

    private final String influxName;
    private final String influxType;
    private final Kind kind;

    @JsonCreator
    public InfluxColumn(@JsonProperty("influxName") String influxName,
                        @JsonProperty("influxType") String influxType,
                        @JsonProperty("kind") Kind kind) {
        super(influxName.toLowerCase(), TYPES_MAPPING.get(influxType), null, kind.name().toLowerCase(), false);
        this.influxName = influxName;
        this.influxType = influxType;
        this.kind = kind;
    }

    @JsonProperty
    public String getInfluxName() {
        return influxName;
    }

    @JsonProperty
    public String getInfluxType() {
        return influxType;
    }

    @JsonProperty
    public Kind getKind() {
        return kind;
    }

    protected MoreObjects.ToStringHelper toStringHelper(Object self) {
        MoreObjects.ToStringHelper helper = com.google.common.base.MoreObjects.toStringHelper(self)
            .addValue(getName())
            .addValue(getType())
            .addValue(kind);
        if (!getName().equals(getInfluxName())) {
            helper.add("influx-name", getInfluxName());
        }
        return helper;
    }

    @Override
    public String toString() {
       return toStringHelper(this).toString();
    }

    public enum Kind {
        TIME,
        TAG,
        FIELD,
    }
}
