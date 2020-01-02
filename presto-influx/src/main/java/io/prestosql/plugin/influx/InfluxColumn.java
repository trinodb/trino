/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.prestosql.plugin.influx;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;

import java.util.Locale;

import static com.google.common.base.MoreObjects.ToStringHelper;

public class InfluxColumn
        extends ColumnMetadata
{
    // map InfluxDB types to Presto types
    private static final ImmutableMap<String, Type> TYPES_MAPPING = new ImmutableMap.Builder<String, Type>()
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
            @JsonProperty("kind") Kind kind)
    {
        super(influxName.toLowerCase(Locale.ENGLISH),
                TYPES_MAPPING.get(influxType),
                null,
                kind.name().toLowerCase(Locale.ENGLISH),
                false);
        this.influxName = influxName;
        this.influxType = influxType;
        this.kind = kind;
    }

    @JsonProperty
    public String getInfluxName()
    {
        return influxName;
    }

    @JsonProperty
    public String getInfluxType()
    {
        return influxType;
    }

    @JsonProperty
    public Kind getKind()
    {
        return kind;
    }

    protected ToStringHelper toStringHelper(Object self)
    {
        ToStringHelper helper = com.google.common.base.MoreObjects.toStringHelper(self)
                .addValue(getName())
                .addValue(getType())
                .addValue(kind);
        if (!getName().equals(getInfluxName())) {
            helper.add("influx-name", getInfluxName());
        }
        return helper;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this).toString();
    }

    public enum Kind
    {
        TIME,
        TAG,
        FIELD,
    }
}
