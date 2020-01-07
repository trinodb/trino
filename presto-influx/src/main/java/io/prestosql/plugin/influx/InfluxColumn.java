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
import static java.util.Objects.requireNonNull;

public class InfluxColumn
        extends ColumnMetadata
{
    // map InfluxDB types to Presto types
    public static final ImmutableMap<String, Type> TYPES_MAPPING = new ImmutableMap.Builder<String, Type>()
            .put("string", VarcharType.VARCHAR)
            .put("boolean", BooleanType.BOOLEAN)
            .put("integer", BigintType.BIGINT)
            .put("float", DoubleType.DOUBLE)
            .put("time", TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE)
            .build();

    private final String influxName;
    private final String influxType;
    private final Kind kind;

    @JsonCreator
    public InfluxColumn(
            @JsonProperty("influxName") String influxName,
            @JsonProperty("influxType") String influxType,
            @JsonProperty("type") Type type,
            @JsonProperty("kind") Kind kind,
            @JsonProperty("hidden") boolean hidden)
    {
        super(influxName.toLowerCase(Locale.ENGLISH),
                type,
                null,
                null,
                hidden);
        this.influxName = requireNonNull(influxName);
        this.influxType = requireNonNull(influxType);
        this.kind = requireNonNull(kind);
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
    @Override
    public Type getType()
    {
        return super.getType();
    }

    @JsonProperty
    public Kind getKind()
    {
        return kind;
    }

    @JsonProperty
    @Override
    public boolean isHidden()
    {
        return super.isHidden();
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
        if (isHidden()) {
            helper.addValue("hidden");
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
