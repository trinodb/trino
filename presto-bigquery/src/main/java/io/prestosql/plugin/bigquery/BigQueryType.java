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
package io.prestosql.plugin.bigquery;

import com.google.cloud.bigquery.Field;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.TimeWithTimeZoneType;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;

import javax.annotation.Nonnull;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.plugin.bigquery.BigQueryMetadata.NUMERIC_DATA_TYPE_PRECISION;
import static io.prestosql.plugin.bigquery.BigQueryMetadata.NUMERIC_DATA_TYPE_SCALE;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public enum BigQueryType
{
    BOOLEAN(BooleanType.BOOLEAN),
    BYTES(VarbinaryType.VARBINARY),
    DATE(DateType.DATE),
    DATETIME(VarcharType.VARCHAR),
    FLOAT(DoubleType.DOUBLE),
    GEOGRAPHY(VarcharType.VARCHAR),
    INTEGER(BigintType.BIGINT),
    NUMERIC(DecimalType.createDecimalType(NUMERIC_DATA_TYPE_PRECISION, NUMERIC_DATA_TYPE_SCALE)),
    RECORD(null),
    STRING(createUnboundedVarcharType()),
    TIME(TimeWithTimeZoneType.TIME_WITH_TIME_ZONE),
    TIMESTAMP(TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE);

    private final Type nativeType;

    BigQueryType(Type nativeType)
    {
        this.nativeType = nativeType;
    }

    static RowType.Field toRawTypeField(Map.Entry<String, BigQueryType.Adaptor> entry)
    {
        return toRawTypeField(entry.getKey(), entry.getValue());
    }

    static RowType.Field toRawTypeField(String name, BigQueryType.Adaptor typeAdaptor)
    {
        Type prestoType = typeAdaptor.getPrestoType();
        return RowType.field(name, prestoType);
    }

    public Type getNativeType(@Nonnull BigQueryType.Adaptor typeAdaptor)
    {
        switch (this) {
            case RECORD:
                // create the row
                requireNonNull(typeAdaptor, "A type BigQueryType.Adaptor is needed in order to convert a record or struct");
                ImmutableMap<String, BigQueryType.Adaptor> subTypes = typeAdaptor.getBigQuerySubTypes();
                checkArgument(!subTypes.isEmpty(), "a record or struct must have sub-fields");
                List<RowType.Field> fields = subTypes.entrySet().stream().map(BigQueryType::toRawTypeField).collect(toList());
                return RowType.from(fields);
            default:
                return nativeType;
        }
    }

    static interface Adaptor
    {
        BigQueryType getBigQueryType();

        ImmutableMap<String, BigQueryType.Adaptor> getBigQuerySubTypes();

        Field.Mode getMode();

        default Type getPrestoType()
        {
            Type rawType = getBigQueryType().getNativeType(this);
            return getMode() == Field.Mode.REPEATED ? new ArrayType(rawType) : rawType;
        }
    }
}
