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
package io.trino.plugin.cassandra;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.InetAddresses;
import io.airlift.slice.Slice;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.net.InetAddresses.toAddrString;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.cassandra.util.CassandraCqlUtils.quoteStringLiteral;
import static io.trino.plugin.cassandra.util.CassandraCqlUtils.quoteStringLiteralForJson;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.util.Objects.requireNonNull;

public enum CassandraType
{
    BOOLEAN(BooleanType.BOOLEAN),

    TINYINT(TinyintType.TINYINT),
    SMALLINT(SmallintType.SMALLINT),
    INT(IntegerType.INTEGER),
    BIGINT(BigintType.BIGINT),

    FLOAT(RealType.REAL),
    DOUBLE(DoubleType.DOUBLE),
    DECIMAL(DoubleType.DOUBLE),

    DATE(DateType.DATE),
    TIMESTAMP(TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS),

    ASCII(createUnboundedVarcharType()),
    TEXT(createUnboundedVarcharType()),
    VARCHAR(createUnboundedVarcharType()),

    BLOB(VarbinaryType.VARBINARY),

    UUID(createVarcharType(Constants.UUID_STRING_MAX_LENGTH)),
    TIMEUUID(createVarcharType(Constants.UUID_STRING_MAX_LENGTH)),
    COUNTER(BigintType.BIGINT),
    VARINT(createUnboundedVarcharType()),
    INET(createVarcharType(Constants.IP_ADDRESS_STRING_MAX_LENGTH)),
    CUSTOM(VarbinaryType.VARBINARY),

    LIST(createUnboundedVarcharType()),
    SET(createUnboundedVarcharType()),
    MAP(createUnboundedVarcharType()),
    /**/;

    private static final class Constants
    {
        private static final int UUID_STRING_MAX_LENGTH = 36;
        // IPv4: 255.255.255.255 - 15 characters
        // IPv6: FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF - 39 characters
        // IPv4 embedded into IPv6: FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:255.255.255.255 - 45 characters
        private static final int IP_ADDRESS_STRING_MAX_LENGTH = 45;
    }

    private final Type trinoType;

    CassandraType(Type trinoType)
    {
        this.trinoType = requireNonNull(trinoType, "trinoType is null");
    }

    public Type getTrinoType()
    {
        return trinoType;
    }

    public static Optional<CassandraType> toCassandraType(DataType dataType)
    {
        switch (dataType.getProtocolCode()) {
            case ProtocolConstants.DataType.ASCII:
                return Optional.of(ASCII);
            case ProtocolConstants.DataType.BIGINT:
                return Optional.of(BIGINT);
            case ProtocolConstants.DataType.BLOB:
                return Optional.of(BLOB);
            case ProtocolConstants.DataType.BOOLEAN:
                return Optional.of(BOOLEAN);
            case ProtocolConstants.DataType.COUNTER:
                return Optional.of(COUNTER);
            case ProtocolConstants.DataType.CUSTOM:
                return Optional.of(CUSTOM);
            case ProtocolConstants.DataType.DATE:
                return Optional.of(DATE);
            case ProtocolConstants.DataType.DECIMAL:
                return Optional.of(DECIMAL);
            case ProtocolConstants.DataType.DOUBLE:
                return Optional.of(DOUBLE);
            case ProtocolConstants.DataType.FLOAT:
                return Optional.of(FLOAT);
            case ProtocolConstants.DataType.INET:
                return Optional.of(INET);
            case ProtocolConstants.DataType.INT:
                return Optional.of(INT);
            case ProtocolConstants.DataType.LIST:
                return Optional.of(LIST);
            case ProtocolConstants.DataType.MAP:
                return Optional.of(MAP);
            case ProtocolConstants.DataType.SET:
                return Optional.of(SET);
            case ProtocolConstants.DataType.SMALLINT:
                return Optional.of(SMALLINT);
            case ProtocolConstants.DataType.TIMESTAMP:
                return Optional.of(TIMESTAMP);
            case ProtocolConstants.DataType.TIMEUUID:
                return Optional.of(TIMEUUID);
            case ProtocolConstants.DataType.TINYINT:
                return Optional.of(TINYINT);
            case ProtocolConstants.DataType.UUID:
                return Optional.of(UUID);
            case ProtocolConstants.DataType.VARCHAR:
                return Optional.of(VARCHAR);
            case ProtocolConstants.DataType.VARINT:
                return Optional.of(VARINT);
            default:
                return Optional.empty();
        }
    }

    public NullableValue getColumnValue(Row row, int position)
    {
        if (row.isNull(position)) {
            return NullableValue.asNull(trinoType);
        }

        switch (this) {
            case ASCII:
            case TEXT:
            case VARCHAR:
                return NullableValue.of(trinoType, utf8Slice(row.getString(position)));
            case INT:
                return NullableValue.of(trinoType, (long) row.getInt(position));
            case SMALLINT:
                return NullableValue.of(trinoType, (long) row.getShort(position));
            case TINYINT:
                return NullableValue.of(trinoType, (long) row.getByte(position));
            case BIGINT:
            case COUNTER:
                return NullableValue.of(trinoType, row.getLong(position));
            case BOOLEAN:
                return NullableValue.of(trinoType, row.getBoolean(position));
            case DOUBLE:
                return NullableValue.of(trinoType, row.getDouble(position));
            case FLOAT:
                return NullableValue.of(trinoType, (long) floatToRawIntBits(row.getFloat(position)));
            case DECIMAL:
                return NullableValue.of(trinoType, row.getBigDecimal(position).doubleValue());
            case UUID:
            case TIMEUUID:
                return NullableValue.of(trinoType, utf8Slice(row.getUuid(position).toString()));
            case TIMESTAMP:
                return NullableValue.of(trinoType, packDateTimeWithZone(row.getInstant(position).toEpochMilli(), TimeZoneKey.UTC_KEY));
            case DATE:
                return NullableValue.of(trinoType, row.getLocalDate(position).toEpochDay());
            case INET:
                return NullableValue.of(trinoType, utf8Slice(toAddrString(row.getInetAddress(position))));
            case VARINT:
                return NullableValue.of(trinoType, utf8Slice(row.getBigInteger(position).toString()));
            case BLOB:
            case CUSTOM:
                return NullableValue.of(trinoType, wrappedBuffer(row.getBytesUnsafe(position)));
            case SET:
                return NullableValue.of(trinoType, utf8Slice(buildArrayValueFromSetType(row, position)));
            case LIST:
                return NullableValue.of(trinoType, utf8Slice(buildArrayValueFromListType(row, position)));
            case MAP:
                return NullableValue.of(trinoType, utf8Slice(buildMapValue(row, position)));
        }
        throw new IllegalStateException("Handling of type " + this + " is not implemented");
    }

    private static String buildMapValue(Row row, int position)
    {
        DataType type = row.getType(position);
        checkArgument(type instanceof MapType, "Expected to deal with an instance of %s class, got: %s", MapType.class, type);
        MapType mapType = (MapType) type;
        return buildMapValue((Map<?, ?>) row.getObject(position), mapType.getKeyType(), mapType.getValueType());
    }

    private static String buildMapValue(Map<?, ?> cassandraMap, DataType keyType, DataType valueType)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (Map.Entry<?, ?> entry : cassandraMap.entrySet()) {
            if (sb.length() > 1) {
                sb.append(",");
            }
            sb.append(objectToJson(entry.getKey(), keyType));
            sb.append(":");
            sb.append(objectToJson(entry.getValue(), valueType));
        }
        sb.append("}");
        return sb.toString();
    }

    private static String buildArrayValueFromListType(Row row, int position)
    {
        DataType type = row.getType(position);
        checkArgument(type instanceof ListType, "Expected to deal with an instance of %s class, got: %s", ListType.class, type);
        ListType listType = (ListType) type;
        return buildArrayValue((Collection<?>) row.getObject(position), listType.getElementType());
    }

    @VisibleForTesting
    static String buildArrayValue(Collection<?> cassandraCollection, DataType elementType)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (Object value : cassandraCollection) {
            if (sb.length() > 1) {
                sb.append(",");
            }
            sb.append(objectToJson(value, elementType));
        }
        sb.append("]");
        return sb.toString();
    }

    // TODO unify with toCqlLiteral
    public String getColumnValueForCql(Row row, int position)
    {
        if (row.isNull(position)) {
            return null;
        }

        switch (this) {
            case ASCII:
            case TEXT:
            case VARCHAR:
                return quoteStringLiteral(row.getString(position));
            case INT:
                return Integer.toString(row.getInt(position));
            case SMALLINT:
                return Short.toString(row.getShort(position));
            case TINYINT:
                return Byte.toString(row.getByte(position));
            case BIGINT:
            case COUNTER:
                return Long.toString(row.getLong(position));
            case BOOLEAN:
                return Boolean.toString(row.getBool(position));
            case DOUBLE:
                return Double.toString(row.getDouble(position));
            case FLOAT:
                return Float.toString(row.getFloat(position));
            case DECIMAL:
                return row.getBigDecimal(position).toString();
            case UUID:
            case TIMEUUID:
                return row.getUuid(position).toString();
            case TIMESTAMP:
                return Long.toString(row.getInstant(position).toEpochMilli());
            case DATE:
                return row.getLocalDate(position).toString();
            case INET:
                return quoteStringLiteral(toAddrString(row.getInetAddress(position)));
            case VARINT:
                return row.getBigInteger(position).toString();
            case BLOB:
            case CUSTOM:
                return Bytes.toHexString(row.getBytesUnsafe(position));

            case LIST:
            case SET:
            case MAP:
                // unsupported
                break;
        }
        throw new IllegalStateException("Handling of type " + this + " is not implemented");
    }

    // TODO unify with getColumnValueForCql
    public String toCqlLiteral(Object trinoNativeValue)
    {
        if (this == TIMESTAMP) {
            return String.valueOf(unpackMillisUtc((Long) trinoNativeValue));
        }

        String value;
        if (trinoNativeValue instanceof Slice) {
            value = ((Slice) trinoNativeValue).toStringUtf8();
        }
        else {
            value = trinoNativeValue.toString();
        }

        switch (this) {
            case ASCII:
            case TEXT:
            case VARCHAR:
                return quoteStringLiteral(value);
            case INET:
                // remove '/' in the string. e.g. /127.0.0.1
                return quoteStringLiteral(value.substring(1));
            default:
                return value;
        }
    }

    private static String objectToJson(Object cassandraValue, DataType dataType)
    {
        CassandraType cassandraType = toCassandraType(dataType)
                .orElseThrow(() -> new IllegalStateException("Unsupported type: " + dataType));

        switch (cassandraType) {
            case ASCII:
            case TEXT:
            case VARCHAR:
            case UUID:
            case TIMEUUID:
            case TIMESTAMP:
            case DATE:
            case INET:
            case VARINT:
                return quoteStringLiteralForJson(cassandraValue.toString());

            case BLOB:
            case CUSTOM:
                return quoteStringLiteralForJson(Bytes.toHexString((ByteBuffer) cassandraValue));

            case SMALLINT:
            case TINYINT:
            case INT:
            case BIGINT:
            case COUNTER:
            case BOOLEAN:
            case DOUBLE:
            case FLOAT:
            case DECIMAL:
                return cassandraValue.toString();
            case LIST:
                checkArgument(dataType instanceof ListType, "Expected to deal with an instance of %s class, got: %s", ListType.class, dataType);
                ListType listType = (ListType) dataType;
                return buildArrayValue((Collection<?>) cassandraValue, listType.getElementType());
            case SET:
                checkArgument(dataType instanceof SetType, "Expected to deal with an instance of %s class, got: %s", SetType.class, dataType);
                SetType setType = (SetType) dataType;
                return buildArrayValue((Collection<?>) cassandraValue, setType.getElementType());
            case MAP:
                checkArgument(dataType instanceof MapType, "Expected to deal with an instance of %s class, got: %s", MapType.class, dataType);
                MapType mapType = (MapType) dataType;
                return buildMapValue((Map<?, ?>) cassandraValue, mapType.getKeyType(), mapType.getValueType());
        }
        throw new IllegalStateException("Unsupported type: " + cassandraType);
    }

    public Object getJavaValue(Object trinoNativeValue)
    {
        switch (this) {
            case ASCII:
            case TEXT:
            case VARCHAR:
                return ((Slice) trinoNativeValue).toStringUtf8();
            case BIGINT:
            case BOOLEAN:
            case DOUBLE:
            case COUNTER:
                return trinoNativeValue;
            case INET:
                return InetAddresses.forString(((Slice) trinoNativeValue).toStringUtf8());
            case INT:
            case SMALLINT:
            case TINYINT:
                return ((Long) trinoNativeValue).intValue();
            case FLOAT:
                // conversion can result in precision lost
                return intBitsToFloat(((Long) trinoNativeValue).intValue());
            case DECIMAL:
                // conversion can result in precision lost
                // Trino uses double for decimal, so to keep the floating point precision, convert it to string.
                // Otherwise partition id doesn't match
                return new BigDecimal(trinoNativeValue.toString());
            case TIMESTAMP:
                return Instant.ofEpochMilli(unpackMillisUtc((Long) trinoNativeValue));
            case DATE:
                return LocalDate.ofEpochDay(((Long) trinoNativeValue).intValue());
            case UUID:
            case TIMEUUID:
                return java.util.UUID.fromString(((Slice) trinoNativeValue).toStringUtf8());
            case BLOB:
            case CUSTOM:
                return ((Slice) trinoNativeValue).toStringUtf8();
            case VARINT:
                return new BigInteger(((Slice) trinoNativeValue).toStringUtf8());
            case SET:
            case LIST:
            case MAP:
        }
        throw new IllegalStateException("Back conversion not implemented for " + this);
    }

    public boolean isSupportedPartitionKey()
    {
        switch (this) {
            case ASCII:
            case TEXT:
            case VARCHAR:
            case BIGINT:
            case BOOLEAN:
            case DOUBLE:
            case INET:
            case INT:
            case FLOAT:
            case DECIMAL:
            case TIMESTAMP:
            case UUID:
            case TIMEUUID:
                return true;
            case COUNTER:
            case BLOB:
            case CUSTOM:
            case VARINT:
            case SET:
            case LIST:
            case MAP:
            default:
                return false;
        }
    }

    public static boolean isFullySupported(DataType dataType)
    {
        if (toCassandraType(dataType).isEmpty()) {
            return false;
        }

        if (dataType instanceof UserDefinedType) {
            return ((UserDefinedType) dataType).getFieldTypes().stream()
                    .allMatch(CassandraType::isFullySupported);
        }

        if (dataType instanceof MapType) {
            MapType mapType = (MapType) dataType;
            return Arrays.stream(new DataType[] {mapType.getKeyType(), mapType.getValueType()})
                    .allMatch(CassandraType::isFullySupported);
        }

        if (dataType instanceof ListType) {
            return CassandraType.isFullySupported(((ListType) dataType).getElementType());
        }

        if (dataType instanceof TupleType) {
            return ((TupleType) dataType).getComponentTypes().stream()
                    .allMatch(CassandraType::isFullySupported);
        }

        if (dataType instanceof SetType) {
            return CassandraType.isFullySupported(((SetType) dataType).getElementType());
        }

        return true;
    }

    public static CassandraType toCassandraType(Type type, ProtocolVersion protocolVersion)
    {
        if (type.equals(BooleanType.BOOLEAN)) {
            return BOOLEAN;
        }
        if (type.equals(BigintType.BIGINT)) {
            return BIGINT;
        }
        if (type.equals(IntegerType.INTEGER)) {
            return INT;
        }
        if (type.equals(SmallintType.SMALLINT)) {
            return SMALLINT;
        }
        if (type.equals(TinyintType.TINYINT)) {
            return TINYINT;
        }
        if (type.equals(DoubleType.DOUBLE)) {
            return DOUBLE;
        }
        if (type.equals(RealType.REAL)) {
            return FLOAT;
        }
        if (type instanceof VarcharType) {
            return TEXT;
        }
        if (type.equals(DateType.DATE)) {
            return DATE;
        }
        if (type.equals(VarbinaryType.VARBINARY)) {
            return BLOB;
        }
        if (type.equals(TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS)) {
            return TIMESTAMP;
        }
        throw new IllegalArgumentException("unsupported type: " + type);
    }

    private static String buildArrayValueFromSetType(Row row, int position)
    {
        DataType type = row.getType(position);
        checkArgument(type instanceof SetType, "Expected to deal with an instance of %s class, got: %s", SetType.class, type);
        SetType setType = (SetType) type;
        return buildArrayValue((Collection<?>) row.getObject(position), setType.getElementType());
    }
}
