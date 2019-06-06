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
package io.prestosql.plugin.cassandra;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.Bytes;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.InetAddresses;
import io.airlift.slice.Slice;
import io.prestosql.plugin.cassandra.util.CassandraCqlUtils;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.predicate.NullableValue;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarbinaryType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.net.InetAddresses.toAddrString;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.util.Objects.requireNonNull;

public enum CassandraType
{
    ASCII(createUnboundedVarcharType()),
    BIGINT(BigintType.BIGINT),
    BLOB(VarbinaryType.VARBINARY),
    CUSTOM(VarbinaryType.VARBINARY),
    BOOLEAN(BooleanType.BOOLEAN),
    COUNTER(BigintType.BIGINT),
    DECIMAL(DoubleType.DOUBLE),
    DOUBLE(DoubleType.DOUBLE),
    FLOAT(RealType.REAL),
    INET(createVarcharType(Constants.IP_ADDRESS_STRING_MAX_LENGTH)),
    INT(IntegerType.INTEGER),
    SMALLINT(SmallintType.SMALLINT),
    TINYINT(TinyintType.TINYINT),
    TEXT(createUnboundedVarcharType()),
    DATE(DateType.DATE),
    TIMESTAMP(TimestampType.TIMESTAMP),
    UUID(createVarcharType(Constants.UUID_STRING_MAX_LENGTH)),
    TIMEUUID(createVarcharType(Constants.UUID_STRING_MAX_LENGTH)),
    VARCHAR(createUnboundedVarcharType()),
    VARINT(createUnboundedVarcharType()),
    LIST(createUnboundedVarcharType()),
    MAP(createUnboundedVarcharType()),
    SET(createUnboundedVarcharType());

    private static class Constants
    {
        private static final int UUID_STRING_MAX_LENGTH = 36;
        // IPv4: 255.255.255.255 - 15 characters
        // IPv6: FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF - 39 characters
        // IPv4 embedded into IPv6: FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:255.255.255.255 - 45 characters
        private static final int IP_ADDRESS_STRING_MAX_LENGTH = 45;
    }

    private final Type nativeType;

    CassandraType(Type nativeType)
    {
        this.nativeType = requireNonNull(nativeType, "nativeType is null");
    }

    public Type getNativeType()
    {
        return nativeType;
    }

    public static Optional<CassandraType> toCassandraType(DataType.Name name)
    {
        switch (name) {
            case ASCII:
                return Optional.of(ASCII);
            case BIGINT:
                return Optional.of(BIGINT);
            case BLOB:
                return Optional.of(BLOB);
            case BOOLEAN:
                return Optional.of(BOOLEAN);
            case COUNTER:
                return Optional.of(COUNTER);
            case CUSTOM:
                return Optional.of(CUSTOM);
            case DATE:
                return Optional.of(DATE);
            case DECIMAL:
                return Optional.of(DECIMAL);
            case DOUBLE:
                return Optional.of(DOUBLE);
            case FLOAT:
                return Optional.of(FLOAT);
            case INET:
                return Optional.of(INET);
            case INT:
                return Optional.of(INT);
            case LIST:
                return Optional.of(LIST);
            case MAP:
                return Optional.of(MAP);
            case SET:
                return Optional.of(SET);
            case SMALLINT:
                return Optional.of(SMALLINT);
            case TEXT:
                return Optional.of(TEXT);
            case TIMESTAMP:
                return Optional.of(TIMESTAMP);
            case TIMEUUID:
                return Optional.of(TIMEUUID);
            case TINYINT:
                return Optional.of(TINYINT);
            case UUID:
                return Optional.of(UUID);
            case VARCHAR:
                return Optional.of(VARCHAR);
            case VARINT:
                return Optional.of(VARINT);
            default:
                return Optional.empty();
        }
    }

    public static NullableValue getColumnValue(Row row, int position, CassandraType cassandraType)
    {
        Type nativeType = cassandraType.getNativeType();
        if (row.isNull(position)) {
            return NullableValue.asNull(nativeType);
        }
        else {
            switch (cassandraType) {
                case ASCII:
                case TEXT:
                case VARCHAR:
                    return NullableValue.of(nativeType, utf8Slice(row.getString(position)));
                case INT:
                    return NullableValue.of(nativeType, (long) row.getInt(position));
                case SMALLINT:
                    return NullableValue.of(nativeType, (long) row.getShort(position));
                case TINYINT:
                    return NullableValue.of(nativeType, (long) row.getByte(position));
                case BIGINT:
                case COUNTER:
                    return NullableValue.of(nativeType, row.getLong(position));
                case BOOLEAN:
                    return NullableValue.of(nativeType, row.getBool(position));
                case DOUBLE:
                    return NullableValue.of(nativeType, row.getDouble(position));
                case FLOAT:
                    return NullableValue.of(nativeType, (long) floatToRawIntBits(row.getFloat(position)));
                case DECIMAL:
                    return NullableValue.of(nativeType, row.getDecimal(position).doubleValue());
                case UUID:
                case TIMEUUID:
                    return NullableValue.of(nativeType, utf8Slice(row.getUUID(position).toString()));
                case TIMESTAMP:
                    return NullableValue.of(nativeType, row.getTimestamp(position).getTime());
                case DATE:
                    return NullableValue.of(nativeType, (long) row.getDate(position).getDaysSinceEpoch());
                case INET:
                    return NullableValue.of(nativeType, utf8Slice(toAddrString(row.getInet(position))));
                case VARINT:
                    return NullableValue.of(nativeType, utf8Slice(row.getVarint(position).toString()));
                case BLOB:
                case CUSTOM:
                    return NullableValue.of(nativeType, wrappedBuffer(row.getBytesUnsafe(position)));
                case SET:
                case LIST:
                    return NullableValue.of(nativeType, utf8Slice(buildArrayValue(row, position)));
                case MAP:
                    return NullableValue.of(nativeType, utf8Slice(buildMapValue(row, position)));
                default:
                    throw new IllegalStateException("Handling of type " + cassandraType
                            + " is not implemented");
            }
        }
    }

    public static NullableValue getColumnValueForPartitionKey(Row row, int position, CassandraType cassandraType)
    {
        Type nativeType = cassandraType.getNativeType();
        if (row.isNull(position)) {
            return NullableValue.asNull(nativeType);
        }
        switch (cassandraType) {
            case ASCII:
            case TEXT:
            case VARCHAR:
                return NullableValue.of(nativeType, utf8Slice(row.getString(position)));
            case UUID:
            case TIMEUUID:
                return NullableValue.of(nativeType, utf8Slice(row.getUUID(position).toString()));
            default:
                return getColumnValue(row, position, cassandraType);
        }
    }

    private static String buildMapValue(Row row, int position)
    {
        DataType type = row.getColumnDefinitions().getType(position);
        checkArgument(type.getTypeArguments().size() == 2, "Expected two type arguments, got: %s", type.getTypeArguments());
        DataType keyType = type.getTypeArguments().get(0);
        DataType valueType = type.getTypeArguments().get(1);
        return buildMapValue((Map<?, ?>) row.getObject(position), keyType, valueType);
    }

    private static String buildMapValue(Map<?, ?> map, DataType keyType, DataType valueType)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (sb.length() > 1) {
                sb.append(",");
            }
            sb.append(objectToString(entry.getKey(), keyType));
            sb.append(":");
            sb.append(objectToString(entry.getValue(), valueType));
        }
        sb.append("}");
        return sb.toString();
    }

    private static String buildArrayValue(Row row, int position)
    {
        DataType type = row.getColumnDefinitions().getType(position);
        DataType elementType = getOnlyElement(type.getTypeArguments());
        return buildArrayValue((Collection<?>) row.getObject(position), elementType);
    }

    @VisibleForTesting
    static String buildArrayValue(Collection<?> collection, DataType elementType)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (Object value : collection) {
            if (sb.length() > 1) {
                sb.append(",");
            }
            sb.append(objectToString(value, elementType));
        }
        sb.append("]");
        return sb.toString();
    }

    public static String getColumnValueForCql(Row row, int position, CassandraType cassandraType)
    {
        if (row.isNull(position)) {
            return null;
        }
        else {
            switch (cassandraType) {
                case ASCII:
                case TEXT:
                case VARCHAR:
                    return CassandraCqlUtils.quoteStringLiteral(row.getString(position));
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
                    return row.getDecimal(position).toString();
                case UUID:
                case TIMEUUID:
                    return row.getUUID(position).toString();
                case TIMESTAMP:
                    return Long.toString(row.getTimestamp(position).getTime());
                case DATE:
                    return row.getDate(position).toString();
                case INET:
                    return CassandraCqlUtils.quoteStringLiteral(toAddrString(row.getInet(position)));
                case VARINT:
                    return row.getVarint(position).toString();
                case BLOB:
                case CUSTOM:
                    return Bytes.toHexString(row.getBytesUnsafe(position));
                default:
                    throw new IllegalStateException("Handling of type " + cassandraType
                            + " is not implemented");
            }
        }
    }

    private static String objectToString(Object object, DataType dataType)
    {
        CassandraType cassandraType = toCassandraType(dataType.getName())
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
                return CassandraCqlUtils.quoteStringLiteralForJson(object.toString());

            case BLOB:
            case CUSTOM:
                return CassandraCqlUtils.quoteStringLiteralForJson(Bytes.toHexString((ByteBuffer) object));

            case SMALLINT:
            case TINYINT:
            case INT:
            case BIGINT:
            case COUNTER:
            case BOOLEAN:
            case DOUBLE:
            case FLOAT:
            case DECIMAL:
                return object.toString();
            case LIST:
            case SET:
                return buildArrayValue((Collection<?>) object, getOnlyElement(dataType.getTypeArguments()));
            case MAP:
                return buildMapValue((Map<?, ?>) object, dataType.getTypeArguments().get(0), dataType.getTypeArguments().get(1));
            default:
                throw new IllegalStateException("Unsupported type: " + cassandraType);
        }
    }

    public Object getJavaValue(Object nativeValue)
    {
        switch (this) {
            case ASCII:
            case TEXT:
            case VARCHAR:
                return ((Slice) nativeValue).toStringUtf8();
            case BIGINT:
            case BOOLEAN:
            case DOUBLE:
            case COUNTER:
                return nativeValue;
            case INET:
                return InetAddresses.forString(((Slice) nativeValue).toStringUtf8());
            case INT:
            case SMALLINT:
            case TINYINT:
                return ((Long) nativeValue).intValue();
            case FLOAT:
                // conversion can result in precision lost
                return intBitsToFloat(((Long) nativeValue).intValue());
            case DECIMAL:
                // conversion can result in precision lost
                // Presto uses double for decimal, so to keep the floating point precision, convert it to string.
                // Otherwise partition id doesn't match
                return new BigDecimal(nativeValue.toString());
            case TIMESTAMP:
                return new Date((Long) nativeValue);
            case DATE:
                return LocalDate.fromDaysSinceEpoch(((Long) nativeValue).intValue());
            case UUID:
            case TIMEUUID:
                return java.util.UUID.fromString(((Slice) nativeValue).toStringUtf8());
            case BLOB:
            case CUSTOM:
                return ((Slice) nativeValue).toStringUtf8();
            case VARINT:
                return new BigInteger(((Slice) nativeValue).toStringUtf8());
            case SET:
            case LIST:
            case MAP:
            default:
                throw new IllegalStateException("Back conversion not implemented for " + this);
        }
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

    public Object validateClusteringKey(Object value)
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
            case SMALLINT:
            case TINYINT:
            case FLOAT:
            case DECIMAL:
            case TIMESTAMP:
            case DATE:
            case UUID:
            case TIMEUUID:
                return value;
            case COUNTER:
            case BLOB:
            case CUSTOM:
            case VARINT:
            case SET:
            case LIST:
            case MAP:
            default:
                // todo should we just skip partition pruning instead of throwing an exception?
                throw new PrestoException(NOT_SUPPORTED, "Unsupported clustering key type: " + this);
        }
    }

    public static boolean isFullySupported(DataType dataType)
    {
        if (!toCassandraType(dataType.getName()).isPresent()) {
            return false;
        }

        return dataType.getTypeArguments().stream()
                       .allMatch(CassandraType::isFullySupported);
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
        if (isVarcharType(type)) {
            return TEXT;
        }
        if (type.equals(DateType.DATE)) {
            return protocolVersion.toInt() <= ProtocolVersion.V3.toInt() ? TEXT : DATE;
        }
        if (type.equals(VarbinaryType.VARBINARY)) {
            return BLOB;
        }
        if (type.equals(TimestampType.TIMESTAMP)) {
            return TIMESTAMP;
        }
        throw new IllegalArgumentException("unsupported type: " + type);
    }
}
