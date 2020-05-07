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
package io.prestosql.elasticsearch.decoders;

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.prestosql.elasticsearch.ElasticsearchTypeHandler;
import io.prestosql.elasticsearch.client.types.ElasticField;
import io.prestosql.elasticsearch.client.types.ElasticType;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;

public class CoderFactory
{
    private static final ImmutableMap<Type, Decoder> decoders = ImmutableMap.<Type, Decoder>builder()
                .put(VARCHAR, new VarcharDecoder())
                .put(VARBINARY, new VarbinaryDecoder())
//                .put(TIMESTAMP, new TimestampDecoder(session, path))
                .put(BOOLEAN, new BooleanDecoder())
                .put(DOUBLE, new DoubleDecoder())
                .put(REAL, new RealDecoder())
                .put(TINYINT, new TinyintDecoder())
                .put(SMALLINT, new SmallintDecoder())
                .put(INTEGER, new IntegerDecoder())
                .put(BIGINT, new BigintDecoder())
//                .put(typeHandler.toPrestoType(ElasticType.IP), new IpAddressDecoder(path, typeHandler.toPrestoType(ElasticType.IP)))
                .build();


    public static Decoder createDecoder(ConnectorSession session, Type type)
    {
        if (type.equals(TIMESTAMP)) {
            return new TimestampDecoder(session);
        }
        if (type.getBaseName().equals(StandardTypes.IPADDRESS)) {
            return new IpAddressDecoder(type);
        }
        if (type instanceof RowType) {
            RowType rowType = (RowType) type;
            return new RowDecoder(session, rowType);
        }
        if (type instanceof ArrayType) {
            Type elementType = ((ArrayType) type).getElementType();
            return new ArrayDecoder(createDecoder(session, elementType));
        }
        if (decoders.containsKey(type)) {
            return decoders.get(type);
        }

        throw new UnsupportedOperationException("Type not supported: " + type);
    }

    public static Object columnValue(ConnectorSession session, Type type, Block block, int position)
    {
        Object value;
        if (block.isNull(position)) {
            value = null;
        }
        else if (BOOLEAN.equals(type)) {
            value = type.getBoolean(block, position);
        }
        else if (BIGINT.equals(type)) {
            value = type.getLong(block, position);
        }
        else if (INTEGER.equals(type)) {
            value = toIntExact(type.getLong(block, position));
        }
        else if (SMALLINT.equals(type)) {
            value = Shorts.checkedCast(type.getLong(block, position));
        }
        else if (TINYINT.equals(type)) {
            value = SignedBytes.checkedCast(type.getLong(block, position));
        }
        else if (DOUBLE.equals(type)) {
            value = type.getDouble(block, position);
        }
        else if (REAL.equals(type)) {
            value = intBitsToFloat(toIntExact(type.getLong(block, position)));
        }
        else if (DATE.equals(type)) {
            LocalDate dateTime = LocalDate.ofEpochDay(type.getLong(block, position));
            return DateTimeFormatter.ofPattern("yyyy-MM-dd").format(dateTime);
        }
        else if (TIMESTAMP.equals(type)) {
            value = createDecoder(session, type).encode(type.getLong(block, position));
//            value = new Timestamp(Instant.ofEpochMilli(type.getLong(block, position)).atZone(zoneId).toLocalDateTime().atZone(ZoneId.of("UTC")).toInstant().toEpochMilli());
//            value = new Timestamp(Instant.ofEpochMilli(type.getLong(block, position)).toEpochMilli());
        }
        else if (isVarcharType(type)) {
            value = type.getSlice(block, position).toStringUtf8();
        }
        else if (VARBINARY.equals(type)) {
            value = type.getSlice(block, position).toByteBuffer();
        }
        else if (type.getBaseName().equals(StandardTypes.IPADDRESS)) {
            value = createDecoder(session, type).encode(type.getSlice(block, position));
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
        }
        return value;
    }


}
