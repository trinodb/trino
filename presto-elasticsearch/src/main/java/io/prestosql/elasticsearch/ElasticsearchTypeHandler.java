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
package io.prestosql.elasticsearch;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.prestosql.elasticsearch.client.types.DateTimeFieldType;
import io.prestosql.elasticsearch.client.types.ElasticField;
import io.prestosql.elasticsearch.client.types.ElasticFieldType;
import io.prestosql.elasticsearch.client.types.ElasticType;
import io.prestosql.elasticsearch.client.types.ObjectFieldType;
import io.prestosql.elasticsearch.client.types.PrimitiveFieldType;
import io.prestosql.elasticsearch.decoders.IpAddressDecoder;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeId;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

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
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class ElasticsearchTypeHandler
{
    private final ImmutableMap<ElasticType, Type> typeMapper;

    public ElasticsearchTypeHandler(TypeManager typeManager)
    {
        requireNonNull(typeManager, "typeManager is null");

        typeMapper = new ImmutableMap.Builder<ElasticType, Type>()
                .put(ElasticType.BINARY, VarbinaryType.VARBINARY)
                .put(ElasticType.BOOLEAN, BooleanType.BOOLEAN)
                .put(ElasticType.BYTE, TinyintType.TINYINT)
                .put(ElasticType.DATE, TimestampType.TIMESTAMP)
                .put(ElasticType.DATE_NANOS, TimestampType.TIMESTAMP)
                .put(ElasticType.DOUBLE, DoubleType.DOUBLE)
                .put(ElasticType.FLOAT, RealType.REAL)
                .put(ElasticType.INTEGER, IntegerType.INTEGER)
                .put(ElasticType.IP, typeManager.getType(new TypeSignature(StandardTypes.IPADDRESS)))
                .put(ElasticType.KEYWORD, VarcharType.VARCHAR)
                .put(ElasticType.LONG, BigintType.BIGINT)
                .put(ElasticType.SHORT, SmallintType.SMALLINT)
                .put(ElasticType.TEXT, VarcharType.VARCHAR)
                .build();
    }

    public Type toPrestoType(ElasticType type)
    {
        return typeMapper.get(type);
    }

    public Type toPrestoType(ElasticField elasticField)
    {
        return toPrestoType(elasticField, elasticField.isArray());
    }

    public Type toPrestoType(ElasticField elasticField, boolean isArray)
    {
        if (isArray) {
            Type elementType = toPrestoType(elasticField, false);
            return new ArrayType(elementType);
        }
        ElasticFieldType type = elasticField.getType();
        if (type instanceof PrimitiveFieldType) {
            ElasticType elasticType = (((PrimitiveFieldType) type).getType());
            return typeMapper.get(elasticType);
        }
        else if (type instanceof DateTimeFieldType) {
            if (((DateTimeFieldType) type).getFormats().isEmpty()) {
                return TIMESTAMP;
            }
            // otherwise, skip -- we don't support custom formats, yet
        }
        else if (type instanceof ObjectFieldType) {
            ObjectFieldType objectType = (ObjectFieldType) type;

            ImmutableList.Builder<RowType.Field> builder = ImmutableList.builder();
            for (ElasticField field : objectType.getFields()) {
                io.prestosql.spi.type.Type prestoType = toPrestoType(field);
                if (prestoType != null) {
                    builder.add(RowType.field(field.getName(), prestoType));
                }
            }

            List<RowType.Field> fields = builder.build();

            if (!fields.isEmpty()) {
                return RowType.from(fields);
            }

            // otherwise, skip -- row types must have at least 1 field
        }

        return null;
    }

    public ElasticFieldType toElasticType(Type type)
    {
        ElasticType elasticType = typeMapper.entrySet().stream()
                .filter(t -> t.getValue().equals(type))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Unknown type '" + type + "'"));

        return elasticType.getFieldType();
    }

    public Object columnValue(Type type, Block block, int position, ZoneId zoneId)
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
            value = new Timestamp(Instant.ofEpochMilli(type.getLong(block, position)).atZone(zoneId).toLocalDateTime().atZone(ZoneId.of("UTC")).toInstant().toEpochMilli());
//            value = new Timestamp(Instant.ofEpochMilli(type.getLong(block, position)).toEpochMilli());
        }
        else if (isVarcharType(type)) {
            value = type.getSlice(block, position).toStringUtf8();
        }
        else if (VARBINARY.equals(type)) {
            value = type.getSlice(block, position).toByteBuffer();
        }
        else if (typeMapper.get(ElasticType.IP).equals(type)) {
            value = IpAddressDecoder.castToVarchar(type.getSlice(block, position)).toStringUtf8();
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
        }
        return value;
    }
}
