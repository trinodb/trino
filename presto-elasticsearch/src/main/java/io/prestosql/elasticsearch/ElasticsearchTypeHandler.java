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
import io.prestosql.elasticsearch.client.types.DateTimeFieldType;
import io.prestosql.elasticsearch.client.types.ElasticField;
import io.prestosql.elasticsearch.client.types.ElasticFieldType;
import io.prestosql.elasticsearch.client.types.ElasticType;
import io.prestosql.elasticsearch.client.types.ObjectFieldType;
import io.prestosql.elasticsearch.client.types.PrimitiveFieldType;
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
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;

import java.util.List;
import java.util.Map;

import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
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
}
