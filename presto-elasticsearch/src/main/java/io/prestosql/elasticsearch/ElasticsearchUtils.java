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

import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.StandardTypes.ARRAY;
import static io.prestosql.spi.type.StandardTypes.MAP;
import static io.prestosql.spi.type.StandardTypes.ROW;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class ElasticsearchUtils
{
    private ElasticsearchUtils() {}

    public static Block serializeObject(Type type, BlockBuilder builder, Object object)
    {
        if (ROW.equals(type.getTypeSignature().getBase())) {
            return serializeStruct(type, builder, object);
        }
        if (MAP.equals(type.getTypeSignature().getBase()) || ARRAY.equals(type.getTypeSignature().getBase())) {
            throw new IllegalArgumentException("Type not supported: " + type.getDisplayName());
        }
        return serializePrimitive(type, builder, object);
    }

    private static Block serializeStruct(Type type, BlockBuilder builder, Object object)
    {
        if (object == null) {
            requireNonNull(builder, "builder is null");
            builder.appendNull();
            return builder.build();
        }

        if (builder == null) {
            builder = type.createBlockBuilder(null, 1);
        }

        BlockBuilder currentBuilder = builder.beginBlockEntry();
        List<Type> typeParameters = type.getTypeParameters();

        for (int i = 0; i < typeParameters.size(); i++) {
            Optional<String> fieldName = type.getTypeSignature().getParameters().get(i).getNamedTypeSignature().getName();
            String name = "";
            if (fieldName.isPresent()) {
                name = fieldName.get().toLowerCase(ENGLISH);
            }
            Object value = ((Map) object).get(name);
            serializeObject(typeParameters.get(i), currentBuilder, value);
        }

        builder.closeEntry();
        return (Block) type.getObject(builder, 0);
    }

    private static Block serializePrimitive(Type type, BlockBuilder builder, Object object)
    {
        requireNonNull(builder, "builder is null");

        if (object == null) {
            builder.appendNull();
            return builder.build();
        }

        if (type.equals(BOOLEAN)) {
            type.writeBoolean(builder, (Boolean) object);
        }
        else if (type.equals(BIGINT)) {
            type.writeLong(builder, (Long) object);
        }
        else if (type.equals(DOUBLE)) {
            type.writeDouble(builder, (Double) object);
        }
        else if (type.equals(INTEGER)) {
            type.writeLong(builder, (Integer) object);
        }
        else if (type.equals(VARCHAR) || type.equals(VARBINARY)) {
            type.writeSlice(builder, utf8Slice(object.toString()));
        }
        else {
            throw new IllegalArgumentException("Unknown primitive type: " + type.getDisplayName());
        }
        return builder.build();
    }
}
