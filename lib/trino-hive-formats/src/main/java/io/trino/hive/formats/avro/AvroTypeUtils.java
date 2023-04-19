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
package io.trino.hive.formats.avro;

import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.avro.Schema;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.function.Predicate.not;

public class AvroTypeUtils
{
    private AvroTypeUtils() {}

    public static Type typeFromAvro(Schema schema, AvroTypeManager avroTypeManager)
    {
        return typeFromAvro(schema, avroTypeManager, new HashSet<>());
    }

    private static Type typeFromAvro(final Schema schema, AvroTypeManager avroTypeManager, Set<Schema> enclosingRecords)
    {
        Optional<Type> customType = avroTypeManager.overrideTypeForSchema(schema);
        return customType.orElseGet(() -> switch (schema.getType()) {
            case RECORD -> {
                if (!enclosingRecords.add(schema)) {
                    throw new UnsupportedOperationException("Unable to represent recursive avro schemas in Trino Type form");
                }
                yield RowType.from(schema.getFields()
                        .stream()
                        .map(field ->
                        {
                            return new RowType.Field(Optional.of(field.name()), typeFromAvro(field.schema(), avroTypeManager, new HashSet<>(enclosingRecords)));
                        }).collect(toImmutableList()));
            }
            case ENUM -> VarcharType.VARCHAR;
            case ARRAY -> new ArrayType(typeFromAvro(schema.getElementType(), avroTypeManager, enclosingRecords));
            case MAP -> new MapType(VarcharType.VARCHAR, typeFromAvro(schema.getValueType(), avroTypeManager, enclosingRecords), new TypeOperators());
            case UNION ->
                    typeFromAvro(unwrapNullableUnion(schema).orElseThrow(() -> new UnsupportedOperationException("Unable to make Trino Type from non nullable Avro Union: %s".formatted(schema))), avroTypeManager, enclosingRecords);
            case FIXED, BYTES -> VarbinaryType.VARBINARY;
            case STRING -> VarcharType.VARCHAR;
            case INT -> IntegerType.INTEGER;
            case LONG -> BigintType.BIGINT;
            case FLOAT -> RealType.REAL;
            case DOUBLE -> DoubleType.DOUBLE;
            case BOOLEAN -> BooleanType.BOOLEAN;
            case NULL -> throw new UnsupportedOperationException("No null column type support");
        });
    }

    public static Optional<Schema> unwrapNullableUnion(Schema schema)
    {
        verify(schema.isUnion());
        if (schema.isNullable() && schema.getTypes().size() == 2) {
            return schema.getTypes().stream().filter(not(Schema::isNullable)).findFirst();
        }
        return Optional.empty();
    }
}
