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

import com.google.common.collect.ImmutableList;
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
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.hive.formats.UnionToRowCoercionUtils.rowTypeForUnionOfTypes;
import static java.util.function.Predicate.not;

public final class AvroTypeUtils
{
    private AvroTypeUtils() {}

    public static Type typeFromAvro(Schema schema, AvroTypeManager avroTypeManager)
            throws AvroTypeException
    {
        return typeFromAvro(schema, avroTypeManager, new HashSet<>());
    }

    private static Type typeFromAvro(final Schema schema, AvroTypeManager avroTypeManager, Set<Schema> enclosingRecords)
            throws AvroTypeException
    {
        Optional<Type> customType = avroTypeManager.overrideTypeForSchema(schema);
        if (customType.isPresent()) {
            return customType.get();
        }
        return switch (schema.getType()) {
            case NULL -> throw new UnsupportedOperationException("No null column type support");
            case BOOLEAN -> BooleanType.BOOLEAN;
            case INT -> IntegerType.INTEGER;
            case LONG -> BigintType.BIGINT;
            case FLOAT -> RealType.REAL;
            case DOUBLE -> DoubleType.DOUBLE;
            case ENUM, STRING -> VarcharType.VARCHAR;
            case FIXED, BYTES -> VarbinaryType.VARBINARY;
            case ARRAY -> new ArrayType(typeFromAvro(schema.getElementType(), avroTypeManager, enclosingRecords));
            case MAP -> new MapType(VarcharType.VARCHAR, typeFromAvro(schema.getValueType(), avroTypeManager, enclosingRecords), new TypeOperators());
            case RECORD -> {
                if (!enclosingRecords.add(schema)) {
                    throw new UnsupportedOperationException("Unable to represent recursive avro schemas in Trino Type form");
                }
                ImmutableList.Builder<RowType.Field> rowFieldTypes = ImmutableList.builder();
                for (Schema.Field field : schema.getFields()) {
                    rowFieldTypes.add(new RowType.Field(Optional.of(field.name()), typeFromAvro(field.schema(), avroTypeManager, new HashSet<>(enclosingRecords))));
                }
                yield RowType.from(rowFieldTypes.build());
            }
            case UNION -> {
                if (isSimpleNullableUnion(schema)) {
                    yield typeFromAvro(unwrapNullableUnion(schema), avroTypeManager, enclosingRecords);
                }
                else {
                    yield rowTypeForUnion(schema, avroTypeManager, enclosingRecords);
                }
            }
        };
    }

    static boolean isSimpleNullableUnion(Schema schema)
    {
        verify(schema.isUnion(), "Schema must be union");
        return schema.getTypes().stream().filter(not(Schema::isNullable)).count() == 1L;
    }

    private static Schema unwrapNullableUnion(Schema schema)
    {
        verify(schema.isUnion(), "Schema must be union");
        verify(schema.isNullable() && schema.getTypes().size() == 2);
        return schema.getTypes().stream().filter(not(Schema::isNullable)).collect(onlyElement());
    }

    private static RowType rowTypeForUnion(Schema schema, AvroTypeManager avroTypeManager, Set<Schema> enclosingRecords)
            throws AvroTypeException
    {
        verify(schema.isUnion());
        ImmutableList.Builder<Type> unionTypes = ImmutableList.builder();
        for (Schema variant : schema.getTypes()) {
            if (!variant.isNullable()) {
                unionTypes.add(typeFromAvro(variant, avroTypeManager, enclosingRecords));
            }
        }
        return rowTypeForUnionOfTypes(unionTypes.build());
    }
}
