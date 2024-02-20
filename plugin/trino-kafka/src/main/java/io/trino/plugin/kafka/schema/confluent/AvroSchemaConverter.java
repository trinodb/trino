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
package io.trino.plugin.kafka.schema.confluent;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.avro.Schema.Type.ARRAY;
import static org.apache.avro.Schema.Type.BYTES;
import static org.apache.avro.Schema.Type.DOUBLE;
import static org.apache.avro.Schema.Type.ENUM;
import static org.apache.avro.Schema.Type.FIXED;
import static org.apache.avro.Schema.Type.FLOAT;
import static org.apache.avro.Schema.Type.INT;
import static org.apache.avro.Schema.Type.LONG;
import static org.apache.avro.Schema.Type.MAP;
import static org.apache.avro.Schema.Type.NULL;
import static org.apache.avro.Schema.Type.RECORD;
import static org.apache.avro.Schema.Type.STRING;
import static org.apache.avro.Schema.Type.UNION;

public class AvroSchemaConverter
{
    public static final String DUMMY_FIELD_NAME = "$empty_field_marker";

    public static final RowType DUMMY_ROW_TYPE = RowType.from(ImmutableList.of(new RowType.Field(Optional.of(DUMMY_FIELD_NAME), BooleanType.BOOLEAN)));

    public enum EmptyFieldStrategy
    {
        IGNORE,
        MARK,
        FAIL,
    }

    private static final Set<Schema.Type> INTEGRAL_TYPES = ImmutableSet.of(INT, LONG);
    private static final Set<Schema.Type> DECIMAL_TYPES = ImmutableSet.of(FLOAT, DOUBLE);
    private static final Set<Schema.Type> STRING_TYPES = ImmutableSet.of(STRING, ENUM);
    private static final Set<Schema.Type> BINARY_TYPES = ImmutableSet.of(BYTES, FIXED);

    private final TypeManager typeManager;
    private final EmptyFieldStrategy emptyFieldStrategy;

    public AvroSchemaConverter(TypeManager typeManager, EmptyFieldStrategy emptyFieldStrategy)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.emptyFieldStrategy = requireNonNull(emptyFieldStrategy, "emptyFieldStrategy is null");
    }

    public List<Type> convertAvroSchema(Schema schema)
    {
        requireNonNull(schema, "schema is null");
        List<Type> types;
        if (schema.getType().equals(RECORD)) {
            types = convertRecordSchema(schema);
        }
        else {
            types = convertSimpleSchema(schema);
        }
        checkState(!types.isEmpty(), "Schema has no valid fields: '%s'", schema);
        return types;
    }

    private List<Type> convertRecordSchema(Schema schema)
    {
        checkState(schema.getType().equals(RECORD), "schema is not an avro record");
        ImmutableList.Builder<Type> builder = ImmutableList.builder();
        for (Field field : schema.getFields()) {
            convert(field.schema()).ifPresent(builder::add);
        }
        return builder.build();
    }

    private List<Type> convertSimpleSchema(Schema schema)
    {
        checkState(!schema.getType().equals(RECORD), "Unexpected type for simple schema, cannot be a record");
        return convert(schema).stream()
                .collect(toImmutableList());
    }

    private Optional<Type> convert(Schema schema)
    {
        switch (schema.getType()) {
            case INT:
                return Optional.of(IntegerType.INTEGER);
            case LONG:
                return Optional.of(BigintType.BIGINT);
            case BOOLEAN:
                return Optional.of(BooleanType.BOOLEAN);
            case FLOAT:
                return Optional.of(RealType.REAL);
            case DOUBLE:
                return Optional.of(DoubleType.DOUBLE);
            case ENUM:
            case STRING:
                return Optional.of(VarcharType.VARCHAR);
            case BYTES:
            case FIXED:
                return Optional.of(VarbinaryType.VARBINARY);
            case UNION:
                return convertUnion(schema);
            case ARRAY:
                return convertArray(schema);
            case MAP:
                return convertMap(schema);
            case RECORD:
                return convertRecord(schema);
            case NULL:
                // unsupported
                break;
        }
        throw new UnsupportedOperationException(format("Type %s not supported", schema.getType()));
    }

    private Optional<Type> convertUnion(Schema schema)
    {
        checkArgument(schema.getType().equals(UNION), "schema is not a union schema");
        // Cannot use ImmutableSet.Builder because types may contain multiple FIXED types with different sizes
        Set<Schema.Type> types = schema.getTypes().stream()
                .map(Schema::getType)
                .collect(toImmutableSet());

        if (types.contains(NULL)) {
            return convertUnion(Schema.createUnion(schema.getTypes().stream()
                    .filter(type -> type.getType() != NULL)
                    .collect(toImmutableList())));
        }
        if (schema.getTypes().size() == 1) {
            return convert(getOnlyElement(schema.getTypes()));
        }
        if (INTEGRAL_TYPES.containsAll(types)) {
            return Optional.of(BigintType.BIGINT);
        }
        if (DECIMAL_TYPES.containsAll(types)) {
            return Optional.of(DoubleType.DOUBLE);
        }
        if (STRING_TYPES.containsAll(types)) {
            return Optional.of(VarcharType.VARCHAR);
        }
        if (BINARY_TYPES.containsAll(types)) {
            return Optional.of(VarbinaryType.VARBINARY);
        }
        throw new UnsupportedOperationException(format("Incompatible UNION type: '%s'", schema.toString(true)));
    }

    private Optional<Type> convertArray(Schema schema)
    {
        checkArgument(schema.getType() == ARRAY, "schema is not an ARRAY");
        return convert(schema.getElementType()).map(ArrayType::new);
    }

    private Optional<Type> convertMap(Schema schema)
    {
        checkArgument(schema.getType() == MAP, "schema is not a MAP");
        return convert(schema.getValueType()).map(this::createMapType);
    }

    private Type createMapType(Type valueType)
    {
        Type keyType = VARCHAR;
        return new MapType(keyType, valueType, typeManager.getTypeOperators());
    }

    private Optional<Type> convertRecord(Schema schema)
    {
        checkArgument(schema.getType() == RECORD, "schema is not a RECORD");
        List<RowType.Field> fields = schema.getFields().stream()
                .map(field -> convert(field.schema()).map(type -> new RowType.Field(Optional.ofNullable(field.name()), type)))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableList());
        if (fields.isEmpty()) {
            switch (emptyFieldStrategy) {
                case IGNORE:
                    return Optional.empty();
                case MARK:
                    return Optional.of(DUMMY_ROW_TYPE);
                case FAIL:
                    throw new IllegalStateException(format("Struct type has no valid fields for schema: '%s'", schema));
            }
            throw new IllegalStateException(format("Unknown emptyFieldStrategy '%s'", emptyFieldStrategy));
        }
        return Optional.of(RowType.from(fields));
    }
}
