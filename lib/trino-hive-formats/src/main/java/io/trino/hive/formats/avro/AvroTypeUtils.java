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

import org.apache.avro.Schema;

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static java.util.function.Predicate.not;

public final class AvroTypeUtils
{
    private AvroTypeUtils() {}

    public static void verifyNoCircularReferences(Schema schema)
            throws AvroTypeException
    {
        verifyNoCircularReferences(schema, new HashSet<>());
    }

    private static void verifyNoCircularReferences(Schema schema, Set<Schema> enclosingRecords)
            throws AvroTypeException
    {
        switch (schema.getType()) {
            case NULL, BOOLEAN, INT, LONG, FLOAT, DOUBLE, ENUM, STRING, FIXED, BYTES -> {} //no-op
            case ARRAY -> verifyNoCircularReferences(schema.getElementType(), enclosingRecords);
            case MAP -> verifyNoCircularReferences(schema.getValueType(), enclosingRecords);
            case RECORD -> {
                if (!enclosingRecords.add(schema)) {
                    throw new AvroTypeException("Recursive Avro Schema not supported: " + schema.getFullName());
                }
                for (Schema.Field field : schema.getFields()) {
                    verifyNoCircularReferences(field.schema(), new HashSet<>(enclosingRecords));
                }
            }
            case UNION -> {
                for (Schema schemaOption : schema.getTypes()) {
                    verifyNoCircularReferences(schemaOption, enclosingRecords);
                }
            }
        }
    }

    public static boolean isSimpleNullableUnion(Schema schema)
    {
        verify(schema.isUnion(), "Schema must be union");
        return schema.getTypes().stream().filter(not(Schema::isNullable)).count() == 1L;
    }

    static Schema unwrapNullableUnion(Schema schema)
    {
        verify(schema.isUnion(), "Schema must be union");
        verify(schema.isNullable() && schema.getTypes().size() == 2);
        return schema.getTypes().stream().filter(not(Schema::isNullable)).collect(onlyElement());
    }

    public static SimpleUnionNullIndex getSimpleNullableUnionNullIndex(Schema schema)
    {
        verify(schema.isUnion(), "Schema must be union");
        verify(schema.isNullable() && schema.getTypes().size() == 2, "Invalid null union: %s", schema);
        return schema.getTypes().get(0).getType() == Schema.Type.NULL ? SimpleUnionNullIndex.ZERO : SimpleUnionNullIndex.ONE;
    }

    enum SimpleUnionNullIndex
    {
        ZERO(0),
        ONE(1);
        private final int index;

        SimpleUnionNullIndex(int index)
        {
            this.index = index;
        }

        public int getIndex()
        {
            return index;
        }
    }

    static Schema lowerCaseAllFieldsForWriter(Schema schema)
    {
        return switch (schema.getType()) {
            case RECORD ->
                Schema.createRecord(
                        schema.getName(),
                        schema.getDoc(),
                        schema.getNamespace(),
                        schema.isError(),
                        schema.getFields().stream()
                                .map(field -> new Schema.Field(
                                        field.name().toLowerCase(Locale.ENGLISH),
                                        lowerCaseAllFieldsForWriter(field.schema()),
                                        field.doc()))// Can ignore field default because only used on read path and opens the opportunity for invalid default errors
                                .collect(toImmutableList()));

            case ARRAY -> Schema.createArray(lowerCaseAllFieldsForWriter(schema.getElementType()));
            case MAP -> Schema.createMap(lowerCaseAllFieldsForWriter(schema.getValueType()));
            case UNION -> Schema.createUnion(schema.getTypes().stream().map(AvroTypeUtils::lowerCaseAllFieldsForWriter).collect(toImmutableList()));
            case NULL, BOOLEAN, INT, LONG, FLOAT, DOUBLE, STRING, BYTES, FIXED, ENUM -> schema;
        };
    }
}
