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
package io.trino.plugin.hudi;

import com.google.common.collect.ImmutableList;
import io.trino.metastore.HiveType;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.io.UncheckedIOException;
import java.util.List;

import static io.trino.plugin.hudi.HudiUtil.constructSchema;
import static io.trino.plugin.hudi.testing.TypeInfoHelper.INT_TYPE_INFO;
import static io.trino.plugin.hudi.testing.TypeInfoHelper.LONG_TYPE_INFO;
import static io.trino.plugin.hudi.testing.TypeInfoHelper.STRING_TYPE_INFO;
import static io.trino.plugin.hudi.testing.TypeInfoHelper.listHiveType;
import static io.trino.plugin.hudi.testing.TypeInfoHelper.mapHiveType;
import static io.trino.plugin.hudi.testing.TypeInfoHelper.structHiveType;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HudiUtilTest
{
    @Test
    void testConstructSchemaWithSimpleTypes()
    {
        List<String> columnNames = List.of("id", "price", "product_name", "is_available");
        List<HiveType> columnTypes = List.of(
                HiveType.HIVE_LONG,
                HiveType.HIVE_DOUBLE,
                HiveType.HIVE_STRING,
                HiveType.HIVE_BOOLEAN);

        Schema schema = constructSchema(columnNames, columnTypes);

        assertThat(schema.getName()).isEqualTo("baseRecord");
        assertThat(schema.getFields()).hasSize(4);

        // Verify that each field is correctly typed and is a UNION with NULL
        assertField(schema, "id", Schema.Type.LONG);
        assertField(schema, "price", Schema.Type.DOUBLE);
        assertField(schema, "product_name", Schema.Type.STRING);
        assertField(schema, "is_available", Schema.Type.BOOLEAN);
    }

    @Test
    void testConstructSchemaWithComplexTypes()
    {
        List<String> columnNames = List.of("tags", "ratings", "metadata");

        // Create complex Hive types
        HiveType tagsType = listHiveType(STRING_TYPE_INFO);
        HiveType ratingsType = mapHiveType(STRING_TYPE_INFO, INT_TYPE_INFO);
        HiveType metadataType = structHiveType(
                ImmutableList.of("author", "version"),
                ImmutableList.of(STRING_TYPE_INFO, LONG_TYPE_INFO));

        List<HiveType> columnTypes = List.of(tagsType, ratingsType, metadataType);

        Schema schema = constructSchema(columnNames, columnTypes);

        assertThat(schema.getName()).isEqualTo("baseRecord");
        assertThat(schema.getFields()).hasSize(3);

        // Verify array field
        Schema tagsFieldSchema = assertAndGetNonNullSchema(schema.getField("tags"));
        assertThat(tagsFieldSchema.getType()).isEqualTo(Schema.Type.ARRAY);
        assertNonNullUnion(tagsFieldSchema.getElementType(), Schema.Type.STRING);

        // Verify map field
        Schema ratingsFieldSchema = assertAndGetNonNullSchema(schema.getField("ratings"));
        assertThat(ratingsFieldSchema.getType()).isEqualTo(Schema.Type.MAP);
        assertNonNullUnion(ratingsFieldSchema.getValueType(), Schema.Type.INT);

        // Verify struct (record) field
        Schema metadataFieldSchema = assertAndGetNonNullSchema(schema.getField("metadata"));
        assertThat(metadataFieldSchema.getType()).isEqualTo(Schema.Type.RECORD);
        assertThat(metadataFieldSchema.getFields()).hasSize(2);
        assertField(metadataFieldSchema, "author", Schema.Type.STRING);
        assertField(metadataFieldSchema, "version", Schema.Type.LONG);
    }

    @Test
    void testConstructSchemaWithEmptyColumns()
    {
        assertThatThrownBy(() -> constructSchema(emptyList(), emptyList()))
                .isInstanceOf(UncheckedIOException.class)
                .hasMessageContaining("Failed to construct Avro schema");
    }

    @Test
    void testConstructSchemaWithMismatchedColumns()
    {
        List<String> columnNames = List.of("col1", "col2");
        List<HiveType> columnTypes = List.of(HiveType.HIVE_INT); // Only one type

        assertThatThrownBy(() -> constructSchema(columnNames, columnTypes))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Number of column name and column type differs");
    }

    /**
     * Helper method to assert a field exists and is a UNION of [NULL, expectedType].
     */
    private void assertField(Schema recordSchema, String fieldName, Schema.Type expectedType)
    {
        Schema fieldSchema = assertAndGetNonNullSchema(recordSchema.getField(fieldName));
        assertThat(fieldSchema.getType()).isEqualTo(expectedType);
    }

    /**
     * Helper method to assert that a field's schema is a UNION of [NULL, other_type]
     * and returns the non-null schema part for further assertions.
     */
    private Schema assertAndGetNonNullSchema(Schema.Field field)
    {
        assertThat(field).isNotNull();
        return assertNonNullUnion(field.schema(), null);
    }

    /**
     * Asserts a schema is a UNION of [NULL, other_type] and returns the non-null part.
     * Optionally checks the type of the non-null part.
     */
    private Schema assertNonNullUnion(Schema unionSchema, Schema.Type expectedType)
    {
        assertThat(unionSchema.isUnion()).isTrue();
        assertThat(unionSchema.getTypes()).hasSize(2);
        // The Trino utility always places NULL first in the union
        assertThat(unionSchema.getTypes().get(0).getType()).isEqualTo(Schema.Type.NULL);

        Schema nonNullSchema = unionSchema.getTypes().get(1);
        if (expectedType != null) {
            assertThat(nonNullSchema.getType()).isEqualTo(expectedType);
        }
        return nonNullSchema;
    }
}
