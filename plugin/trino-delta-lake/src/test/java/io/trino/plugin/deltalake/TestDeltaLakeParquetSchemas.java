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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static io.trino.plugin.deltalake.DeltaLakeParquetSchemas.createParquetSchemaMapping;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.testng.Assert.assertEquals;

public class TestDeltaLakeParquetSchemas
{
    private final TypeManager typeManager = new TestingTypeManager();

    @Test
    public void testStringFieldColumnMappingNoneUnpartitioned()
    {
        @Language("JSON")
        String jsonSchema = """
                {
                    "type": "struct",
                    "fields": [
                        {
                            "name": "a_string",
                            "type": "string",
                            "nullable": true,
                            "metadata": {}
                        }
                    ]
                }
                """;
        DeltaLakeSchemaSupport.ColumnMappingMode columnMappingMode = DeltaLakeSchemaSupport.ColumnMappingMode.NONE;
        List<String> partitionColumnNames = ImmutableList.of();
        org.apache.parquet.schema.Type expectedMessageType = Types.buildMessage()
                .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, OPTIONAL)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("a_string"))
                .named("trino_schema");
        Map<List<String>, Type> expectedPrimitiveTypes = ImmutableMap.<List<String>, Type>builder()
                .put(List.of("a_string"), VARCHAR)
                .buildOrThrow();

        assertParquetSchemaMappingCreationAccuracy(jsonSchema, columnMappingMode, partitionColumnNames, expectedMessageType, expectedPrimitiveTypes);
    }

    @Test
    public void testStringFieldColumnMappingNonePartitioned()
    {
        @Language("JSON")
        String jsonSchema = """
                {
                     "type": "struct",
                     "fields": [
                         {
                             "name": "a_string",
                             "type": "string",
                             "nullable": true,
                             "metadata": {}
                         },
                         {
                             "name": "part",
                             "type": "string",
                             "nullable": true,
                             "metadata": {}
                         }
                     ]
                 }
                """;
        DeltaLakeSchemaSupport.ColumnMappingMode columnMappingMode = DeltaLakeSchemaSupport.ColumnMappingMode.NONE;
        List<String> partitionColumnNames = ImmutableList.of("part");
        org.apache.parquet.schema.Type expectedMessageType = Types.buildMessage()
                .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, OPTIONAL)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("a_string"))
                .named("trino_schema");
        Map<List<String>, Type> expectedPrimitiveTypes = ImmutableMap.<List<String>, Type>builder()
                .put(List.of("a_string"), VARCHAR)
                .buildOrThrow();

        assertParquetSchemaMappingCreationAccuracy(jsonSchema, columnMappingMode, partitionColumnNames, expectedMessageType, expectedPrimitiveTypes);
    }

    @Test
    public void testStringFieldColumnMappingIdUnpartitioned()
    {
        @Language("JSON")
        String jsonSchema = """
                {
                    "type": "struct",
                    "fields": [
                        {
                            "name": "a_string",
                            "type": "string",
                            "nullable": true,
                            "metadata": {
                                "delta.columnMapping.id": 1,
                                "delta.columnMapping.physicalName": "col-eafe32e6-bd93-47f7-8921-34b7a4e66a06"
                            }
                        }
                    ]
                }
                """;
        org.apache.parquet.schema.Type expectedMessageType = Types.buildMessage()
                .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, OPTIONAL)
                        .as(LogicalTypeAnnotation.stringType())
                        .id(1)
                        .named("col-eafe32e6-bd93-47f7-8921-34b7a4e66a06"))
                .named("trino_schema");
        Map<List<String>, Type> expectedPrimitiveTypes = ImmutableMap.<List<String>, Type>builder()
                .put(List.of("col-eafe32e6-bd93-47f7-8921-34b7a4e66a06"), VARCHAR)
                .buildOrThrow();

        assertParquetSchemaMappingCreationAccuracy(jsonSchema, DeltaLakeSchemaSupport.ColumnMappingMode.ID, ImmutableList.of(), expectedMessageType, expectedPrimitiveTypes);
    }

    @Test
    public void testStringFieldColumnMappingIdPartitioned()
    {
        @Language("JSON")
        String jsonSchema = """
                {
                    "type": "struct",
                    "fields": [
                        {
                            "name": "a_string",
                            "type": "string",
                            "nullable": true,
                            "metadata": {
                                "delta.columnMapping.id": 1,
                                "delta.columnMapping.physicalName": "col-40feefa6-d999-4c90-a923-190ecea9191c"
                            }
                        },
                        {
                            "name": "part",
                            "type": "string",
                            "nullable": true,
                            "metadata": {
                                "delta.columnMapping.id": 2,
                                "delta.columnMapping.physicalName": "col-77789070-4b77-44b4-adf2-32d5df94f9e7"
                            }
                        }
                    ]
                }
                """;
        org.apache.parquet.schema.Type expectedMessageType = Types.buildMessage()
                .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, OPTIONAL)
                        .as(LogicalTypeAnnotation.stringType())
                        .id(1)
                        .named("col-40feefa6-d999-4c90-a923-190ecea9191c"))
                .named("trino_schema");
        Map<List<String>, Type> expectedPrimitiveTypes = ImmutableMap.<List<String>, Type>builder()
                .put(List.of("col-40feefa6-d999-4c90-a923-190ecea9191c"), VARCHAR)
                .buildOrThrow();

        assertParquetSchemaMappingCreationAccuracy(jsonSchema, DeltaLakeSchemaSupport.ColumnMappingMode.ID, ImmutableList.of("part"), expectedMessageType, expectedPrimitiveTypes);
    }

    @Test
    public void testStringFieldColumnMappingNameUnpartitioned()
    {
        @Language("JSON")
        String jsonSchema = """
                {
                    "type": "struct",
                    "fields": [
                        {
                            "name": "a_string",
                            "type": "string",
                            "nullable": true,
                            "metadata": {
                                "delta.columnMapping.id": 1,
                                "delta.columnMapping.physicalName": "col-0200c2be-bb8d-4be8-b724-674d71074143"
                            }
                        }
                    ]
                }
                """;
        org.apache.parquet.schema.Type expectedMessageType = Types.buildMessage()
                .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, OPTIONAL)
                        .as(LogicalTypeAnnotation.stringType())
                        .id(1)
                        .named("col-0200c2be-bb8d-4be8-b724-674d71074143"))
                .named("trino_schema");
        Map<List<String>, Type> expectedPrimitiveTypes = ImmutableMap.<List<String>, Type>builder()
                .put(List.of("col-0200c2be-bb8d-4be8-b724-674d71074143"), VARCHAR)
                .buildOrThrow();

        assertParquetSchemaMappingCreationAccuracy(jsonSchema, DeltaLakeSchemaSupport.ColumnMappingMode.ID, ImmutableList.of(), expectedMessageType, expectedPrimitiveTypes);
    }

    @Test
    public void testRowFieldColumnMappingNameUnpartitioned()
    {
        // Corresponds to Databricks Delta type `a_complex_struct STRUCT<nested_struct: STRUCT<a_string: STRING>, a_string_array ARRAY<STRING>, a_complex_map MAP<STRING, STRUCT<a_string: STRING>>>`
        @Language("JSON")
        String jsonSchema = """
                {
                    "type": "struct",
                    "fields": [
                        {
                            "name": "a_complex_struct",
                            "type": {
                                "type": "struct",
                                "fields": [
                                    {
                                        "name": "nested_struct",
                                        "type": {
                                            "type": "struct",
                                            "fields": [
                                                {
                                                    "name": "a_string",
                                                    "type": "string",
                                                    "nullable": true,
                                                    "metadata": {
                                                        "delta.columnMapping.id": 3,
                                                        "delta.columnMapping.physicalName": "col-1830cfed-bdd2-43c4-98c8-f2685cff6faf"
                                                    }
                                                }
                                            ]
                                        },
                                        "nullable": true,
                                        "metadata": {
                                            "delta.columnMapping.id": 2,
                                            "delta.columnMapping.physicalName": "col-5e0e4060-8e54-427b-8a8d-72a4fd6b67bd"
                                        }
                                    },
                                    {
                                        "name": "a_string_array",
                                        "type": {
                                            "type": "array",
                                            "elementType": "string",
                                            "containsNull": true
                                        },
                                        "nullable": true,
                                        "metadata": {
                                            "delta.columnMapping.id": 4,
                                            "delta.columnMapping.physicalName": "col-ff99c229-b1ce-4971-bfbc-3a68fec3dfea"
                                        }
                                    },
                                    {
                                        "name": "a_complex_map",
                                        "type": {
                                            "type": "map",
                                            "keyType": "string",
                                            "valueType": {
                                                "type": "struct",
                                                "fields": [
                                                    {
                                                        "name": "a_string",
                                                        "type": "string",
                                                        "nullable": true,
                                                        "metadata": {
                                                            "delta.columnMapping.id": 6,
                                                            "delta.columnMapping.physicalName": "col-5cb932a5-69aa-47e6-9d75-40f87bd8a239"
                                                        }
                                                    }
                                                ]
                                            },
                                            "valueContainsNull": true
                                        },
                                        "nullable": true,
                                        "metadata": {
                                            "delta.columnMapping.id": 5,
                                            "delta.columnMapping.physicalName": "col-85dededd-8dd2-4a81-ab3c-1439c1fd895a"
                                        }
                                    }
                                ]
                            },
                            "nullable": true,
                            "metadata": {
                                "delta.columnMapping.id": 1,
                                "delta.columnMapping.physicalName": "col-306694c6-846e-4c72-a3ea-976e4b19160a"
                            }
                        }
                    ]
                }
                """;
        org.apache.parquet.schema.Type expectedMessageType = Types.buildMessage()
                .addField(Types.buildGroup(OPTIONAL)
                        .addField(Types.buildGroup(OPTIONAL)
                                .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, OPTIONAL)
                                        .as(LogicalTypeAnnotation.stringType())
                                        .id(3)
                                        .named("col-1830cfed-bdd2-43c4-98c8-f2685cff6faf"))
                                .id(2)
                                .named("col-5e0e4060-8e54-427b-8a8d-72a4fd6b67bd"))
                        .addField(Types.optionalList()
                                .element(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, OPTIONAL)
                                        .as(LogicalTypeAnnotation.stringType())
                                        .named("element"))
                                .id(4)
                                .named("col-ff99c229-b1ce-4971-bfbc-3a68fec3dfea"))
                        .addField(Types.map(OPTIONAL)
                                .key(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, REQUIRED)
                                        .as(LogicalTypeAnnotation.stringType())
                                        .named("key"))
                                .value(Types.buildGroup(OPTIONAL)
                                        .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, OPTIONAL)
                                                .as(LogicalTypeAnnotation.stringType())
                                                .id(6)
                                                .named("col-5cb932a5-69aa-47e6-9d75-40f87bd8a239"))
                                        .named("value"))
                                .id(5)
                                .named("col-85dededd-8dd2-4a81-ab3c-1439c1fd895a"))
                        .id(1)
                        .named("col-306694c6-846e-4c72-a3ea-976e4b19160a"))
                .named("trino_schema");
        Map<List<String>, Type> expectedPrimitiveTypes = ImmutableMap.<List<String>, Type>builder()
                .put(List.of("col-306694c6-846e-4c72-a3ea-976e4b19160a", "col-5e0e4060-8e54-427b-8a8d-72a4fd6b67bd", "col-1830cfed-bdd2-43c4-98c8-f2685cff6faf"), VARCHAR)
                .put(List.of("col-306694c6-846e-4c72-a3ea-976e4b19160a", "col-ff99c229-b1ce-4971-bfbc-3a68fec3dfea", "list", "element"), VARCHAR)
                .put(List.of("col-306694c6-846e-4c72-a3ea-976e4b19160a", "col-85dededd-8dd2-4a81-ab3c-1439c1fd895a", "key_value", "key"), VARCHAR)
                .put(List.of("col-306694c6-846e-4c72-a3ea-976e4b19160a", "col-85dededd-8dd2-4a81-ab3c-1439c1fd895a", "key_value", "value", "col-5cb932a5-69aa-47e6-9d75-40f87bd8a239"), VARCHAR)
                .buildOrThrow();

        assertParquetSchemaMappingCreationAccuracy(
                jsonSchema, DeltaLakeSchemaSupport.ColumnMappingMode.ID, ImmutableList.of(), expectedMessageType, expectedPrimitiveTypes);
    }

    private void assertParquetSchemaMappingCreationAccuracy(
            @Language("JSON") String jsonSchema,
            DeltaLakeSchemaSupport.ColumnMappingMode columnMappingMode,
            List<String> partitionColumnNames,
            org.apache.parquet.schema.Type expectedMessageType,
            Map<List<String>, Type> expectedPrimitiveTypes)
    {
        DeltaLakeParquetSchemaMapping parquetSchemaMapping = createParquetSchemaMapping(jsonSchema, typeManager, columnMappingMode, partitionColumnNames);
        assertEquals(parquetSchemaMapping.messageType(), expectedMessageType);
        assertEquals(parquetSchemaMapping.primitiveTypes(), expectedPrimitiveTypes);
    }
}
