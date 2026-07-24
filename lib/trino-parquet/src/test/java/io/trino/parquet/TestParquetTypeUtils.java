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
package io.trino.parquet;

import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;

import static io.trino.parquet.ParquetTypeUtils.constructField;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.RowType.rowType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestParquetTypeUtils
{
    /**
     * Regression for <a href="https://github.com/trinodb/trino/issues/27766">#27766</a>:
     * nested LIST-annotated groups where the middle repeated group is named
     * {@code array}, whose single child is itself a LIST group, used to be
     * misresolved and blow up with {@code ClassCastException}.
     */
    @Test
    public void testNestedLegacyArrayList()
    {
        MessageType schema = MessageTypeParser.parseMessageType(
                """
                message schema {
                  optional group nested_list (LIST) {
                    repeated group array {
                      optional group inner_list (LIST) {
                        repeated group array {
                          optional binary element (STRING);
                        }
                      }
                    }
                  }
                }
                """);
        Type trinoType = new ArrayType(new ArrayType(VARCHAR));

        Field field = constructField(trinoType, columnIO(schema)).orElseThrow();

        assertNestedArrayOfPrimitive(field, trinoType, VARCHAR);
    }

    @Test
    public void testStandardThreeLevelNestedList()
    {
        // Modern spec-compliant 3-level shape at both levels — must keep working.
        MessageType schema = MessageTypeParser.parseMessageType(
                """
                message schema {
                  optional group outer (LIST) {
                    repeated group list {
                      optional group element (LIST) {
                        repeated group list {
                          optional binary element (STRING);
                        }
                      }
                    }
                  }
                }
                """);
        Type trinoType = new ArrayType(new ArrayType(VARCHAR));

        Field field = constructField(trinoType, columnIO(schema)).orElseThrow();

        assertNestedArrayOfPrimitive(field, trinoType, VARCHAR);
    }

    @Test
    public void testLegacyTwoLevelArrayOfInt()
    {
        MessageType schema = MessageTypeParser.parseMessageType(
                """
                message schema {
                  optional group values (LIST) {
                    repeated int32 element;
                  }
                }
                """);
        Type trinoType = new ArrayType(INTEGER);

        GroupField outer = (GroupField) constructField(trinoType, columnIO(schema)).orElseThrow();

        assertThat(outer.getType()).isEqualTo(trinoType);
        PrimitiveField element = (PrimitiveField) outer.getChildren().get(0).orElseThrow();
        assertThat(element.getType()).isEqualTo(INTEGER);
    }

    @Test
    public void testLegacyMultiFieldStructArray()
    {
        // Spec rule 2: repeated group with >1 field IS the element (a struct).
        MessageType schema = MessageTypeParser.parseMessageType(
                """
                message schema {
                  optional group pairs (LIST) {
                    repeated group array {
                      required int32 a;
                      required int32 b;
                    }
                  }
                }
                """);
        Type trinoType = new ArrayType(rowType(field("a", INTEGER), field("b", INTEGER)));

        GroupField outer = (GroupField) constructField(trinoType, columnIO(schema)).orElseThrow();

        assertThat(outer.getType()).isEqualTo(trinoType);
        GroupField row = (GroupField) outer.getChildren().get(0).orElseThrow();
        assertThat(row.getType()).isInstanceOf(RowType.class);
        assertThat(row.getChildren()).hasSize(2);
        assertThat(((PrimitiveField) row.getChildren().get(0).orElseThrow()).getType()).isEqualTo(INTEGER);
        assertThat(((PrimitiveField) row.getChildren().get(1).orElseThrow()).getType()).isEqualTo(INTEGER);
    }

    /**
     * Regression for the {@code SingleLevelArraySchemaConverter} shape: nested
     * arrays are encoded with the middle repeated group carrying its own
     * {@code LIST} annotation. The repeated group IS the element.
     */
    @Test
    public void testSingleLevelSchemaNestedArrays()
    {
        MessageType schema = MessageTypeParser.parseMessageType(
                """
                message schema {
                  optional group test (LIST) {
                    repeated group array (LIST) {
                      repeated int32 array;
                    }
                  }
                }
                """);
        Type trinoType = new ArrayType(new ArrayType(INTEGER));

        GroupField outer = (GroupField) constructField(trinoType, columnIO(schema)).orElseThrow();

        assertThat(outer.getType()).isEqualTo(trinoType);
        GroupField inner = (GroupField) outer.getChildren().get(0).orElseThrow();
        assertThat(inner.getType()).isInstanceOf(ArrayType.class);
        PrimitiveField leaf = (PrimitiveField) inner.getChildren().get(0).orElseThrow();
        assertThat(leaf.getType()).isEqualTo(INTEGER);
    }

    @Test
    public void testSingleLevelSchemaDeeplyNestedArrays()
    {
        // Matches the shape from AbstractTestParquetReader.testSingleLevelSchemaNestedArrays
        // when nestingLevel produces 5 middle LIST-annotated repeated groups terminating
        // in a repeated primitive.
        MessageType schema = MessageTypeParser.parseMessageType(
                """
                message schema {
                  optional group test (LIST) {
                    repeated group array (LIST) {
                      repeated group array (LIST) {
                        repeated group array (LIST) {
                          repeated group array (LIST) {
                            repeated int32 array;
                          }
                        }
                      }
                    }
                  }
                }
                """);
        Type trinoType = new ArrayType(new ArrayType(new ArrayType(new ArrayType(new ArrayType(INTEGER)))));

        Field field = constructField(trinoType, columnIO(schema)).orElseThrow();

        // Walk down 5 array levels ending in a primitive integer.
        Field current = field;
        for (int level = 0; level < 5; level++) {
            GroupField group = (GroupField) current;
            assertThat(group.getType()).isInstanceOf(ArrayType.class);
            current = group.getChildren().get(0).orElseThrow();
        }
        assertThat(((PrimitiveField) current).getType()).isEqualTo(INTEGER);
    }

    /**
     * Regression for the {@code SingleLevelArraySchemaConverter} shape when the
     * element is a MAP: the middle repeated group carries a MAP annotation.
     * The repeated group IS the element.
     */
    @Test
    public void testSingleLevelSchemaArrayOfMap()
    {
        MessageType schema = MessageTypeParser.parseMessageType(
                """
                message schema {
                  optional group test (LIST) {
                    repeated group array (MAP) {
                      repeated group map (MAP_KEY_VALUE) {
                        required binary key (STRING);
                        optional int32 value;
                      }
                    }
                  }
                }
                """);
        MapType mapType = new MapType(VARCHAR, INTEGER, new TypeOperators());
        Type trinoType = new ArrayType(mapType);

        GroupField outer = (GroupField) constructField(trinoType, columnIO(schema)).orElseThrow();

        assertThat(outer.getType()).isEqualTo(trinoType);
        GroupField mapField = (GroupField) outer.getChildren().get(0).orElseThrow();
        assertThat(mapField.getType()).isInstanceOf(MapType.class);
    }

    @Test
    public void testLegacySingleFieldStructArray()
    {
        // Spec rule 3: repeated group with a single field, named "array", where the
        // caller expects a row(...) — the repeated group IS the element.
        MessageType schema = MessageTypeParser.parseMessageType(
                """
                message schema {
                  optional group boxes (LIST) {
                    repeated group array {
                      required int32 f;
                    }
                  }
                }
                """);
        Type trinoType = new ArrayType(rowType(field("f", INTEGER)));

        GroupField outer = (GroupField) constructField(trinoType, columnIO(schema)).orElseThrow();

        assertThat(outer.getType()).isEqualTo(trinoType);
        GroupField row = (GroupField) outer.getChildren().get(0).orElseThrow();
        assertThat(row.getType()).isInstanceOf(RowType.class);
        assertThat(row.getChildren()).hasSize(1);
        assertThat(((PrimitiveField) row.getChildren().get(0).orElseThrow()).getType()).isEqualTo(INTEGER);
    }

    private static void assertNestedArrayOfPrimitive(Field field, Type expectedType, Type expectedLeafType)
    {
        GroupField outer = (GroupField) field;
        assertThat(outer.getType()).isEqualTo(expectedType);
        assertThat(outer.getChildren()).hasSize(1);

        GroupField inner = (GroupField) outer.getChildren().get(0).orElseThrow();
        assertThat(inner.getType()).isInstanceOf(ArrayType.class);
        assertThat(inner.getChildren()).hasSize(1);

        PrimitiveField leaf = (PrimitiveField) inner.getChildren().get(0).orElseThrow();
        assertThat(leaf.getType()).isEqualTo(expectedLeafType);
    }

    private static ColumnIO columnIO(MessageType schema)
    {
        MessageColumnIO messageColumnIO = new ColumnIOFactory().getColumnIO(schema, schema, true);
        return messageColumnIO.getChild(0);
    }
}
