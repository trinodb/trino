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
package io.trino.plugin.weaviate;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.Type;
import io.weaviate.client6.v1.api.collections.GeoCoordinates;
import io.weaviate.client6.v1.api.collections.PhoneNumber;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

import static io.trino.plugin.weaviate.Decoder.decode;
import static io.trino.plugin.weaviate.TestWeaviate.NOW;
import static io.trino.plugin.weaviate.WeaviateColumnHandle.BOOL_ARRAY;
import static io.trino.plugin.weaviate.WeaviateColumnHandle.DATE_ARRAY;
import static io.trino.plugin.weaviate.WeaviateColumnHandle.GEO_COORDINATES;
import static io.trino.plugin.weaviate.WeaviateColumnHandle.INT_ARRAY;
import static io.trino.plugin.weaviate.WeaviateColumnHandle.MULTI_VECTOR;
import static io.trino.plugin.weaviate.WeaviateColumnHandle.NUMBER_ARRAY;
import static io.trino.plugin.weaviate.WeaviateColumnHandle.PHONE_NUMBER;
import static io.trino.plugin.weaviate.WeaviateColumnHandle.SINGLE_VECTOR;
import static io.trino.plugin.weaviate.WeaviateColumnHandle.TEXT_ARRAY;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class TestDecoder
{
    @ParameterizedTest
    @MethodSource("testCases")
    public void testDecoder(Type type, Object raw, Object expected)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, 1);
        decode(blockBuilder, raw, type);
        Block block = blockBuilder.build();

        switch (type) {
            case ArrayType arrayType -> checkArrayValues(arrayType.getObject(block, 0), type, expected, type.getDisplayName());
            case RowType rowType -> checkRowValues(rowType.getObject(block, 0), type, expected);
            default -> checkPrimitiveValue(type.getObjectValue(block, 0), expected, type.getDisplayName());
        }
    }

    public static Object[][] testCases()
    {
        return new Object[][] {
                // Primitive values
                {VARCHAR, "text-value", "text-value"},
                {BOOLEAN, true, true},
                {INTEGER, 1, 1},
                {INTEGER, 2L, 2L},
                {DOUBLE, 3D, 3D},
                {TIMESTAMP_MILLIS, NOW, NOW},

                // Row values
                {
                        GEO_COORDINATES,
                        new GeoCoordinates(123f, 456f),
                        Map.of("latitude", 123d, "longitude", 456d)
                },
                {
                        PHONE_NUMBER,
                        new PhoneNumber(
                                "01 546 48",
                                "AT",
                                43,
                                "+43 1 546 48",
                                1_546_48,
                                "01 546 48",
                                true),
                        Map.of(
                                "defaultCountry", "AT",
                                "countryCode", 43,
                                "internationalFormatted", "+43 1 546 48",
                                "national", 1_546_48,
                                "nationalFormatted", "01 546 48",
                                "valid", true)
                },
                {
                        RowType.rowType(
                                RowType.field("foo", VARCHAR),
                                RowType.field("bar", RowType.rowType(
                                        RowType.field("baz", BOOLEAN)))),
                        Map.of(
                                "foo", "foo-value",
                                "bar", Map.of("baz", false)),
                        Map.of(
                                "foo", "foo-value",
                                "bar", Map.of("baz", false)),
                },

                // Array values
                {
                        TEXT_ARRAY,
                        List.of("a", "b", "c"),
                        List.of("a", "b", "c"),
                },
                {
                        BOOL_ARRAY,
                        List.of(true, true, false),
                        List.of(true, true, false),
                },
                {
                        INT_ARRAY,
                        List.of(1, 2, 3),
                        List.of(1L, 2L, 3L),
                },
                {
                        INT_ARRAY,
                        List.of(1L, 2L, 3L),
                        List.of(1L, 2L, 3L),
                },
                {
                        NUMBER_ARRAY,
                        List.of(1f, 2f, 3f),
                        List.of(1d, 2d, 3d),
                },
                {
                        NUMBER_ARRAY,
                        List.of(1d, 2d, 3d),
                        List.of(1d, 2d, 3d),
                },
                {
                        DATE_ARRAY,
                        List.of(NOW, NOW),
                        List.of(NOW, NOW),
                },
                {
                        SINGLE_VECTOR,
                        List.of(1f, 2f, 3f),
                        List.of(1d, 2d, 3d),
                },
                {
                        MULTI_VECTOR,
                        List.of(List.of(1f, 2f, 3f), List.of(4f, 5f, 6f)),
                        List.of(List.of(1d, 2d, 3d), List.of(4d, 5d, 6d)),
                },
        };
    }

    private static void checkRowValues(SqlRow actualRow, Type type, Object expected)
    {
        Assertions.assertThat(type)
                .describedAs("Type is null")
                .isNotNull();
        Assertions.assertThat(type instanceof RowType)
                .describedAs("Unexpected type")
                .isTrue();
        Assertions.assertThat(actualRow)
                .describedAs("sqlRow is null")
                .isNotNull();
        Assertions.assertThat(expected)
                .describedAs("Value is null")
                .isNotNull();

        RowType rowType = (RowType) type;
        Map<String, Object> row = (Map<String, Object>) expected;

        Assertions.assertThat(actualRow.getFieldCount())
                .describedAs("Trino type field size mismatch")
                .isEqualTo(rowType.getFields().size());

        List<RowType.Field> fields = rowType.getFields();
        int rawIndex = actualRow.getRawIndex();
        for (int i = 0; i < fields.size(); i++) {
            RowType.Field field = fields.get(i);
            String fieldName = field.getName().orElseThrow();
            Object expectedValue = row.get(fieldName);
            Block actualBlock = actualRow.getRawFieldBlock(i);
            if (expectedValue == null) {
                Assertions.assertThat(actualBlock.isNull(rawIndex))
                        .describedAs("expect %s is null", fieldName)
                        .isTrue();
            }
            else {
                checkField(actualBlock, field.getType(), rawIndex, expectedValue, fieldName);
            }
        }
    }

    private static void checkArrayValues(Block actualBlock, Type type, Object expected, String fieldName)
    {
        Assertions.assertThat(type)
                .describedAs("Type is null")
                .isNotNull();
        Assertions.assertThat(type instanceof ArrayType)
                .describedAs("Unexpected type")
                .isTrue();
        Assertions.assertThat(actualBlock)
                .describedAs("Block is null")
                .isNotNull();
        Assertions.assertThat(expected)
                .describedAs("Value is null")
                .isNotNull();

        List<?> list = (List<?>) expected;

        Assertions.assertThat(actualBlock.getPositionCount())
                .describedAs("arrays are equal in size")
                .isEqualTo(list.size());
        Type elementType = ((ArrayType) type).getElementType();
        for (int index = 0; index < actualBlock.getPositionCount(); index++) {
            if (actualBlock.isNull(index)) {
                Assertions.assertThat(list.get(index)).isNull();
                continue;
            }
            checkField(actualBlock, elementType, index, list.get(index), "%s[%d]".formatted(fieldName, index));
        }
    }

    private static void checkField(Block actualBlock, Type type, int position, Object expectedValue, String fieldName)
    {
        Assertions.assertThat(type)
                .describedAs("Type is null")
                .isNotNull();
        Assertions.assertThat(actualBlock)
                .describedAs("actualBlock is null")
                .isNotNull();
        Assertions.assertThat(actualBlock.isNull(position))
                .describedAs("actual %s is null", fieldName)
                .isFalse();
        Assertions.assertThat(expectedValue)
                .describedAs("expectedValue is null")
                .isNotNull();

        switch (type) {
            case ArrayType arrayType -> checkArrayValues(arrayType.getObject(actualBlock, position), type, expectedValue, fieldName);
            case RowType rowType -> checkRowValues(rowType.getObject(actualBlock, position), type, expectedValue);
            default -> checkPrimitiveValue(type.getObjectValue(actualBlock, position), expectedValue, fieldName);
        }
    }

    private static void checkPrimitiveValue(Object actual, Object expected, String fieldName)
    {
        if (actual == null || expected == null) {
            Assertions.assertThat(expected).describedAs(fieldName).isNull();
            Assertions.assertThat(actual).describedAs(fieldName).isNull();
        }
        else if (actual instanceof CharSequence) {
            Assertions.assertThat(actual.toString()).describedAs(fieldName)
                    .isEqualTo(expected.toString());
        }
        else if (isIntegralType(actual) && isIntegralType(expected)) {
            Assertions.assertThat(((Number) actual).longValue())
                    .describedAs(fieldName)
                    .isEqualTo(((Number) expected).longValue());
        }
        else if (isRealType(actual) && isRealType(expected)) {
            Assertions.assertThat(((Number) actual).doubleValue())
                    .describedAs(fieldName)
                    .isEqualTo(((Number) expected).doubleValue());
        }
        else if (actual instanceof SqlTimestamp ts) {
            Assertions.assertThat(ts.getEpochMicros())
                    .describedAs("%s (SqlTimestamp epoch micros)", fieldName)
                    .isEqualTo(((OffsetDateTime) expected).toInstant().toEpochMilli() * MICROSECONDS_PER_MILLISECOND);
        }
        else {
            Assertions.assertThat(actual)
                    .describedAs(fieldName)
                    .isEqualTo(expected);
        }
    }

    private static boolean isIntegralType(Object value)
    {
        return value instanceof Long
                || value instanceof Integer
                || value instanceof Short
                || value instanceof Byte;
    }

    private static boolean isRealType(Object value)
    {
        return value instanceof Float || value instanceof Double;
    }
}
