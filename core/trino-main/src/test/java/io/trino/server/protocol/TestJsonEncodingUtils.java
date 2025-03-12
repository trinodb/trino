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
package io.trino.server.protocol;

import com.google.common.collect.ImmutableList;
import io.trino.client.CloseableIterator;
import io.trino.client.Column;
import io.trino.client.QueryDataDecoder;
import io.trino.client.Row;
import io.trino.client.spooling.DataAttributes;
import io.trino.client.spooling.encoding.JsonQueryDataDecoder;
import io.trino.server.protocol.spooling.QueryDataEncoder;
import io.trino.server.protocol.spooling.encoding.JsonQueryDataEncoder;
import io.trino.spi.Page;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.block.BlockAssertions.createBlockOfReals;
import static io.trino.block.BlockAssertions.createBooleansBlock;
import static io.trino.block.BlockAssertions.createCharsBlock;
import static io.trino.block.BlockAssertions.createDoublesBlock;
import static io.trino.block.BlockAssertions.createIntsBlock;
import static io.trino.block.BlockAssertions.createSmallintsBlock;
import static io.trino.block.BlockAssertions.createStringsBlock;
import static io.trino.block.BlockAssertions.createTinyintsBlock;
import static io.trino.block.BlockAssertions.createTypedLongsBlock;
import static io.trino.server.protocol.ProtocolUtil.createColumn;
import static io.trino.server.protocol.TestJsonEncodingUtils.TypedColumn.typed;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestJsonEncodingUtils
{
    protected QueryDataDecoder createDecoder(List<Column> columns)
    {
        return new JsonQueryDataDecoder.Factory().create(columns, DataAttributes.empty());
    }

    protected QueryDataEncoder createEncoder(List<OutputColumn> columns)
    {
        return new JsonQueryDataEncoder.Factory().create(TEST_SESSION, columns);
    }

    @Test
    public void testInvalidJson()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("col0", BIGINT));

        assertInvalidJson(columns, "invalid", "Unrecognized token 'invalid'");
        assertInvalidJson(columns, "", "Expected start of an array, but got null");
        assertInvalidJson(columns, "[[]", "Unexpected token END_ARRAY");
        assertInvalidJson(columns, "[[", "Unexpected end-of-input");
        assertInvalidJson(columns, "[[5", "Unexpected end-of-input");
        assertInvalidJson(columns, "[[5]", "Unexpected end-of-input");
        assertInvalidJson(columns, "[[5],]", "Unexpected character (']' (code 93))");
        assertInvalidJson(columns, "[[5][]", "Unexpected character ('[' (code 91))");

        assertThat(parseJson(columns, "[[5]]")).isEqualTo(List.of(List.of(5L)));
    }

    @Test
    public void testBigintSerialization()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("col0", BIGINT));
        Page page = page(createTypedLongsBlock(BIGINT, 1L, 2L, 3L, 4L));

        assertThat(roundTrip(columns, page, "[[1],[2],[3],[4]]"))
                .isEqualTo(column(1L, 2L, 3L, 4L));
    }

    @Test
    public void testBigintArraySerialization()
            throws IOException
    {
        ArrayType arrayType = new ArrayType(BIGINT);
        List<TypedColumn> columns = ImmutableList.of(typed("col0", arrayType));
        ArrayBlockBuilder blockBuilder = arrayType.createBlockBuilder(null, 10);

        blockBuilder.buildEntry(builder -> {
            for (int i = 0; i < 10; i++) {
                BIGINT.writeLong(builder, i);
            }
        });

        Page page = page(blockBuilder.build());
        assertThat(roundTrip(columns, page, "[[[0,1,2,3,4,5,6,7,8,9]]]"))
                .isEqualTo(column(List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L)));
    }

    @Test
    public void testIntegerSerialization()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("col0", INTEGER));
        Page page = page(createIntsBlock(1, 2, 3, 4));

        assertThat(roundTrip(columns, page, "[[1],[2],[3],[4]]"))
                .isEqualTo(column(1, 2, 3, 4));
    }

    @Test
    public void testIntegerArraySerialization()
            throws IOException
    {
        ArrayType arrayType = new ArrayType(INTEGER);
        List<TypedColumn> columns = ImmutableList.of(typed("col0", arrayType));
        ArrayBlockBuilder blockBuilder = arrayType.createBlockBuilder(null, 10);

        blockBuilder.buildEntry(builder -> {
            for (int i = 0; i < 10; i++) {
                INTEGER.writeLong(builder, i);
            }
        });

        Page page = page(blockBuilder.build());
        assertThat(roundTrip(columns, page, "[[[0,1,2,3,4,5,6,7,8,9]]]"))
                .isEqualTo(column(List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)));
    }

    @Test
    public void testTinyintSerialization()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("col0", TINYINT));
        Page page = page(createTinyintsBlock(1, 2, 3, 4));

        assertThat(roundTrip(columns, page, "[[1],[2],[3],[4]]"))
                .isEqualTo(column((byte) 1, (byte) 2, (byte) 3, (byte) 4));
    }

    @Test
    public void testTinyintArraySerialization()
            throws IOException
    {
        ArrayType arrayType = new ArrayType(TINYINT);
        List<TypedColumn> columns = ImmutableList.of(typed("col0", arrayType));
        ArrayBlockBuilder blockBuilder = arrayType.createBlockBuilder(null, 5);

        blockBuilder.buildEntry(builder -> {
            for (int i = 0; i < 5; i++) {
                TINYINT.writeLong(builder, i);
            }
            builder.appendNull();
        });

        Page page = page(blockBuilder.build());
        assertThat(roundTrip(columns, page, "[[[0,1,2,3,4,null]]]"))
                .isEqualTo(column(array((byte) 0, (byte) 1, (byte) 2, (byte) 3, (byte) 4, null)));
    }

    @Test
    public void testSmallintSerialization()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("col0", SMALLINT));
        Page page = page(createSmallintsBlock(1, 2, 3, 4));

        assertThat(roundTrip(columns, page, "[[1],[2],[3],[4]]"))
                .isEqualTo(column((short) 1, (short) 2, (short) 3, (short) 4));
    }

    @Test
    public void testSmallintArraySerialization()
            throws IOException
    {
        ArrayType arrayType = new ArrayType(SMALLINT);
        List<TypedColumn> columns = ImmutableList.of(typed("col0", arrayType));
        ArrayBlockBuilder blockBuilder = arrayType.createBlockBuilder(null, 5);

        blockBuilder.buildEntry(builder -> {
            for (int i = 0; i < 5; i++) {
                SMALLINT.writeLong(builder, i);
            }
            builder.appendNull();
        });

        Page page = page(blockBuilder.build());
        assertThat(roundTrip(columns, page, "[[[0,1,2,3,4,null]]]"))
                .isEqualTo(column(array((short) 0, (short) 1, (short) 2, (short) 3, (short) 4, null)));
    }

    @Test
    public void testDoubleSerialization()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("col0", DOUBLE));
        Page page = page(createDoublesBlock(1.0d, 2.11d, 3.11d, 4.13d, Double.NaN));

        assertThat(roundTrip(columns, page, "[[1.0],[2.11],[3.11],[4.13],[\"NaN\"]]"))
                .isEqualTo(column(1.0d, 2.11d, 3.11d, 4.13d, Double.NaN));
    }

    @Test
    public void testDoubleArraySerialization()
            throws IOException
    {
        ArrayType arrayType = new ArrayType(DOUBLE);
        List<TypedColumn> columns = ImmutableList.of(typed("col0", arrayType));
        ArrayBlockBuilder blockBuilder = arrayType.createBlockBuilder(null, 5);

        blockBuilder.buildEntry(builder -> {
            for (int i = 0; i < 5; i++) {
                DOUBLE.writeDouble(builder, i);
            }
            builder.appendNull();
        });

        Page page = page(blockBuilder.build());
        assertThat(roundTrip(columns, page, "[[[0.0,1.0,2.0,3.0,4.0,null]]]"))
                .isEqualTo(column(array(0.0d, 1.0d, 2.0d, 3.0d, 4.0d, null)));
    }

    @Test
    public void testRealSerialization()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("col0", REAL));
        Page page = page(createBlockOfReals(1.0f, 2.11f, 3.11f, 4.13f, Float.NaN));

        assertThat(roundTrip(columns, page, "[[1.0],[2.11],[3.11],[4.13],[\"NaN\"]]"))
                .isEqualTo(column(1.0f, 2.11f, 3.11f, 4.13f, Float.NaN));
    }

    @Test
    public void testRealArraySerialization()
            throws IOException
    {
        ArrayType arrayType = new ArrayType(REAL);
        List<TypedColumn> columns = ImmutableList.of(typed("col0", arrayType));
        ArrayBlockBuilder blockBuilder = arrayType.createBlockBuilder(null, 7);

        blockBuilder.buildEntry(builder -> {
            for (int i = 0; i < 5; i++) {
                REAL.writeFloat(builder, i);
            }
            builder.appendNull();
            REAL.writeFloat(builder, Float.NaN);
        });

        Page page = page(blockBuilder.build());
        assertThat(roundTrip(columns, page, "[[[0.0,1.0,2.0,3.0,4.0,null,\"NaN\"]]]").getFirst())
                .containsExactly(array(0.0f, 1.0f, 2.0f, 3.0f, 4.0f, null, Float.NaN));
    }

    @Test
    public void testVarcharSerialization()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("col0", VARCHAR));
        Page page = page(createStringsBlock("ala", "ma", "kota", "a", "kot", "ma", "ale", "", null));

        assertThat(roundTrip(columns, page, "[[\"ala\"],[\"ma\"],[\"kota\"],[\"a\"],[\"kot\"],[\"ma\"],[\"ale\"],[\"\"],[null]]"))
                .isEqualTo(column("ala", "ma", "kota", "a", "kot", "ma", "ale", "", null));
    }

    @Test
    public void testVarcharUtf8Serialization()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("col0", VARCHAR));
        Page page = page(createStringsBlock(
                "数据应用",
                "\"quoted\"",
                "zażółć gęślą jaźń",
                "\0\0\0",  // garbage in, garbage out
                "\r\t\n",
                "\uD83E\uDD83"));

        assertThat(roundTrip(columns, page, "[[\"数据应用\"],[\"\\\"quoted\\\"\"],[\"zażółć gęślą jaźń\"],[\"\\u0000\\u0000\\u0000\"],[\"\\r\\t\\n\"],[\"🦃\"]]"))
                .isEqualTo(column("数据应用", "\"quoted\"", "zażółć gęślą jaźń", "\0\0\0", "\r\t\n", "🦃"));
    }

    @Test
    public void testVarcharArraySerialization()
            throws IOException
    {
        ArrayType arrayType = new ArrayType(VARCHAR);
        List<TypedColumn> columns = ImmutableList.of(typed("col0", arrayType));
        ArrayBlockBuilder blockBuilder = arrayType.createBlockBuilder(null, 6);

        blockBuilder.buildEntry(builder -> {
            for (int i = 0; i < 5; i++) {
                VARCHAR.writeSlice(builder, utf8Slice("kot" + i));
            }
            builder.appendNull();
        });

        Page page = page(blockBuilder.build());
        assertThat(roundTrip(columns, page, "[[[\"kot0\",\"kot1\",\"kot2\",\"kot3\",\"kot4\",null]]]").getFirst())
                .containsExactly(array("kot0", "kot1", "kot2", "kot3", "kot4", null));
    }

    @Test
    public void testCharSerialization()
            throws IOException
    {
        CharType charType = CharType.createCharType(5);
        List<TypedColumn> columns = ImmutableList.of(typed("col0", charType));
        Page page = page(createCharsBlock(charType, List.of(
                "ala",
                "ma",
                "kota")));

        assertThat(roundTrip(columns, page, "[[\"ala  \"],[\"ma   \"],[\"kota \"]]"))
                .isEqualTo(column(
                        "ala  ",
                        "ma   ",
                        "kota "));
    }

    @Test
    public void testCharArraySerialization()
            throws IOException
    {
        CharType charType = CharType.createCharType(5);
        ArrayType arrayType = new ArrayType(charType);
        List<TypedColumn> columns = ImmutableList.of(typed("col0", arrayType));
        ArrayBlockBuilder blockBuilder = arrayType.createBlockBuilder(null, 6);

        blockBuilder.buildEntry(builder -> {
            for (int i = 0; i < 5; i++) {
                VARCHAR.writeSlice(builder, utf8Slice("kot" + i));
            }
            builder.appendNull();
        });

        Page page = page(blockBuilder.build());
        assertThat(roundTrip(columns, page, "[[[\"kot0 \",\"kot1 \",\"kot2 \",\"kot3 \",\"kot4 \",null]]]").getFirst())
                .containsExactly(array("kot0 ", "kot1 ", "kot2 ", "kot3 ", "kot4 ", null));
    }

    @Test
    public void testBooleanSerialization()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("col0", BOOLEAN));
        Page page = page(createBooleansBlock(true, true, true, false, false, true));

        assertThat(roundTrip(columns, page, "[[true],[true],[true],[false],[false],[true]]"))
                .isEqualTo(column(true, true, true, false, false, true));
    }

    @Test
    public void testBooleanArraySerialization()
            throws IOException
    {
        ArrayType arrayType = new ArrayType(BOOLEAN);
        List<TypedColumn> columns = ImmutableList.of(typed("col0", arrayType));
        ArrayBlockBuilder blockBuilder = arrayType.createBlockBuilder(null, 6);

        blockBuilder.buildEntry(builder -> {
            for (int i = 0; i < 5; i++) {
                BOOLEAN.writeBoolean(builder, i % 2 == 0);
            }
            builder.appendNull();
        });

        Page page = page(blockBuilder.build());
        assertThat(roundTrip(columns, page, "[[[true,false,true,false,true,null]]]").getFirst())
                .containsExactly(array(true, false, true, false, true, null));
    }

    @Test
    public void testMapSerialization()
            throws IOException
    {
        MapType mapType = new MapType(REAL, DOUBLE, new TypeOperators());
        List<TypedColumn> columns = ImmutableList.of(typed("col0", mapType));
        MapBlockBuilder blockBuilder = mapType.createBlockBuilder(null, 6);

        blockBuilder.buildEntry((keyBuilder, valueBuilder) -> {
            REAL.writeFloat(keyBuilder, 0.0f);
            DOUBLE.writeDouble(valueBuilder, 0.0d);

            REAL.writeFloat(keyBuilder, 1.0f);
            DOUBLE.writeDouble(valueBuilder, 1.0d);

            REAL.writeFloat(keyBuilder, 2.0f);
            DOUBLE.writeDouble(valueBuilder, 2.0d);

            REAL.writeFloat(keyBuilder, 3.0f);
            DOUBLE.writeDouble(valueBuilder, 3.0d);

            REAL.writeFloat(keyBuilder, 4.0f);
            DOUBLE.writeDouble(valueBuilder, 4.0d);

            REAL.writeFloat(keyBuilder, 5.0f);
            valueBuilder.appendNull();
        });

        Page page = page(blockBuilder.build());
        assertThat(roundTrip(columns, page, "[[{\"0.0\":0.0,\"1.0\":1.0,\"2.0\":2.0,\"3.0\":3.0,\"4.0\":4.0,\"5.0\":null}]]").getFirst())
                .containsExactly(map(
                        entry(0.0f, 0.0d),
                        entry(1.0f, 1.0d),
                        entry(2.0f, 2.0d),
                        entry(3.0f, 3.0d),
                        entry(4.0f, 4.0d),
                        entry(5.0f, null)));
    }

    @Test
    public void testMapOfVarbinaryKeysSerialization()
            throws IOException
    {
        MapType mapType = new MapType(VARBINARY, DOUBLE, new TypeOperators());
        List<TypedColumn> columns = ImmutableList.of(typed("col0", mapType));
        MapBlockBuilder blockBuilder = mapType.createBlockBuilder(null, 6);

        blockBuilder.buildEntry((keyBuilder, valueBuilder) -> {
            VARBINARY.writeSlice(keyBuilder, utf8Slice("value"));
            DOUBLE.writeDouble(valueBuilder, 0.0d);

            VARBINARY.writeSlice(keyBuilder, utf8Slice("value2"));
            valueBuilder.appendNull();
        });

        Page page = page(blockBuilder.build());
        List<Object> values = roundTrip(columns, page, "[[{\"dmFsdWU=\":0.0,\"dmFsdWUy\":null}]]").getFirst();
        assertThat(values.getFirst()).isInstanceOf(Map.class);
        Map<Object, Object> valuesMap = (Map<Object, Object>) values.getFirst();

        assertThat(valuesMap.keySet().stream()
                .map(bytes -> new String((byte[]) bytes, UTF_8))
                .collect(toImmutableSet()))
                .containsExactlyInAnyOrder("value", "value2");
    }

    @Test
    public void testRowSerialization()
            throws IOException
    {
        RowType rowType = RowType.rowType(
                RowType.field("a", BIGINT),
                RowType.field("b", VARCHAR),
                RowType.field("c", BOOLEAN));

        List<TypedColumn> columns = ImmutableList.of(typed("col0", rowType));
        RowBlockBuilder blockBuilder = rowType.createBlockBuilder(null, 2);

        blockBuilder.buildEntry(builders -> {
            BIGINT.writeLong(builders.get(0), 1);
            VARCHAR.writeSlice(builders.get(1), utf8Slice("ala"));
            BOOLEAN.writeBoolean(builders.get(2), true);
        });

        blockBuilder.buildEntry(builders -> {
            builders.get(0).appendNull();
            builders.get(1).appendNull();
            builders.get(2).appendNull();
        });

        Page page = page(blockBuilder.build());
        assertThat(roundTrip(columns, page, "[[[1,\"ala\",true]],[[null,null,null]]]"))
                .containsExactly(
                        List.of(Row.builderWithExpectedSize(3)
                                .addField("a", 1L)
                                .addField("b", "ala")
                                .addField("c", true)
                                .build()),
                        List.of(Row.builderWithExpectedSize(3)
                                .addField("a", null)
                                .addField("b", null)
                                .addField("c", null)
                                .build()));
    }

    protected List<List<Object>> roundTrip(List<TypedColumn> columns, Page page, String expectedJson)
            throws IOException
    {
        QueryDataEncoder encoder = newEncoder(columns);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        encoder.encodeTo(output, List.of(page));

        assertThat(output.toString(UTF_8)).isEqualTo(expectedJson);

        return ImmutableList.copyOf(parseJson(columns, output.toByteArray()));
    }

    protected void assertInvalidJson(List<TypedColumn> columns, String json, String expectedError)
    {
        assertThatThrownBy(() -> parseJson(columns, json))
                .hasMessageContaining(expectedError);
    }

    protected List<List<Object>> parseJson(List<TypedColumn> columns, String json)
            throws IOException
    {
        return parseJson(columns, json.getBytes(UTF_8));
    }

    protected List<List<Object>> parseJson(List<TypedColumn> columns, byte[] json)
            throws IOException
    {
        QueryDataDecoder decoder = newDecoder(columns);
        try (CloseableIterator<List<Object>> iterator = decoder.decode(new ByteArrayInputStream(json), null)) {
            return ImmutableList.copyOf(iterator);
        }
    }

    record TypedColumn(String name, Type type)
    {
        public TypedColumn
        {
            requireNonNull(name, "name is null");
            requireNonNull(type, "type is null");
        }

        public static TypedColumn typed(String name, Type type)
        {
            return new TypedColumn(name, type);
        }
    }

    private QueryDataEncoder newEncoder(List<TypedColumn> types)
    {
        ImmutableList.Builder<OutputColumn> columns = ImmutableList.builderWithExpectedSize(types.size());
        for (int i = 0; i < types.size(); i++) {
            TypedColumn typedColumn = types.get(i);
            columns.add(new OutputColumn(i, typedColumn.name(), typedColumn.type()));
        }
        return createEncoder(columns.build());
    }

    private QueryDataDecoder newDecoder(List<TypedColumn> types)
    {
        ImmutableList.Builder<Column> columns = ImmutableList.builderWithExpectedSize(types.size());
        for (TypedColumn typedColumn : types) {
            columns.add(createColumn(typedColumn.name(), typedColumn.type(), true));
        }
        return createDecoder(columns.build());
    }

    private static Page page(Block... blocks)
    {
        return new Page(blocks);
    }

    private static <T> List<List<T>> column(T... values)
    {
        return Arrays.stream(values)
                // Allow nulls in values
                .map(value -> {
                    List<T> list = new ArrayList<>();
                    list.add(value);
                    return list;
                })
                .collect(toList());
    }

    private static <T> List<T> array(T... values)
    {
        return Arrays.asList(values);
    }

    private static <K, V> Map<K, V> map(Entry<K, V>... entries)
    {
        Map<K, V> values = new HashMap<>();
        for (Entry<K, V> entry : entries) {
            values.put(entry.key(), entry.value());
        }
        return values;
    }

    record Entry<K, V>(K key, V value)
    {
        // Allow nulls
    }

    static <K, V> Entry<K, V> entry(K key, V value)
    {
        return new Entry<>(key, value);
    }
}
