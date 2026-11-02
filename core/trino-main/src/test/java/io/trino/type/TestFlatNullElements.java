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
package io.trino.type;

import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.type.BlockTypeOperators.BlockPositionIsIdentical;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandle;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FLAT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.VALUE_BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.BLOCK_BUILDER;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FLAT_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.util.StructuralTestUtil.appendToBlockBuilder;
import static io.trino.util.StructuralTestUtil.mapType;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that values containing null elements round trip through the flat representation
 * when the backing arrays are not zeroed, including when a slot previously held another value.
 */
public class TestFlatNullElements
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();
    private static final BlockTypeOperators BLOCK_TYPE_OPERATORS = new BlockTypeOperators(TYPE_OPERATORS);
    private static final String LONG_STRING = "a string value longer than the inline flat size";

    @Test
    public void testArray()
            throws Throwable
    {
        assertFlatRoundTrip(
                new ArrayType(BIGINT),
                List.of(),
                Arrays.asList(1L, null, 3L),
                Arrays.asList((Object) null),
                List.of(4L, 5L, 6L));
        assertFlatRoundTrip(
                new ArrayType(VARCHAR),
                Arrays.asList("a", null, LONG_STRING),
                Arrays.asList(null, null),
                List.of(LONG_STRING, "b"));
    }

    @Test
    public void testMap()
            throws Throwable
    {
        assertFlatRoundTrip(
                mapType(BIGINT, VARCHAR),
                orderedMap(1L, null, 2L, "x"),
                orderedMap(3L, LONG_STRING),
                orderedMap(4L, null));
    }

    @Test
    public void testRow()
            throws Throwable
    {
        assertFlatRoundTrip(
                RowType.from(List.of(field("a", BIGINT), field("b", VARCHAR))),
                Arrays.asList(null, "x"),
                Arrays.asList(1L, null),
                Arrays.asList(null, null),
                List.of(2L, LONG_STRING));
    }

    @Test
    public void testNested()
            throws Throwable
    {
        assertFlatRoundTrip(
                new ArrayType(new ArrayType(BIGINT)),
                Arrays.asList(Arrays.asList(1L, null), null),
                List.of(List.of(2L, 3L)),
                Arrays.asList(null, Arrays.asList((Object) null)));
        assertFlatRoundTrip(
                new ArrayType(RowType.from(List.of(field("a", BIGINT), field("b", VARCHAR)))),
                Arrays.asList(Arrays.asList(null, "x"), null),
                List.of(List.of(1L, LONG_STRING)));
        assertFlatRoundTrip(
                mapType(VARCHAR, new ArrayType(BIGINT)),
                orderedMap("a", Arrays.asList(null, 1L), "b", null),
                orderedMap(LONG_STRING, List.of(2L)));
    }

    private static void assertFlatRoundTrip(Type type, Object... values)
            throws Throwable
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, values.length);
        for (Object value : values) {
            appendToBlockBuilder(type, value, blockBuilder);
        }
        ValueBlock block = blockBuilder.buildValueBlock();

        MethodHandle writeFlat = TYPE_OPERATORS.getReadValueOperator(type, simpleConvention(FLAT_RETURN, VALUE_BLOCK_POSITION));
        MethodHandle readFlat = TYPE_OPERATORS.getReadValueOperator(type, simpleConvention(BLOCK_BUILDER, FLAT));
        BlockPositionIsIdentical identical = BLOCK_TYPE_OPERATORS.getIdenticalOperator(type);

        int maxVariableWidthSize = 0;
        for (int position = 0; position < block.getPositionCount(); position++) {
            maxVariableWidthSize = Math.max(maxVariableWidthSize, type.getFlatVariableWidthSize(block, position));
        }
        byte[] fixed = new byte[type.getFlatFixedSize()];
        byte[] variable = new byte[maxVariableWidthSize];

        // write each value over every other value to simulate reuse of the same slot
        for (int previous = 0; previous < block.getPositionCount(); previous++) {
            for (int current = 0; current < block.getPositionCount(); current++) {
                Arrays.fill(fixed, (byte) 0xFF);
                Arrays.fill(variable, (byte) 0xFF);
                writeFlat.invokeExact(block, previous, fixed, 0, variable, 0);
                writeFlat.invokeExact(block, current, fixed, 0, variable, 0);

                BlockBuilder resultBuilder = type.createBlockBuilder(null, 1);
                readFlat.invokeExact(fixed, 0, variable, 0, resultBuilder);
                ValueBlock result = resultBuilder.buildValueBlock();

                assertThat(identical.isIdentical(result, 0, block, current))
                        .describedAs("%s value at position %s written over position %s", type, current, previous)
                        .isTrue();
            }
        }
    }

    private static Map<Object, Object> orderedMap(Object... keysAndValues)
    {
        Map<Object, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            map.put(keysAndValues[i], keysAndValues[i + 1]);
        }
        return map;
    }
}
