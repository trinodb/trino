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
package io.trino.operator.scalar;

import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.type.ArrayType;
import io.trino.sql.gen.ColumnarProjectionAssertions;
import io.trino.sql.gen.ColumnarProjectionAssertions.Input;
import io.trino.sql.ir.Reference;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.spi.function.SelectedPositions.positionsList;
import static io.trino.spi.function.SelectedPositions.positionsRange;
import static io.trino.spi.type.BigintType.BIGINT;

public class TestArrayFlattenFunction
{
    private static final ArrayType BIGINT_ARRAY = new ArrayType(BIGINT);
    private static final ArrayType NESTED_ARRAY = new ArrayType(BIGINT_ARRAY);
    private static final ColumnarProjectionAssertions COLUMNAR = new ColumnarProjectionAssertions(new TestingFunctionResolution());
    private static final int POSITION_COUNT = 40;

    @Test
    public void testFlattenColumnarRegion()
    {
        // The region starts and ends inside the nested elements, and covers empty, null, and partially null arrays
        Block arrays = nestedArrayBlock(Arrays.asList(
                List.of(List.of(1L)),
                List.of(),
                null,
                Arrays.asList(List.of(), null, List.of(2L, 3L)),
                List.of(List.of(4L), List.of(5L)),
                List.of(List.of(6L))))
                .getRegion(1, 4);

        assertBlockEquals(
                BIGINT_ARRAY,
                ArrayFlattenFunction.flattenColumnar(positionsRange(0, 4), arrays),
                arrayBlock(Arrays.asList(List.of(), null, List.of(2L, 3L), List.of(4L, 5L))));
        assertBlockEquals(
                BIGINT_ARRAY,
                ArrayFlattenFunction.flattenColumnar(positionsList(new int[] {0, 2, 3}, 0, 3), arrays),
                arrayBlock(Arrays.asList(List.of(), List.of(2L, 3L), List.of(4L, 5L))));
        assertBlockEquals(
                BIGINT_ARRAY,
                ArrayFlattenFunction.flattenColumnar(positionsRange(1, 2), arrays),
                arrayBlock(Arrays.asList(null, List.of(2L, 3L))));

        // many selected elements are flattened over every array, and few selected elements are gathered
        assertBlockEquals(
                BIGINT_ARRAY,
                ArrayFlattenFunction.flattenColumnar(positionsList(new int[] {0, 1, 2, 3}, 0, 4), arrays),
                arrayBlock(Arrays.asList(List.of(), null, List.of(2L, 3L), List.of(4L, 5L))));
        assertBlockEquals(
                BIGINT_ARRAY,
                ArrayFlattenFunction.flattenColumnar(positionsList(new int[] {1, 2}, 0, 2), arrays),
                arrayBlock(Arrays.asList(null, List.of(2L, 3L))));
        assertBlockEquals(
                BIGINT_ARRAY,
                ArrayFlattenFunction.flattenColumnar(positionsList(new int[] {0, 1}, 0, 2), arrays),
                arrayBlock(Arrays.asList(List.of(), null)));
    }

    private static Block nestedArrayBlock(List<List<List<Long>>> values)
    {
        ArrayBlockBuilder builder = NESTED_ARRAY.createBlockBuilder(null, values.size());
        for (List<List<Long>> value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                builder.buildEntry(innerArrays -> {
                    for (List<Long> innerArray : value) {
                        writeArray(innerArrays, innerArray);
                    }
                });
            }
        }
        return builder.build();
    }

    private static Block arrayBlock(List<List<Long>> values)
    {
        BlockBuilder builder = BIGINT_ARRAY.createBlockBuilder(null, values.size());
        for (List<Long> value : values) {
            writeArray(builder, value);
        }
        return builder.build();
    }

    private static void writeArray(BlockBuilder builder, List<Long> value)
    {
        if (value == null) {
            builder.appendNull();
            return;
        }
        ((ArrayBlockBuilder) builder).buildEntry(elements -> {
            for (Long element : value) {
                if (element == null) {
                    elements.appendNull();
                }
                else {
                    BIGINT.writeLong(elements, element);
                }
            }
        });
    }

    @Test
    public void testColumnarEquivalence()
    {
        Block arrays = nestedArrayBlock(IntStream.range(0, POSITION_COUNT)
                .mapToObj(TestArrayFlattenFunction::nestedArrayValue)
                .toList());
        COLUMNAR.assertEquivalent(
                COLUMNAR.function("flatten", new Reference(NESTED_ARRAY, "input")),
                new Input("input", NESTED_ARRAY, arrays));

        // the inner arrays are dictionary encoded
        ArrayBlock arrayBlock = (ArrayBlock) arrays;
        Block innerArrays = arrayBlock.getRawElementBlock();
        int[] ids = new int[innerArrays.getPositionCount()];
        Arrays.setAll(ids, id -> ids.length - 1 - id);
        Block encodedArrays = ArrayBlock.fromElementBlock(
                POSITION_COUNT,
                Optional.ofNullable(arrayBlock.getRawValueIsValid()),
                arrayBlock.getRawOffsets(),
                DictionaryBlock.create(ids.length, innerArrays, ids));
        COLUMNAR.assertEquivalent(
                COLUMNAR.function("flatten", new Reference(NESTED_ARRAY, "input")),
                new Input("input", NESTED_ARRAY, encodedArrays));
    }

    private static List<List<Long>> nestedArrayValue(int position)
    {
        // every seventh value is null, and the others have one to three inner arrays of up to two elements, some of them null
        if (position % 7 == 3) {
            return null;
        }
        List<List<Long>> innerArrays = new ArrayList<>();
        for (int array = 0; array < (position % 3) + 1; array++) {
            if ((position + array) % 5 == 4) {
                innerArrays.add(null);
                continue;
            }
            List<Long> elements = new ArrayList<>();
            for (int element = 0; element < (position + array) % 3; element++) {
                elements.add((position + array + element) % 6 == 5 ? null : position * 10L + array * 2L + element);
            }
            innerArrays.add(elements);
        }
        return innerArrays;
    }
}
