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

import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.IntStream;

import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.block.BlockAssertions.createStringSequenceBlock;
import static io.trino.block.BlockAssertions.createStringsBlock;
import static io.trino.operator.scalar.ArrayElementBlockProjection.Selection.INDEX;
import static io.trino.spi.function.SelectedPositions.positionsList;
import static io.trino.spi.function.SelectedPositions.positionsRange;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Collections.nCopies;
import static org.assertj.core.api.Assertions.assertThat;

public class TestArrayElementBlockProjection
{
    @Test
    public void testSyntheticNullRowHasNullFields()
    {
        // Six single element arrays of rows, evaluated over a region that starts and ends inside the elements
        Block rows = RowBlock.fromFieldBlocks(6, new Block[] {createLongSequenceBlock(0, 6)});
        Block arrays = ArrayBlock.fromElementBlock(6, Optional.empty(), new int[] {0, 1, 2, 3, 4, 5, 6}, rows).getRegion(1, 4);

        Block result = ArrayElementBlockProjection.project(positionsList(new int[] {0, 1, 3}, 0, 3), arrays, createLongsBlock(1L, 5L, 1L, 1L), INDEX);

        assertThat(result.getPositionCount()).isEqualTo(3);
        Block field = RowBlock.getRowFieldsFromBlock(result).getFirst();
        assertThat(BIGINT.getLong(field, 0)).isEqualTo(1);
        assertThat(result.isNull(1)).isTrue();
        assertThat(field.isNull(1)).isTrue();
        assertThat(BIGINT.getLong(field, 2)).isEqualTo(4);
    }

    @Test
    public void testSyntheticNullWithWideElementRange()
    {
        Block arrays = singleElementArrays(40);
        Block indexes = createLongsBlock(nCopies(40, 1L));
        assertBlockEquals(
                BIGINT,
                ArrayElementBlockProjection.project(positionsList(new int[] {0, 20, 39}, 0, 3), arrays, indexes, INDEX),
                createLongsBlock(0L, 20L, 39L));

        // the selected elements are far apart, so they are copied instead of appending a null to their whole range
        assertBlockEquals(
                BIGINT,
                ArrayElementBlockProjection.project(positionsList(new int[] {0, 20, 39}, 0, 3), arrays, replace(indexes, 20, 5L), INDEX),
                createLongsBlock(0L, null, 39L));
    }

    @Test
    public void testSyntheticNullWithWideVariableWidthRange()
    {
        // Appending a null to variable width elements copies only their offsets, so a wider range of them is kept
        Block arrays = singleElementArrays(createStringSequenceBlock(0, 200));
        Block indexes = createLongsBlock(nCopies(200, 1L));
        Block result = ArrayElementBlockProjection.project(positionsList(new int[] {0, 50, 99}, 0, 3), arrays, replace(indexes, 50, 5L), INDEX);
        assertBlockEquals(VARCHAR, result, createStringsBlock("0", null, "99"));
        assertThat(((DictionaryBlock) result).getDictionary().getPositionCount()).isEqualTo(101);

        // the selected elements are far apart, so they are copied instead of appending a null to their whole range
        result = ArrayElementBlockProjection.project(positionsList(new int[] {0, 100, 199}, 0, 3), arrays, replace(indexes, 100, 5L), INDEX);
        assertBlockEquals(VARCHAR, result, createStringsBlock("0", null, "199"));
        assertThat(((DictionaryBlock) result).getDictionary().getPositionCount()).isEqualTo(3);
    }

    @Test
    public void testMissingElementsReuseNullElement()
    {
        // The elements contain a null, so the missing elements point at it instead of a copy of the elements
        Block arrays = singleElementArrays(createLongsBlock(0L, null, 2L, 3L));
        Block result = ArrayElementBlockProjection.project(positionsRange(0, 4), arrays, createLongsBlock(1L, 1L, 5L, 1L), INDEX);
        assertBlockEquals(BIGINT, result, createLongsBlock(0L, null, null, 3L));
        assertThat(((DictionaryBlock) result).getDictionary().getPositionCount()).isEqualTo(4);
    }

    @Test
    public void testSyntheticNullWithoutSelectedElements()
    {
        Block arrays = singleElementArrays(4);
        Block result = ArrayElementBlockProjection.project(positionsRange(1, 2), arrays, createLongsBlock(5L, 5L, 5L, 5L), INDEX);
        assertBlockEquals(BIGINT, result, createLongsBlock((Long) null, null));
    }

    @Test
    public void testDictionaryArrays()
    {
        // The arrays are read through the dictionary, while the indexes are read at the positions of the page
        Block dictionary = singleElementArrays(4);
        Block arrays = DictionaryBlock.create(5, dictionary, new int[] {3, 0, 3, 1, 2});
        Block indexes = createLongsBlock(1L, 1L, 2L, 1L, 1L);
        assertBlockEquals(
                BIGINT,
                ArrayElementBlockProjection.project(positionsList(new int[] {0, 2, 3}, 0, 3), arrays, indexes, INDEX),
                createLongsBlock(3L, null, 1L));
        assertBlockEquals(
                BIGINT,
                ArrayElementBlockProjection.project(positionsRange(0, 5), RunLengthEncodedBlock.create(dictionary.getSingleValueBlock(2), 5), indexes, INDEX),
                createLongsBlock(2L, 2L, null, 2L, 2L));
    }

    private static Block singleElementArrays(int count)
    {
        return singleElementArrays(createLongSequenceBlock(0, count));
    }

    private static Block singleElementArrays(Block elements)
    {
        return ArrayBlock.fromElementBlock(elements.getPositionCount(), Optional.empty(), IntStream.rangeClosed(0, elements.getPositionCount()).toArray(), elements);
    }

    private static Block replace(Block longs, int position, long value)
    {
        long[] values = new long[longs.getPositionCount()];
        for (int index = 0; index < values.length; index++) {
            values[index] = BIGINT.getLong(longs, index);
        }
        values[position] = value;
        return createLongsBlock(Arrays.stream(values).boxed().toList());
    }
}
