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
package io.trino.operator.join;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestBlockPositionIndex
{
    @Test
    public void testUniformFullPages()
    {
        assertResolvesEveryRow(8192, 8192, 8192, 8192);
    }

    @Test
    public void testShortFinalPage()
    {
        assertResolvesEveryRow(8192, 8192, 5000);
    }

    @Test
    public void testShortInteriorPageDeAlignsGrid()
    {
        assertResolvesEveryRow(8192, 100, 8192, 8192);
    }

    @Test
    public void testSingleBlock()
    {
        assertResolvesEveryRow(5000);
    }

    @Test
    public void testSingleFullWindowBlock()
    {
        assertResolvesEveryRow(8192);
    }

    @Test
    public void testNonPowerOfTwoPages()
    {
        assertResolvesEveryRow(1000, 2000, 3000, 1);
    }

    @Test
    public void testManyWindowsSpanned()
    {
        int[] counts = new int[10];
        for (int i = 0; i < counts.length; i++) {
            counts[i] = 8192;
        }
        assertResolvesEveryRow(counts);
    }

    @Test
    public void testCoarseWindows()
    {
        // more rows than the directory budget, with tiny blocks so one window spans several blocks
        IntArrayList counts = new IntArrayList();
        for (int i = 0; i < 100; i++) {
            counts.add(10_000);
            counts.add(1);
            counts.add(0);
            counts.add(2);
            counts.add(1);
        }
        assertResolvesEveryRow(counts.toIntArray());
    }

    @Test
    public void testEmptyInput()
    {
        // no rows: directory is empty and construction must not index a phantom window
        assertResolvesEveryRow();
        assertResolvesEveryRow(0);
    }

    @Test
    public void testZeroLengthInteriorBlock()
    {
        assertResolvesEveryRow(8192, 0, 8192);
    }

    @Test
    public void testWindowBoundaryRows()
    {
        // counts that place block boundaries away from window boundaries
        int[] counts = {8000, 8000, 8000};
        BlockPositionIndex index = build(counts);
        int[] blockStarts = prefixSum(counts);
        int total = blockStarts[counts.length];
        for (int rowNumber : List.of(0, 8191, 8192, 8193, 15999, 16000, 16383, 16384, total - 1)) {
            assertResolved(index, blockStarts, rowNumber);
        }
    }

    private static void assertResolvesEveryRow(int... counts)
    {
        BlockPositionIndex index = build(counts);
        int[] blockStarts = prefixSum(counts);
        int total = blockStarts[counts.length];
        for (int rowNumber = 0; rowNumber < total; rowNumber++) {
            assertResolved(index, blockStarts, rowNumber);
        }
        assertThat(index.getRetainedSizeInBytes()).isEqualTo(BlockPositionIndex.getEstimatedRetainedSizeInBytes(counts.length, total));
    }

    private static void assertResolved(BlockPositionIndex index, int[] blockStarts, int rowNumber)
    {
        int expectedBlock = expectedBlockIndex(blockStarts, rowNumber);
        int blockIndex = index.decodeBlockIndex(rowNumber);
        assertThat(blockIndex)
                .as("decodeBlockIndex for row number %d", rowNumber)
                .isEqualTo(expectedBlock);
        assertThat(index.decodePosition(rowNumber, blockIndex))
                .as("decodePosition for row number %d", rowNumber)
                .isEqualTo(rowNumber - blockStarts[expectedBlock]);
    }

    private static BlockPositionIndex build(int[] counts)
    {
        IntArrayList positionCounts = new IntArrayList(counts);
        return new BlockPositionIndex(positionCounts);
    }

    private static int[] prefixSum(int[] counts)
    {
        int[] starts = new int[counts.length + 1];
        for (int i = 0; i < counts.length; i++) {
            starts[i + 1] = starts[i] + counts[i];
        }
        return starts;
    }

    private static int expectedBlockIndex(int[] blockStarts, int rowNumber)
    {
        for (int block = 0; block < blockStarts.length - 1; block++) {
            if (rowNumber < blockStarts[block + 1]) {
                return block;
            }
        }
        throw new IllegalArgumentException("row number out of range: " + rowNumber);
    }
}
