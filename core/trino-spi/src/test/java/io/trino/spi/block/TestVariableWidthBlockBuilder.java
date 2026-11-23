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
package io.trino.spi.block;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.PageBuilder;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.block.BlockUtil.MAX_ARRAY_SIZE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.ceil;
import static java.util.Collections.nCopies;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestVariableWidthBlockBuilder
        extends AbstractTestBlockBuilder<String>
{
    private static final int BLOCK_BUILDER_INSTANCE_SIZE = instanceSize(VariableWidthBlockBuilder.class);
    private static final int VARCHAR_VALUE_SIZE = 7;
    private static final int VARCHAR_ENTRY_SIZE = SIZE_OF_INT + VARCHAR_VALUE_SIZE;
    private static final int EXPECTED_ENTRY_COUNT = 3;

    @Test
    public void testFixedBlockIsFull()
    {
        testIsFull(PageBuilder.withMaxPageSize(VARCHAR_ENTRY_SIZE * EXPECTED_ENTRY_COUNT, ImmutableList.of(VARCHAR)));
    }

    @Test
    public void testInitialEntryCapacity()
    {
        assertInitialEntryCapacity(0, 1, 1);
        assertInitialEntryCapacity(1, 1, 1);
        assertInitialEntryCapacity(3, 1, 3);
        assertInitialEntryCapacity(1, 5, 5);
        assertInitialEntryCapacity(100, 1, 100);
    }

    private static void assertInitialEntryCapacity(int expectedEntries, int requiredEntries, int expectedCapacity)
    {
        VariableWidthBlockBuilder builder = new VariableWidthBlockBuilder(null, expectedEntries, 0);
        assertThat(builder.buildValueBlock().getRawOffsets()).hasSize(1);

        VariableWidthBlock value = new VariableWidthBlockBuilder(null, 1, 0).writeEntry(EMPTY_SLICE).buildValueBlock();
        builder.appendRepeated(value, 0, requiredEntries);

        VariableWidthBlock block = builder.buildValueBlock();
        assertThat(block.getRawOffsets()).hasSize(expectedCapacity + 1);
        assertThat(toStrings(block)).isEqualTo(nCopies(requiredEntries, ""));
    }

    @Test
    public void testInvalidExpectedEntries()
    {
        for (int expectedEntries : new int[] {-1, Integer.MAX_VALUE}) {
            VariableWidthBlockBuilder builder = new VariableWidthBlockBuilder(null, expectedEntries, 0);
            assertThatThrownBy(() -> builder.writeEntry(EMPTY_SLICE))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Invalid currentSize or minimumSize");
        }
    }

    @Test
    public void testInvalidInitialCapacity()
    {
        VariableWidthBlock value = new VariableWidthBlockBuilder(null, 1, 0).writeEntry(EMPTY_SLICE).buildValueBlock();
        for (int count : new int[] {MAX_ARRAY_SIZE + 1, Integer.MAX_VALUE}) {
            VariableWidthBlockBuilder builder = new VariableWidthBlockBuilder(null, 1, 0);
            assertThatThrownBy(() -> builder.appendRepeated(value, 0, count))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("Invalid currentSize or minimumSize");
        }

        VariableWidthBlockBuilder builder = new VariableWidthBlockBuilder(null, 1, 0);
        assertThatThrownBy(() -> builder.appendRepeated(value, 0, MAX_ARRAY_SIZE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cannot grow array beyond size " + MAX_ARRAY_SIZE);
    }

    @Test
    public void testGrowthAfterInitialAllocation()
    {
        VariableWidthBlockBuilder builder = new VariableWidthBlockBuilder(null, 1, 0);
        builder.appendNull();
        assertThat(builder.buildValueBlock().getRawOffsets()).containsExactly(0, 0);

        builder.writeEntry(utf8Slice("value"));
        VariableWidthBlock block = builder.buildValueBlock();
        assertThat(block.getRawOffsets()).hasSize(65);
        assertThat(block.getPositionCount()).isEqualTo(2);
        assertThat(block.isNull(0)).isTrue();
        assertThat(block.isNull(1)).isFalse();
        assertThat(block.getSlice(1)).isEqualTo(utf8Slice("value"));
    }

    @Test
    public void testNewBlockBuilderLike()
    {
        int entries = 12345;
        double resetSkew = 1.25;
        VariableWidthBlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, entries, entries);
        for (int i = 0; i < entries; i++) {
            blockBuilder.writeEntry(Slices.wrappedBuffer((byte) i));
        }
        blockBuilder = (VariableWidthBlockBuilder) blockBuilder.newBlockBuilderLike(null);
        // force to initialize capacity
        blockBuilder.writeEntry(Slices.wrappedBuffer((byte) 1));

        long actualArraySize = sizeOf(new int[(int) ceil(resetSkew * (entries + 1))]);
        long actualBytesSize = sizeOf(new byte[(int) ceil(resetSkew * entries)]);
        assertThat(blockBuilder.getRetainedSizeInBytes()).isEqualTo(BLOCK_BUILDER_INSTANCE_SIZE + actualBytesSize + actualArraySize);
    }

    @Test
    public void testNewBlockBuilderLikeWithExpectedEntries()
    {
        VariableWidthBlockBuilder previous = new VariableWidthBlockBuilder(null, 100, 0);
        for (int i = 0; i < 100; i++) {
            previous.writeEntry(utf8Slice("abcdefgh"));
        }

        assertNewBlockBuilderLikeSize(previous, 25, 250);
        assertNewBlockBuilderLikeSize(previous, 100, 1000);
        // growth is capped at twice the previous bytes
        assertNewBlockBuilderLikeSize(previous, 400, 2000);
    }

    @Test
    public void testNewBlockBuilderLikeCapsGrowthFromSingleWideRow()
    {
        VariableWidthBlockBuilder previous = new VariableWidthBlockBuilder(null, 1, 0);
        previous.writeEntry(Slices.allocate(100 * 1024));

        assertNewBlockBuilderLikeSize(previous, 8192, 256_000);
    }

    @Test
    public void testNewBlockBuilderLikeRejectsInvalidExpectedEntries()
    {
        VariableWidthBlockBuilder previous = new VariableWidthBlockBuilder(null, 100, 0);
        for (int i = 0; i < 100; i++) {
            previous.writeEntry(utf8Slice("abcdefgh"));
        }

        assertThatThrownBy(() -> previous.newBlockBuilderLike(Integer.MAX_VALUE, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("expectedEntries");
        assertThatThrownBy(() -> previous.newBlockBuilderLike(-1, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("expectedEntries");
    }

    private static void assertNewBlockBuilderLikeSize(VariableWidthBlockBuilder previous, int expectedEntries, int expectedBytes)
    {
        VariableWidthBlockBuilder next = (VariableWidthBlockBuilder) previous.newBlockBuilderLike(expectedEntries, null);
        next.writeEntry(utf8Slice("abcdefgh"));
        long expectedSize = BLOCK_BUILDER_INSTANCE_SIZE + sizeOf(new int[expectedEntries + 1]) + sizeOf(new byte[expectedBytes]);
        assertThat(next.getRetainedSizeInBytes()).isEqualTo(expectedSize);
        assertThat(toStrings(next.buildValueBlock())).containsExactly("abcdefgh");
    }

    private void testIsFull(PageBuilder pageBuilder)
    {
        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(0);
        assertThat(pageBuilder.isEmpty()).isTrue();
        while (!pageBuilder.isFull()) {
            pageBuilder.declarePosition();
            VARCHAR.writeSlice(blockBuilder, Slices.allocate(VARCHAR_VALUE_SIZE));
        }
        assertThat(blockBuilder.getPositionCount()).isEqualTo(EXPECTED_ENTRY_COUNT);
        assertThat(pageBuilder.isFull()).isEqualTo(true);
    }

    @Test
    public void testBuilderProducesNullRleForNullRows()
    {
        // empty block
        assertIsAllNulls(blockBuilder().build(), 0);

        // single null
        assertIsAllNulls(blockBuilder().appendNull().build(), 1);

        // multiple nulls
        assertIsAllNulls(blockBuilder().appendNull().appendNull().build(), 2);
    }

    @Test
    public void testAppendRepeatedEmpty()
    {
        VariableWidthBlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, 1, 100);
        ValueBlock value = VARCHAR.createBlockBuilder(null, 1)
                .writeEntry(utf8Slice("ignored"))
                .writeEntry(Slices.EMPTY_SLICE)
                .writeEntry(utf8Slice("ignored"))
                .buildValueBlock();
        blockBuilder.appendRepeated(value, 1, 10);

        List<String> strings = toStrings(blockBuilder.buildValueBlock());
        assertThat(strings).isEqualTo(nCopies(10, ""));
    }

    @Test
    public void testAppendRepeatedSingle()
    {
        VariableWidthBlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, 1, 100);
        VariableWidthBlock value = VARCHAR.createBlockBuilder(null, 1)
                .writeEntry(utf8Slice("ignored"))
                .writeEntry(utf8Slice("ab"))
                .writeEntry(utf8Slice("ignored"))
                .buildValueBlock();
        blockBuilder.appendRepeated(value, 1, 1);

        List<String> strings = toStrings(blockBuilder.buildValueBlock());
        assertThat(strings).isEqualTo(ImmutableList.of("ab"));
    }

    @Test
    public void testAppendRepeated1Byte()
    {
        VariableWidthBlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, 1, 100);
        VariableWidthBlock value = VARCHAR.createBlockBuilder(null, 1)
                .writeEntry(utf8Slice("ignored"))
                .writeEntry(utf8Slice("X"))
                .writeEntry(utf8Slice("ignored"))
                .writeEntry(utf8Slice("Y"))
                .writeEntry(utf8Slice("ignored"))
                .buildValueBlock();
        blockBuilder.appendRepeated(value, 1, 3);
        blockBuilder.appendRepeated(value, 3, 2);

        List<String> strings = toStrings(blockBuilder.buildValueBlock());
        assertThat(strings).isEqualTo(ImmutableList.of("X", "X", "X", "Y", "Y"));
    }

    @Test
    public void testAppendRepeated2Bytes()
    {
        VariableWidthBlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, 1, 100);
        VariableWidthBlock value = VARCHAR.createBlockBuilder(null, 1)
                .writeEntry(utf8Slice("ignored"))
                .writeEntry(utf8Slice("ab"))
                .writeEntry(utf8Slice("ignored"))
                .writeEntry(utf8Slice("Y"))
                .writeEntry(utf8Slice("ignored"))
                .buildValueBlock();
        blockBuilder.appendRepeated(value, 1, 3);

        List<String> strings = toStrings(blockBuilder.buildValueBlock());
        assertThat(strings).isEqualTo(ImmutableList.of("ab", "ab", "ab"));
    }

    @Test
    public void testAppendRepeatedMultipleBytesOddNumberOfTimes()
    {
        VariableWidthBlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, 1, 100);
        VariableWidthBlock value = VARCHAR.createBlockBuilder(null, 1)
                .writeEntry(utf8Slice("ignored"))
                .writeEntry(utf8Slice("abc"))
                .writeEntry(utf8Slice("ignored"))
                .buildValueBlock();
        blockBuilder.appendRepeated(value, 1, 5);

        List<String> strings = toStrings(blockBuilder.buildValueBlock());
        assertThat(strings).isEqualTo(nCopies(5, "abc"));
    }

    @Test
    public void testAppendRepeatedMultipleBytesEvenNumberOfTimes()
    {
        VariableWidthBlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, 1, 100);
        VariableWidthBlock value = VARCHAR.createBlockBuilder(null, 1)
                .writeEntry(utf8Slice("ignored"))
                .writeEntry(utf8Slice("abc"))
                .writeEntry(utf8Slice("ignored"))
                .buildValueBlock();
        blockBuilder.appendRepeated(value, 1, 6);

        List<String> strings = toStrings(blockBuilder.buildValueBlock());
        assertThat(strings).isEqualTo(nCopies(6, "abc"));
    }

    @Test
    public void testAppendRepeatedMultipleBytesPowerOf2NumberOfTimes()
    {
        VariableWidthBlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, 1, 100);
        VariableWidthBlock value = VARCHAR.createBlockBuilder(null, 1)
                .writeEntry(utf8Slice("ignored"))
                .writeEntry(utf8Slice("abc"))
                .writeEntry(utf8Slice("ignored"))
                .buildValueBlock();
        blockBuilder.appendRepeated(value, 1, 8);

        List<String> strings = toStrings(blockBuilder.buildValueBlock());
        assertThat(strings).isEqualTo(nCopies(8, "abc"));
    }

    private static BlockBuilder blockBuilder()
    {
        return new VariableWidthBlockBuilder(null, 10, 0);
    }

    private static void assertIsAllNulls(Block block, int expectedPositionCount)
    {
        assertThat(block.getPositionCount()).isEqualTo(expectedPositionCount);
        if (expectedPositionCount <= 1) {
            assertThat(block.getClass()).isEqualTo(VariableWidthBlock.class);
        }
        else {
            assertThat(block.getClass()).isEqualTo(RunLengthEncodedBlock.class);
            assertThat(((RunLengthEncodedBlock) block).getValue().getClass()).isEqualTo(VariableWidthBlock.class);
        }
        if (expectedPositionCount > 0) {
            assertThat(block.isNull(0)).isTrue();
        }
    }

    private static List<String> toStrings(VariableWidthBlock block)
    {
        ImmutableList.Builder<String> list = ImmutableList.builder();
        for (int i = 0; i < block.getPositionCount(); i++) {
            list.add(VARCHAR.getSlice(block, i).toStringUtf8());
        }
        return list.build();
    }

    @Override
    protected BlockBuilder createBlockBuilder()
    {
        return new VariableWidthBlockBuilder(null, 1, 100);
    }

    @Override
    protected List<String> getTestValues()
    {
        return List.of("a", "bb", "cCc", "dddd", "eeEee");
    }

    @Override
    protected String getUnusedTestValue()
    {
        return "unused";
    }

    @Override
    protected ValueBlock blockFromValues(Iterable<String> values)
    {
        VariableWidthBlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, 1, 100);
        for (String value : values) {
            if (value == null) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.writeEntry(utf8Slice(value));
            }
        }
        return blockBuilder.buildValueBlock();
    }

    @Override
    protected List<String> blockToValues(ValueBlock valueBlock)
    {
        VariableWidthBlock block = (VariableWidthBlock) valueBlock;
        List<String> actualValues = new ArrayList<>(block.getPositionCount());
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                actualValues.add(null);
            }
            else {
                actualValues.add(block.getSlice(i).toStringUtf8());
            }
        }
        return actualValues;
    }

    @Test
    public void testAppendReportsSizeToStatus()
    {
        VariableWidthBlockBuilder sourceBuilder = new VariableWidthBlockBuilder(null, 4, 0);
        sourceBuilder.writeEntry(utf8Slice("a"));
        sourceBuilder.appendNull();
        sourceBuilder.writeEntry(utf8Slice("bcd"));
        sourceBuilder.writeEntry(utf8Slice("efghij"));
        VariableWidthBlock source = sourceBuilder.buildValueBlock();

        PageBuilderStatus status = new PageBuilderStatus(1000);
        VariableWidthBlockBuilder builder = new VariableWidthBlockBuilder(status.createBlockBuilderStatus(), 0, 0);
        builder.appendRange(source, 1, 3);
        assertThat(status.getSizeInBytes()).isEqualTo(builder.getSizeInBytes());
        builder.appendPositions(source, new int[] {3, 0, 2}, 0, 3);
        assertThat(status.getSizeInBytes()).isEqualTo(builder.getSizeInBytes());
    }

    @Test
    public void testAppendPositionsOfVariousLengths()
    {
        Random random = new Random(8473);
        // lengths around the sizes of the chunks that values are copied in
        for (int maxLength : new int[] {0, 12, 16, 17, 32, 33, 64, 65, 300}) {
            for (boolean longValues : new boolean[] {false, true}) {
                VariableWidthBlock block = randomBlock(random, 500, maxLength, longValues);
                // a block whose values are in the middle of an array, and a block that does not start at the first offset
                Slice padded = Slices.allocate(block.getRawSlice().length() + 200);
                padded.setBytes(13, block.getRawSlice());
                VariableWidthBlock inArray = new VariableWidthBlock(500, padded.slice(13, block.getRawSlice().length()), block.getRawOffsets(), Optional.ofNullable(block.getRawValueIsValid()));
                for (VariableWidthBlock source : List.of(block, inArray, block.getRegion(100, 300))) {
                    int[] positions = random.ints(400, 0, source.getPositionCount()).toArray();
                    assertAppendPositions(source, positions);
                    Arrays.sort(positions);
                    assertAppendPositions(source, positions);
                    // the last positions, whose values are at the end of the array
                    assertAppendPositions(source, new int[] {source.getPositionCount() - 1, source.getPositionCount() - 2, source.getPositionCount() - 1});
                }
            }
        }
    }

    private static void assertAppendPositions(VariableWidthBlock source, int[] positions)
    {
        List<Slice> expected = new ArrayList<>();
        int size = 0;
        int maxLength = 0;
        for (int position : positions) {
            Slice value = null;
            if (!source.isNull(position)) {
                value = source.getSlice(position);
            }
            expected.add(value);
            size += source.getSliceLength(position);
            maxLength = Math.max(maxLength, source.getSliceLength(position));
        }

        // a builder that has no room for the values, and one that has room for exactly the values
        for (int expectedBytes : new int[] {0, size}) {
            VariableWidthBlockBuilder builder = new VariableWidthBlockBuilder(null, positions.length, expectedBytes);
            builder.appendPositions(source, positions, 0, positions.length);
            assertThat(toSlices(builder.buildValueBlock())).isEqualTo(expected);
            // values appended after values that the builder already has
            builder.appendPositions(source, positions, 1, positions.length - 1);
            assertThat(toSlices(builder.buildValueBlock()).subList(positions.length, 2 * positions.length - 1)).isEqualTo(expected.subList(1, positions.length));
        }

        // copying in chunks and copying the bytes of the values give the same bytes
        int[] targetOffsets = new int[positions.length + 1];
        for (int i = 0; i < positions.length; i++) {
            targetOffsets[i + 1] = targetOffsets[i] + source.getSliceLength(positions[i]);
        }
        Slice rawSlice = source.getRawSlice();
        byte[] copiedInChunks = new byte[size];
        VariableWidthBlockBuilder.copyValues(rawSlice.byteArray(), rawSlice.byteArrayOffset(), source.getRawOffsets(), source.getRawArrayBase(), positions, 0, positions.length, copiedInChunks, targetOffsets, 0, size, maxLength, true);
        byte[] copiedBytes = new byte[size];
        VariableWidthBlockBuilder.copyValues(rawSlice.byteArray(), rawSlice.byteArrayOffset(), source.getRawOffsets(), source.getRawArrayBase(), positions, 0, positions.length, copiedBytes, targetOffsets, 0, size, maxLength, false);
        assertThat(copiedInChunks).isEqualTo(copiedBytes);
    }

    private static VariableWidthBlock randomBlock(Random random, int positionCount, int maxLength, boolean longValues)
    {
        VariableWidthBlockBuilder builder = new VariableWidthBlockBuilder(null, positionCount, 0);
        for (int i = 0; i < positionCount; i++) {
            if (random.nextInt(10) == 0) {
                builder.appendNull();
                continue;
            }
            int length = random.nextInt(maxLength + 1);
            if (longValues && random.nextInt(20) == 0) {
                length *= 10;
            }
            byte[] value = new byte[length];
            random.nextBytes(value);
            builder.writeEntry(Slices.wrappedBuffer(value));
        }
        // a copy, so that the array of the block has no bytes after the values
        VariableWidthBlock built = builder.buildValueBlock();
        return new VariableWidthBlock(positionCount, built.getRawSlice().copy(), built.getRawOffsets(), Optional.ofNullable(built.getRawValueIsValid()));
    }

    private static List<Slice> toSlices(VariableWidthBlock block)
    {
        List<Slice> values = new ArrayList<>();
        for (int position = 0; position < block.getPositionCount(); position++) {
            Slice value = null;
            if (!block.isNull(position)) {
                value = block.getSlice(position);
            }
            values.add(value);
        }
        return values;
    }
}
