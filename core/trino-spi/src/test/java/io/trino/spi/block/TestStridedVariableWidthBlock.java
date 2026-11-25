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

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestStridedVariableWidthBlock
{
    private static final int[] MAX_LENGTHS = {0, 1, 15, 16, 17, 32, 33, 64, 65, 300};

    @Test
    void testBuild()
    {
        for (int maxLength : MAX_LENGTHS) {
            for (int expectedEntries : new int[] {0, 7, 1000}) {
                Slice[] values = randomValues(1000, maxLength, 20);
                StridedVariableWidthBlockBuilder builder = new StridedVariableWidthBlockBuilder(expectedEntries, expectedEntries * 3);
                append(builder, values, 0, 10);
                // a block built before the builder grows keeps its values
                StridedVariableWidthBlock prefix = builder.buildValueBlock();
                append(builder, values, 10, values.length);

                assertValues(prefix, Arrays.copyOf(values, 10));
                assertValues(builder.buildValueBlock(), values);
                assertThat(builder.getPositionCount()).isEqualTo(values.length);
                assertThat(builder.getSizeInBytes()).isEqualTo(builder.buildValueBlock().getSizeInBytes());
            }
        }
    }

    @Test
    void testNullAndEmptyValues()
    {
        StridedVariableWidthBlock block = new StridedVariableWidthBlockBuilder(4, 5)
                .appendNull()
                .writeEntry(Slices.EMPTY_SLICE)
                .writeEntry(Slices.utf8Slice("value"))
                .appendNull()
                .buildValueBlock();

        assertValues(block, new Slice[] {null, Slices.EMPTY_SLICE, Slices.utf8Slice("value"), null});
        assertThat(block.mayHaveNull()).isTrue();
        assertThat(new StridedVariableWidthBlockBuilder(1, 0).writeEntry(Slices.EMPTY_SLICE).buildValueBlock().mayHaveNull()).isFalse();
        assertValues(new StridedVariableWidthBlockBuilder(0, 0).buildValueBlock(), new Slice[0]);

        StridedVariableWidthBlock noNulls = new StridedVariableWidthBlockBuilder(1, 0).writeEntry(Slices.utf8Slice("ab")).buildValueBlock();
        assertThat(noNulls.isNull(0)).isFalse();
        assertThatThrownBy(() -> noNulls.isNull(1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testRegionAndCopyPositions()
    {
        for (int maxLength : MAX_LENGTHS) {
            Slice[] values = randomValues(1000, maxLength, 20);
            StridedVariableWidthBlock block = build(values);

            assertValues(block.getRegion(0, values.length), values);
            assertValues(block.getRegion(300, 400).getRegion(100, 50), Arrays.copyOfRange(values, 400, 450));
            assertValues(block.getRegion(1000, 0), new Slice[0]);
            assertThatThrownBy(() -> block.getRegion(990, 11)).isInstanceOf(IndexOutOfBoundsException.class);

            // positions that are out of order and repeated
            int[] positions = new Random(7).ints(500, 0, 400).toArray();
            Slice[] expected = Arrays.stream(positions).mapToObj(position -> values[300 + position]).toArray(Slice[]::new);
            StridedVariableWidthBlock copy = block.getRegion(300, 400).copyPositions(positions, 0, positions.length);
            assertValues(copy, expected);
            assertValues(block.copyPositions(positions, 100, 0), new Slice[0]);
            assertThatThrownBy(() -> block.getRegion(300, 400).copyPositions(new int[] {400}, 0, 1)).isInstanceOf(IndexOutOfBoundsException.class);
            assertThatThrownBy(() -> block.getRegion(300, 400).copyPositions(new int[] {-1}, 0, 1)).isInstanceOf(IndexOutOfBoundsException.class);

            // a block read from an array that ends with the block, which has fewer bytes after its last values than a chunk
            StridedVariableWidthBlock exact = serializeAndRead(block.getRegion(0, 5));
            assertValues(exact.copyPositions(new int[] {4, 0, 3, 3, 4, 1}, 0, 6), new Slice[] {values[4], values[0], values[3], values[3], values[4], values[1]});

            // positions appended after values, and values written after appended positions
            StridedVariableWidthBlockBuilder builder = new StridedVariableWidthBlockBuilder(0, 0);
            append(builder, values, 0, 3);
            builder.appendPositions(block, new int[] {5, 4}, 0, 2);
            append(builder, values, 6, 8);
            assertValues(builder.buildValueBlock(), new Slice[] {values[0], values[1], values[2], values[5], values[4], values[6], values[7]});

            // a builder that grows while positions are appended to it
            StridedVariableWidthBlockBuilder small = new StridedVariableWidthBlockBuilder(1, 0);
            small.writeEntry(Slices.EMPTY_SLICE);
            small.appendPositions(block, positions, 0, positions.length);
            small.appendPositions(block, positions, 0, positions.length);
            assertValues(small.buildValueBlock().getRegion(1 + positions.length, positions.length), Arrays.stream(positions).mapToObj(position -> values[position]).toArray(Slice[]::new));
        }
    }

    @Test
    void testCopyPositionsOfLongValues()
    {
        for (int maxLength : new int[] {65, 128, 129, 192, 193, 256}) {
            Random random = new Random(maxLength);
            Slice[] values = new Slice[500];
            for (int i = 0; i < values.length; i++) {
                if (i % 7 != 3) {
                    byte[] bytes = new byte[maxLength - random.nextInt(20)];
                    random.nextBytes(bytes);
                    values[i] = Slices.wrappedBuffer(bytes);
                }
            }
            values[17] = Slices.wrappedBuffer(new byte[maxLength]);
            StridedVariableWidthBlock block = build(values);
            int[] positions = random.ints(700, 0, values.length).toArray();
            Slice[] expected = Arrays.stream(positions).mapToObj(position -> values[position]).toArray(Slice[]::new);

            assertValues(block.copyPositions(positions, 0, positions.length), expected);
            StridedVariableWidthBlockBuilder builder = new StridedVariableWidthBlockBuilder(1, 1);
            builder.writeEntry(Slices.utf8Slice("first"));
            builder.appendPositions(block, positions, 0, positions.length);
            builder.appendPositions(serializeAndRead(block), positions, 0, positions.length);
            assertValues(builder.buildValueBlock().getRegion(1, positions.length), expected);
            assertValues(builder.buildValueBlock().getRegion(1 + positions.length, positions.length), expected);
        }
    }

    @Test
    void testSerialization()
    {
        Slice[] values = randomValues(1000, 20, 20);
        StridedVariableWidthBlock block = build(values);

        DynamicSliceOutput output = new DynamicSliceOutput(0);
        output.writeLong(-1);
        block.writeTo(output);
        int regionOffset = output.size();
        block.getRegion(300, 400).writeTo(output);
        int emptyOffset = output.size();
        block.getRegion(1000, 0).writeTo(output);
        output.writeLong(-1);
        Slice serialized = output.slice();

        StridedVariableWidthBlock read = StridedVariableWidthBlock.wrap(serialized.slice(Long.BYTES, regionOffset - Long.BYTES));
        assertValues(read, values);
        assertThat(read.getSizeInBytes()).isEqualTo(block.getSizeInBytes());
        assertThat(read.rawBytes()).isSameAs(serialized.byteArray());
        StridedVariableWidthBlock region = StridedVariableWidthBlock.wrap(serialized.byteArray(), serialized.byteArrayOffset() + regionOffset);
        assertValues(region, Arrays.copyOfRange(values, 300, 700));
        assertValues(region.getRegion(100, 200).copyPositions(new int[] {199, 0, 7}, 0, 3), new Slice[] {values[599], values[400], values[407]});
        assertValues(StridedVariableWidthBlock.wrap(serialized.byteArray(), serialized.byteArrayOffset() + emptyOffset), new Slice[0]);

        DynamicSliceOutput copy = new DynamicSliceOutput(0);
        read.writeTo(copy);
        assertThat(copy.slice()).isEqualTo(serialized.slice(Long.BYTES, regionOffset - Long.BYTES));
        DynamicSliceOutput regionCopy = new DynamicSliceOutput(0);
        region.writeTo(regionCopy);
        assertThat(regionCopy.slice()).isEqualTo(serialized.slice(regionOffset, emptyOffset - regionOffset));
        assertThat(region.firstStart()).isEqualTo(0);

        assertThatThrownBy(() -> StridedVariableWidthBlock.wrap(serialized.slice(Long.BYTES, regionOffset - Long.BYTES - 1).copy()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> StridedVariableWidthBlock.wrap(new byte[StridedVariableWidthBlock.HEADER_SIZE - 1], 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testValueBlock()
    {
        Slice[] values = randomValues(300, 20, 20);
        StridedVariableWidthBlock block = build(values);
        assertThat(block.getUnderlyingValueBlock()).isSameAs(block);

        for (int position : new int[] {0, 7, 299}) {
            assertValues(block.getSingleValueBlock(position), new Slice[] {values[position]});
            long expectedSize = values[position] != null ? values[position].length() : 0;
            assertThat(block.getEstimatedDataSizeForStats(position)).isEqualTo(expectedSize);
        }

        StridedVariableWidthBlock copy = block.copyRegion(100, 50);
        assertValues(copy, Arrays.copyOfRange(values, 100, 150));
        assertThat(copy.rawBytes()).isNotSameAs(block.rawBytes());
        assertThat(copy.getRetainedSizeInBytes()).isLessThan(block.getRetainedSizeInBytes());
        assertValues(block.copyRegion(300, 0), new Slice[0]);

        StridedVariableWidthBlock withNull = block.copyWithAppendedNull();
        assertValues(withNull, Arrays.copyOf(values, values.length + 1));
        assertValues(build(randomValues(10, 20, 0)).copyWithAppendedNull().getRegion(10, 1), new Slice[] {null});

        Bitmap bitmap = block.getValidityBitmap().orElseThrow();
        Bitmap regionBitmap = block.getRegion(100, 50).getValidityBitmap().orElseThrow();
        assertThat(bitmap.getBitCount()).isEqualTo(300);
        assertThat(regionBitmap.getBitCount()).isEqualTo(50);
        for (int position = 0; position < 300; position++) {
            assertThat(bitmap.isSet(position)).isEqualTo(values[position] != null);
        }
        for (int position = 0; position < 50; position++) {
            assertThat(regionBitmap.isSet(position)).isEqualTo(values[100 + position] != null);
        }
        assertThat(build(randomValues(10, 20, 0)).getValidityBitmap()).isEmpty();

        assertThat(block.getRegionSizeInBytes(0, 300)).isEqualTo(block.getSizeInBytes());
        assertThat(block.getRegionSizeInBytes(10, 0)).isEqualTo(0);
        long[] retained = new long[1];
        block.retainedBytesForEachPart((_, size) -> retained[0] += size);
        assertThat(retained[0]).isEqualTo(block.getRetainedSizeInBytes());
    }

    @Test
    void testBlockBuilder()
    {
        Slice[] values = randomValues(300, 20, 20);
        VariableWidthBlockBuilder variableWidthBuilder = new VariableWidthBlockBuilder(null, 300, 0);
        for (Slice value : values) {
            if (value == null) {
                variableWidthBuilder.appendNull();
            }
            else {
                variableWidthBuilder.writeEntry(value);
            }
        }
        Slice first = Slices.utf8Slice("first");

        for (ValueBlock source : new ValueBlock[] {build(values), variableWidthBuilder.buildValueBlock()}) {
            StridedVariableWidthBlockBuilder builder = new StridedVariableWidthBlockBuilder(0, 0);
            builder.writeEntry(first);
            builder.append(source, 5);
            builder.appendRepeated(source, 6, 3);
            builder.appendRange(source, 100, 50);
            builder.appendPositions(source, new int[] {299, 0, 150}, 0, 3);
            Slice[] expected = new Slice[58];
            expected[0] = first;
            expected[1] = values[5];
            Arrays.fill(expected, 2, 5, values[6]);
            System.arraycopy(values, 100, expected, 5, 50);
            expected[55] = values[299];
            expected[56] = values[0];
            expected[57] = values[150];
            assertValues(builder.buildValueBlock(), expected);

            builder.resetTo(5);
            builder.appendNull();
            builder.writeEntry(Slices.utf8Slice("last"));
            assertValues(builder.buildValueBlock(), new Slice[] {first, values[5], values[6], values[6], values[6], null, Slices.utf8Slice("last")});
            builder.resetTo(0);
            builder.appendRange(source, 290, 10);
            assertValues(builder.buildValueBlock(), Arrays.copyOfRange(values, 290, 300));
        }

        StridedVariableWidthBlockBuilder nulls = new StridedVariableWidthBlockBuilder(0, 0);
        nulls.appendNull().appendNull();
        assertThat(nulls.build()).isInstanceOf(RunLengthEncodedBlock.class);
        assertThat(nulls.build().getPositionCount()).isEqualTo(2);
        assertThat(nulls.build().isNull(1)).isTrue();
        nulls.writeEntry(first);
        assertThat(nulls.build()).isInstanceOf(StridedVariableWidthBlock.class);

        PageBuilderStatus status = new PageBuilderStatus(PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES);
        StridedVariableWidthBlockBuilder reporting = new StridedVariableWidthBlockBuilder(status.createBlockBuilderStatus(), 0, 0);
        for (int i = 0; i < 100; i++) {
            reporting.writeEntry(Slices.utf8Slice("2026-11-24"));
        }
        assertThat(status.getSizeInBytes()).isEqualTo(100 * (10 + Integer.BYTES));
    }

    @Test
    void testEncoding()
    {
        StridedVariableWidthBlockEncoding encoding = new StridedVariableWidthBlockEncoding();
        Slice[] values = randomValues(300, 20, 20);
        StridedVariableWidthBlock block = build(values);
        DynamicSliceOutput output = new DynamicSliceOutput(0);
        encoding.writeBlock(null, output, block.getRegion(10, 100));
        encoding.writeBlock(null, output, block);
        SliceInput input = output.slice().getInput();
        assertValues((StridedVariableWidthBlock) encoding.readBlock(null, input), Arrays.copyOfRange(values, 10, 110));
        assertValues((StridedVariableWidthBlock) encoding.readBlock(null, input), values);
        assertThat(input.isReadable()).isFalse();
    }

    @Test
    void testLargeValues()
    {
        Slice big = Slices.wrappedBuffer(new byte[70_000]);
        Slice[] values = new Slice[40];
        for (int i = 0; i < values.length; i++) {
            values[i] = i == 5 ? null : (i == 17 ? big : Slices.utf8Slice("value-" + i));
        }
        StridedVariableWidthBlock block = build(values);
        assertValues(block, values);
        assertValues(block.getRegion(10, 20), Arrays.copyOfRange(values, 10, 30));
        assertValues(serializeAndRead(block), values);
        assertValues(serializeAndRead(block.getRegion(10, 20)), Arrays.copyOfRange(values, 10, 30));
    }

    @Test
    void testBatchOpsAndValueSpan()
    {
        Slice[] values = new Slice[200];
        Slice needle = Slices.utf8Slice("target-val");
        for (int i = 0; i < values.length; i++) {
            if (i % 9 == 0) {
                values[i] = null;
            }
            else if (i % 7 == 0) {
                values[i] = needle;
            }
            else {
                values[i] = Slices.utf8Slice("other-" + i);
            }
        }
        StridedVariableWidthBlock block = build(values);
        for (StridedVariableWidthBlock candidate : new StridedVariableWidthBlock[] {block, block.getRegion(10, 150)}) {
            int n = candidate.getPositionCount();
            int[] allPos = new int[n];
            for (int i = 0; i < n; i++) {
                allPos[i] = i;
                long span = candidate.valueSpan(i);
                if (candidate.isNull(i)) {
                    assertThat(span).isEqualTo(-1L);
                }
                else {
                    int off = (int) (span >>> 32);
                    int len = (int) span;
                    assertThat(Slices.wrappedBuffer(candidate.rawBytes(), off, len)).isEqualTo(candidate.getSlice(i));
                }
            }
            long[] hashes = new long[n];
            candidate.hashPositions(allPos, 0, n, hashes, 0);
            for (int i = 0; i < n; i++) {
                assertThat(hashes[i]).isEqualTo(candidate.isNull(i) ? 0L : XxHash64.hash(candidate.getSlice(i)));
            }
            int matched = candidate.filterEquals(needle.byteArray(), needle.byteArrayOffset(), needle.length(), allPos, n);
            int expectedMatched = 0;
            for (int i = 0; i < n; i++) {
                if (!candidate.isNull(i) && candidate.getSlice(i).equals(needle)) {
                    assertThat(allPos[expectedMatched++]).isEqualTo(i);
                }
            }
            assertThat(matched).isEqualTo(expectedMatched);
        }
    }

    private static StridedVariableWidthBlock serializeAndRead(StridedVariableWidthBlock block)
    {
        DynamicSliceOutput output = new DynamicSliceOutput(0);
        block.writeTo(output);
        return StridedVariableWidthBlock.wrap(output.slice().copy());
    }

    private static StridedVariableWidthBlock build(Slice[] values)
    {
        StridedVariableWidthBlockBuilder builder = new StridedVariableWidthBlockBuilder(values.length, totalLength(values));
        append(builder, values, 0, values.length);
        return builder.buildValueBlock();
    }

    private static void append(StridedVariableWidthBlockBuilder builder, Slice[] values, int from, int to)
    {
        for (int i = from; i < to; i++) {
            if (values[i] == null) {
                builder.appendNull();
            }
            else {
                builder.writeEntry(values[i]);
            }
        }
    }

    private static int totalLength(Slice[] values)
    {
        int totalLength = 0;
        for (Slice value : values) {
            if (value != null) {
                totalLength += value.length();
            }
        }
        return totalLength;
    }

    private static Slice[] randomValues(int count, int maxLength, int nullPercent)
    {
        Random random = new Random(count + maxLength + nullPercent);
        Slice[] values = new Slice[count];
        for (int i = 0; i < count; i++) {
            if (random.nextInt(100) >= nullPercent) {
                byte[] bytes = new byte[random.nextInt(maxLength + 1)];
                random.nextBytes(bytes);
                values[i] = Slices.wrappedBuffer(bytes);
            }
        }
        return values;
    }

    private static void assertValues(StridedVariableWidthBlock block, Slice[] expected)
    {
        assertThat(block.getPositionCount()).isEqualTo(expected.length);
        Slice raw = block.getRawSlice();
        for (int position = 0; position < expected.length; position++) {
            assertThat(block.isNull(position)).isEqualTo(expected[position] == null);
            Slice value = expected[position];
            if (value == null) {
                value = Slices.EMPTY_SLICE;
            }
            assertThat(block.getSlice(position)).isEqualTo(value);
            assertThat(block.getSliceLength(position)).isEqualTo(value.length());
            assertThat(raw.slice(block.getRawSliceOffset(position), block.getSliceLength(position))).isEqualTo(value);
            assertThat(block.valueHash(position)).isEqualTo(XxHash64.hash(value));
            byte[] copy = new byte[value.length() + 2];
            block.copyValue(position, copy, 1);
            assertThat(Slices.wrappedBuffer(copy, 1, value.length())).isEqualTo(value);
            assertThat(block.valueEquals(position, value.byteArray(), value.byteArrayOffset(), value.length())).isTrue();
            assertThat(block.compareValue(position, value.byteArray(), value.byteArrayOffset(), value.length())).isEqualTo(0);
            int other = Math.max(0, position - 1);
            assertThat(block.valueEquals(position, block, other)).isEqualTo(value.equals(block.getSlice(other)));
            assertThat(Integer.signum(block.compareValue(position, block, other))).isEqualTo(Integer.signum(value.compareTo(block.getSlice(other))));
        }
        assertThatThrownBy(() -> block.getSlice(expected.length)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> block.getSliceLength(-1)).isInstanceOf(IllegalArgumentException.class);
    }
}
