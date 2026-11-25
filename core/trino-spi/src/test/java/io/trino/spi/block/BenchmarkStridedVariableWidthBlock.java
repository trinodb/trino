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
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.GCProfiler;

import java.util.Random;

import static io.trino.jmh.Benchmarks.benchmark;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

/// Compares [VariableWidthBlock] with [StridedVariableWidthBlock] for appending values, reading them at any
/// position, copying positions, and serializing to and from a single byte array. Every benchmark processes
/// [#POSITIONS] positions, or half of them where positions are selected.
///
/// A value is read only when it is not null, as callers of a block do. The positions that are read and selected
/// change with every call and repeat only after far more of them than a branch predictor learns: with the same
/// positions in every call a processor learns where the nulls are, and the cost of a null is not measured.
@OutputTimeUnit(MICROSECONDS)
@Fork(2)
@Warmup(iterations = 4, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 6, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(AverageTime)
public class BenchmarkStridedVariableWidthBlock
{
    private static final int POSITIONS = 8192;
    private static final int SELECTED_POSITIONS = POSITIONS / 2;
    // random positions that are read a part at a time: a power of two, and a multiple of POSITIONS
    private static final int POSITION_STREAM = 1 << 20;
    // the blocks that are read in order one after the other, and the selections of positions: powers of two
    private static final int BLOCK_RING = 32;
    private static final int SELECTION_RING = 64;
    private static final long MANY_BLOCKS_BYTES = 512L << 20;
    private static final int MAX_BLOCKS = 4096;

    /// Appends values one at a time to a builder that is reachable from a field, like the builders of a page are.
    /// The JIT compiler keeps a builder that only a loop sees in registers, which no builder of an operator is.
    @Benchmark
    public Object build(BenchmarkData data)
    {
        return buildBlock(data.layout, data.values, POSITIONS, data.totalBytes, data.builderHolder);
    }

    /// Reads the values of a block in order.
    @Benchmark
    public long sequentialRead(BenchmarkData data)
    {
        long sum = 0;
        int ring = data.nextBlock();
        if (data.layout != Layout.VARIABLE_WIDTH) {
            StridedVariableWidthBlock block = data.stridedRing[ring];
            for (int position = 0; position < POSITIONS; position++) {
                if (!block.isNull(position)) {
                    sum += block.getSlice(position).length();
                }
            }
            return sum;
        }
        VariableWidthBlock block = data.variableWidthRing[ring];
        for (int position = 0; position < POSITIONS; position++) {
            if (!block.isNull(position)) {
                sum += block.getSlice(position).length();
            }
        }
        return sum;
    }

    /// Reads values at random positions of one block.
    @Benchmark
    public long randomRead(BenchmarkData data)
    {
        long sum = 0;
        int[] positions = data.positionStream;
        int start = data.nextWindow();
        if (data.layout != Layout.VARIABLE_WIDTH) {
            StridedVariableWidthBlock block = data.stridedBlock;
            for (int i = start; i < start + POSITIONS; i++) {
                int position = positions[i];
                if (!block.isNull(position)) {
                    sum += block.getSlice(position).length();
                }
            }
            return sum;
        }
        VariableWidthBlock block = data.variableWidthBlock;
        for (int i = start; i < start + POSITIONS; i++) {
            int position = positions[i];
            if (!block.isNull(position)) {
                sum += block.getSlice(position).length();
            }
        }
        return sum;
    }

    /// Reads the length of values at random positions of a [VariableWidthBlock] as [#randomRead] does, for the
    /// three ways of making the slice of [SliceWay]. Measures nothing for the other layouts.
    @Benchmark
    public long randomReadOfLength(BenchmarkData data, SliceWay sliceWay)
    {
        long sum = 0;
        int[] positions = data.positionStream;
        int start = data.nextWindow();
        if (data.layout != Layout.VARIABLE_WIDTH) {
            return sum;
        }
        VariableWidthBlock block = data.variableWidthBlock;
        int way = sliceWay.way;
        for (int i = start; i < start + POSITIONS; i++) {
            int position = positions[i];
            if (!block.isNull(position)) {
                sum += slice(block, position, way).length();
            }
        }
        return sum;
    }

    /// Reads the first byte of values at random positions of a [VariableWidthBlock]. Unlike the length, the byte
    /// can only be read through the slice.
    @Benchmark
    public long randomReadOfFirstByte(BenchmarkData data, SliceWay sliceWay)
    {
        long sum = 0;
        int[] positions = data.positionStream;
        int start = data.nextWindow();
        if (data.layout != Layout.VARIABLE_WIDTH) {
            return sum;
        }
        VariableWidthBlock block = data.variableWidthBlock;
        int way = sliceWay.way;
        for (int i = start; i < start + POSITIONS; i++) {
            int position = positions[i];
            if (!block.isNull(position) && block.getSliceLength(position) != 0) {
                sum += slice(block, position, way).getByte(0);
            }
        }
        return sum;
    }

    private static Slice slice(VariableWidthBlock block, int position, int way)
    {
        if (way == 0) {
            return block.getSlice(position);
        }
        if (way == 1) {
            return block.getRawSlice().slice(block.getRawSliceOffset(position), block.getSliceLength(position));
        }
        Slice rawSlice = block.getRawSlice();
        return Slices.wrappedBuffer(rawSlice.byteArray(), rawSlice.byteArrayOffset() + block.getRawSliceOffset(position), block.getSliceLength(position));
    }

    /// How a slice of a value of a [VariableWidthBlock] is made: 0 asks the block, 1 slices the slice of the block
    /// with the offsets that the block exposes, 2 wraps the bytes of the value in a new slice.
    @State(Thread)
    public static class SliceWay
    {
        @Param({"0", "1", "2"})
        private int way;
    }

    /// Hashes values at random positions of one block using the batch monomorphic API (vs scalar loop for VariableWidthBlock).
    @Benchmark
    public long randomHashBatch(BenchmarkData data)
    {
        int[] positions = data.positionStream;
        int start = data.nextWindow();
        long[] out = data.hashOut;
        if (data.layout != Layout.VARIABLE_WIDTH) {
            data.stridedBlock.hashPositions(positions, start, POSITIONS, out, 0);
        }
        else {
            VariableWidthBlock block = data.variableWidthBlock;
            Slice raw = block.getRawSlice();
            for (int i = 0; i < POSITIONS; i++) {
                int position = positions[start + i];
                out[i] = block.isNull(position) ? 0L : XxHash64.hash(raw, block.getRawSliceOffset(position), block.getSliceLength(position));
            }
        }
        long sum = 0;
        for (int i = 0; i < POSITIONS; i++) {
            sum += out[i];
        }
        return sum;
    }

    /// Filters all positions of a block for equality with a constant: [StridedVariableWidthBlock#filterEquals]
    /// against a loop over the positions of a [VariableWidthBlock], which has no such method.
    @Benchmark
    public int filterEquals(BenchmarkData data)
    {
        int[] selected = data.filterScratch;
        for (int i = 0; i < POSITIONS; i++) {
            selected[i] = i;
        }
        byte[] needle = data.filterNeedleBytes;
        int needleOff = data.filterNeedleOffset;
        int needleLen = data.filterNeedleLength;
        if (data.layout != Layout.VARIABLE_WIDTH) {
            return data.stridedBlock.filterEquals(needle, needleOff, needleLen, selected, POSITIONS);
        }
        VariableWidthBlock block = data.variableWidthBlock;
        Slice raw = block.getRawSlice();
        int matched = 0;
        for (int i = 0; i < POSITIONS; i++) {
            if (!block.isNull(i) && block.getSliceLength(i) == needleLen && raw.equals(block.getRawSliceOffset(i), needleLen, needle, needleOff, needleLen)) {
                selected[matched++] = i;
            }
        }
        return matched;
    }

    /// Hashes values at random positions of one block, reading them in place as generated code does.
    @Benchmark
    public long randomHash(BenchmarkData data)
    {
        long sum = 0;
        int[] positions = data.positionStream;
        int start = data.nextWindow();
        if (data.layout != Layout.VARIABLE_WIDTH) {
            StridedVariableWidthBlock block = data.stridedBlock;
            Slice raw = block.getRawSlice();
            for (int i = start; i < start + POSITIONS; i++) {
                int position = positions[i];
                if (!block.isNull(position)) {
                    sum += XxHash64.hash(raw, block.getRawSliceOffset(position), block.getSliceLength(position));
                }
            }
            return sum;
        }
        VariableWidthBlock block = data.variableWidthBlock;
        Slice raw = block.getRawSlice();
        for (int i = start; i < start + POSITIONS; i++) {
            int position = positions[i];
            if (!block.isNull(position)) {
                sum += XxHash64.hash(raw, block.getRawSliceOffset(position), block.getSliceLength(position));
            }
        }
        return sum;
    }

    /// Compares pairs of values at random positions of one block in place, as the comparison operator of a type
    /// does. With [RangeAccessor] 1 the new block compares the values in its own array, without its raw slice.
    @Benchmark
    public long randomCompare(BenchmarkData data, RangeAccessor rangeAccessor)
    {
        long sum = 0;
        int[] positions = data.positionStream;
        int start = data.nextWindow();
        if (data.layout != Layout.VARIABLE_WIDTH) {
            StridedVariableWidthBlock block = data.stridedBlock;
            Slice raw = block.getRawSlice();
            if (rangeAccessor.range == 1) {
                for (int i = start; i < start + POSITIONS; i += 2) {
                    int left = positions[i];
                    int right = positions[i + 1];
                    if (!block.isNull(left) && !block.isNull(right)) {
                        sum += block.compareValue(left, block, right);
                    }
                }
                return sum;
            }
            for (int i = start; i < start + POSITIONS; i += 2) {
                int left = positions[i];
                int right = positions[i + 1];
                if (!block.isNull(left) && !block.isNull(right)) {
                    sum += raw.compareTo(block.getRawSliceOffset(left), block.getSliceLength(left), raw, block.getRawSliceOffset(right), block.getSliceLength(right));
                }
            }
            return sum;
        }
        if (rangeAccessor.range >= 2) {
            // the old block as the comparison operator of a type reads it: 2 is the operator of this branch with the
            // switch off, 3 is the operator of master. Both get a block whose class the caller does not know.
            ValueBlock valueBlock = data.valueBlock;
            VariableWidthBlock typedBlock = data.variableWidthBlock;
            boolean throughValues = rangeAccessor.range == 2;
            for (int i = start; i < start + POSITIONS; i += 2) {
                int left = positions[i];
                int right = positions[i + 1];
                if (!typedBlock.isNull(left) && !typedBlock.isNull(right)) {
                    if (throughValues) {
                        sum += compareThroughValues(valueBlock, left, valueBlock, right);
                    }
                    else {
                        sum += compareTyped((VariableWidthBlock) valueBlock, left, (VariableWidthBlock) valueBlock, right);
                    }
                }
            }
            return sum;
        }
        VariableWidthBlock block = data.variableWidthBlock;
        Slice raw = block.getRawSlice();
        for (int i = start; i < start + POSITIONS; i += 2) {
            int left = positions[i];
            int right = positions[i + 1];
            if (!block.isNull(left) && !block.isNull(right)) {
                sum += raw.compareTo(block.getRawSliceOffset(left), block.getSliceLength(left), raw, block.getRawSliceOffset(right), block.getSliceLength(right));
            }
        }
        return sum;
    }

    private static long compareTyped(VariableWidthBlock leftBlock, int leftPosition, VariableWidthBlock rightBlock, int rightPosition)
    {
        Slice leftRawSlice = leftBlock.getRawSlice();
        int leftRawSliceOffset = leftBlock.getRawSliceOffset(leftPosition);
        int leftLength = leftBlock.getSliceLength(leftPosition);

        Slice rightRawSlice = rightBlock.getRawSlice();
        int rightRawSliceOffset = rightBlock.getRawSliceOffset(rightPosition);
        int rightLength = rightBlock.getSliceLength(rightPosition);

        return leftRawSlice.compareTo(leftRawSliceOffset, leftLength, rightRawSlice, rightRawSliceOffset, rightLength);
    }

    private static long compareThroughValues(ValueBlock leftBlock, int leftPosition, ValueBlock rightBlock, int rightPosition)
    {
        Slice leftRawSlice = VariableWidthValues.getRawSlice(leftBlock);
        int leftRawSliceOffset = VariableWidthValues.getRawSliceOffset(leftBlock, leftPosition);
        int leftLength = VariableWidthValues.getSliceLength(leftBlock, leftPosition);

        Slice rightRawSlice = VariableWidthValues.getRawSlice(rightBlock);
        int rightRawSliceOffset = VariableWidthValues.getRawSliceOffset(rightBlock, rightPosition);
        int rightLength = VariableWidthValues.getSliceLength(rightBlock, rightPosition);

        return leftRawSlice.compareTo(leftRawSliceOffset, leftLength, rightRawSlice, rightRawSliceOffset, rightLength);
    }

    /// How values are compared: 0 through the raw slice of the block, 1 by the new block in its array,
    /// 2 and 3 through the comparison operator of this branch and of master (old block only).
    @State(Thread)
    public static class RangeAccessor
    {
        @Param({"0", "1"})
        private int range;
    }

    /// Hashes values at random positions of random blocks, as a join does with the rows of its build side.
    @Benchmark
    public long randomHashInManyBlocks(ManyBlocksData data)
    {
        long sum = 0;
        int[] blockIndexes = data.blockIndexes;
        int[] positions = data.positions;
        int start = data.nextWindow();
        if (data.layout != Layout.VARIABLE_WIDTH) {
            StridedVariableWidthBlock[] blocks = data.stridedBlocks;
            for (int i = start; i < start + POSITIONS; i++) {
                StridedVariableWidthBlock block = blocks[blockIndexes[i]];
                int position = positions[i];
                if (!block.isNull(position)) {
                    sum += XxHash64.hash(block.getRawSlice(), block.getRawSliceOffset(position), block.getSliceLength(position));
                }
            }
            return sum;
        }
        VariableWidthBlock[] blocks = data.variableWidthBlocks;
        for (int i = start; i < start + POSITIONS; i++) {
            VariableWidthBlock block = blocks[blockIndexes[i]];
            int position = positions[i];
            if (!block.isNull(position)) {
                sum += XxHash64.hash(block.getRawSlice(), block.getRawSliceOffset(position), block.getSliceLength(position));
            }
        }
        return sum;
    }

    @Benchmark
    public Object copyPositions(BenchmarkData data)
    {
        int[] selectedPositions = data.nextSelection();
        if (data.layout != Layout.VARIABLE_WIDTH) {
            return data.stridedBlock.copyPositions(selectedPositions, 0, SELECTED_POSITIONS);
        }
        return data.variableWidthBlock.copyPositions(selectedPositions, 0, SELECTED_POSITIONS);
    }

    /// Appends half of the positions of a block to a builder, as the partitioned output does for every partition.
    @Benchmark
    public Object appendPositions(BenchmarkData data)
    {
        int[] selectedPositions = data.nextSelection();
        if (data.layout != Layout.VARIABLE_WIDTH) {
            StridedVariableWidthBlockBuilder builder = new StridedVariableWidthBlockBuilder(SELECTED_POSITIONS, data.totalBytes / 2);
            data.builderHolder[0] = builder;
            builder.appendPositions(data.stridedBlock, selectedPositions, 0, SELECTED_POSITIONS);
            return builder.buildValueBlock();
        }
        VariableWidthBlockBuilder builder = new VariableWidthBlockBuilder(null, SELECTED_POSITIONS, data.totalBytes / 2);
        data.builderHolder[0] = builder;
        builder.appendPositions(data.variableWidthBlock, selectedPositions, 0, SELECTED_POSITIONS);
        return builder.buildValueBlock();
    }

    @Benchmark
    public int serialize(BenchmarkData data)
    {
        DynamicSliceOutput output = data.output;
        output.reset();
        data.serialize(output);
        return output.size();
    }

    /// Creates a block from serialized bytes that the block may keep.
    @Benchmark
    public Object deserialize(BenchmarkData data)
    {
        if (data.layout != Layout.VARIABLE_WIDTH) {
            return StridedVariableWidthBlock.wrap(data.serialized);
        }
        return data.encoding.readBlock(null, data.serialized.getInput());
    }

    /// Reads the value at every position, null or not, of the same positions in every call. Compared with
    /// [#unguardedReadOfStream], this shows how much of the cost of a null a processor hides by learning the positions.
    @Benchmark
    public long unguardedReadOfSamePositions(BenchmarkData data)
    {
        return unguardedRead(data, 0);
    }

    /// Reads the value at every position, null or not, of positions that change with every call.
    @Benchmark
    public long unguardedReadOfStream(BenchmarkData data)
    {
        return unguardedRead(data, data.nextWindow());
    }

    private static long unguardedRead(BenchmarkData data, int start)
    {
        long sum = 0;
        int[] positions = data.positionStream;
        if (data.layout != Layout.VARIABLE_WIDTH) {
            StridedVariableWidthBlock block = data.stridedBlock;
            for (int i = start; i < start + POSITIONS; i++) {
                sum += block.getSlice(positions[i]).length();
            }
            return sum;
        }
        VariableWidthBlock block = data.variableWidthBlock;
        for (int i = start; i < start + POSITIONS; i++) {
            sum += block.getSlice(positions[i]).length();
        }
        return sum;
    }

    public enum Layout
    {
        VARIABLE_WIDTH,
        STRIDED,
    }

    public enum Lengths
    {
        FIXED_10(10, 10),
        NARROW_8_TO_12(8, 12),
        SHORT_0_TO_12(0, 12),
        FIXED_36(36, 36),
        MEDIUM_13_TO_32(13, 32),
        MIXED_0_TO_64(0, 64),
        LONG_100_TO_200(100, 200);

        private final int minLength;
        private final int maxLength;

        Lengths(int minLength, int maxLength)
        {
            this.minLength = minLength;
            this.maxLength = maxLength;
        }

        /// Returns values where a null position is a null element.
        Slice[] generate(Random random, int count, int nullPercent)
        {
            Slice[] values = new Slice[count];
            for (int i = 0; i < count; i++) {
                if (random.nextInt(100) < nullPercent) {
                    continue;
                }
                byte[] bytes = new byte[minLength + random.nextInt(maxLength - minLength + 1)];
                for (int j = 0; j < bytes.length; j++) {
                    bytes[j] = (byte) ('a' + random.nextInt(26));
                }
                values[i] = Slices.wrappedBuffer(bytes);
            }
            return values;
        }
    }

    @State(Thread)
    public static class BenchmarkData
    {
        @Param
        private Layout layout = Layout.STRIDED;

        @Param
        private Lengths lengths = Lengths.NARROW_8_TO_12;

        @Param({"0", "10"})
        private int nullPercent;

        private Slice[] values;
        private int totalBytes;
        private VariableWidthBlock variableWidthBlock;
        // the same block, as the operators of a type see it
        private ValueBlock valueBlock;
        private StridedVariableWidthBlock stridedBlock;
        private VariableWidthBlock[] variableWidthRing;
        private StridedVariableWidthBlock[] stridedRing;
        private int[][] selections;
        private int[] positionStream;
        private final Object[] builderHolder = new Object[1];
        private final long[] hashOut = new long[POSITIONS];
        private final int[] filterScratch = new int[POSITIONS];
        private byte[] filterNeedleBytes;
        private int filterNeedleOffset;
        private int filterNeedleLength;
        private int nextBlock;
        private int nextSelection;
        private int window;
        private final VariableWidthBlockEncoding encoding = new VariableWidthBlockEncoding();
        private DynamicSliceOutput output;
        private Slice serialized;

        public BenchmarkData() {}

        public BenchmarkData(Layout layout, Lengths lengths, int nullPercent)
        {
            this.layout = layout;
            this.lengths = lengths;
            this.nullPercent = nullPercent;
        }

        @Setup
        public void setup()
        {
            Random random = new Random(831);
            values = lengths.generate(random, POSITIONS, nullPercent);
            totalBytes = totalBytes(values);

            if (layout != Layout.VARIABLE_WIDTH) {
                stridedBlock = (StridedVariableWidthBlock) buildBlock(layout, values, POSITIONS, totalBytes, builderHolder);
                stridedRing = new StridedVariableWidthBlock[BLOCK_RING];
            }
            else {
                variableWidthBlock = (VariableWidthBlock) buildBlock(layout, values, POSITIONS, totalBytes, builderHolder);
                valueBlock = variableWidthBlock;
                variableWidthRing = new VariableWidthBlock[BLOCK_RING];
            }
            for (int i = 0; i < BLOCK_RING; i++) {
                Slice[] ringValues = lengths.generate(random, POSITIONS, nullPercent);
                Object block = buildBlock(layout, ringValues, POSITIONS, totalBytes(ringValues), builderHolder);
                if (layout != Layout.VARIABLE_WIDTH) {
                    stridedRing[i] = (StridedVariableWidthBlock) block;
                }
                else {
                    variableWidthRing[i] = (VariableWidthBlock) block;
                }
            }

            // positions in increasing order, like the positions a filter or a partitioner selects
            selections = new int[SELECTION_RING][SELECTED_POSITIONS];
            for (int[] selection : selections) {
                int selected = 0;
                for (int position = 0; selected < SELECTED_POSITIONS; position++) {
                    int remainingPositions = POSITIONS - position;
                    if (random.nextInt(remainingPositions) < SELECTED_POSITIONS - selected) {
                        selection[selected] = position;
                        selected++;
                    }
                }
            }
            positionStream = random.ints(POSITION_STREAM, 0, POSITIONS).toArray();

            output = new DynamicSliceOutput(totalBytes + POSITIONS * Long.BYTES + 1024);
            serialize(output);
            serialized = output.slice().copy();

            Slice needle = Slices.EMPTY_SLICE;
            for (Slice value : values) {
                if (value != null) {
                    needle = value;
                    break;
                }
            }
            filterNeedleBytes = needle.byteArray();
            filterNeedleOffset = needle.byteArrayOffset();
            filterNeedleLength = needle.length();
        }

        /// Returns the start of the next [#POSITIONS] positions of the stream.
        int nextWindow()
        {
            int start = window;
            window = (start + POSITIONS) & (POSITION_STREAM - 1);
            return start;
        }

        int nextBlock()
        {
            nextBlock = (nextBlock + 1) & (BLOCK_RING - 1);
            return nextBlock;
        }

        int[] nextSelection()
        {
            nextSelection = (nextSelection + 1) & (SELECTION_RING - 1);
            return selections[nextSelection];
        }

        void serialize(DynamicSliceOutput output)
        {
            if (layout != Layout.VARIABLE_WIDTH) {
                stridedBlock.writeTo(output);
            }
            else {
                encoding.writeBlock(null, output, variableWidthBlock);
            }
        }
    }

    @State(Thread)
    public static class ManyBlocksData
    {
        @Param
        private Layout layout = Layout.STRIDED;

        @Param
        private Lengths lengths = Lengths.NARROW_8_TO_12;

        @Param({"0", "10"})
        private int nullPercent;

        private VariableWidthBlock[] variableWidthBlocks;
        private StridedVariableWidthBlock[] stridedBlocks;
        private int[] blockIndexes;
        private int[] positions;
        private int window;

        public ManyBlocksData() {}

        public ManyBlocksData(Layout layout, Lengths lengths, int nullPercent)
        {
            this.layout = layout;
            this.lengths = lengths;
            this.nullPercent = nullPercent;
        }

        @Setup
        public void setup()
        {
            setup(MAX_BLOCKS);
        }

        void setup(int maxBlocks)
        {
            Random random = new Random(831);
            // every layout gets the same number of blocks, which is what a build side of the same rows holds
            long blockBytes = (long) POSITIONS * (lengths.minLength + lengths.maxLength) / 2 + POSITIONS * 4L;
            int blockCount = (int) Math.max(2, Math.min(maxBlocks, MANY_BLOCKS_BYTES / blockBytes));
            if (layout != Layout.VARIABLE_WIDTH) {
                stridedBlocks = new StridedVariableWidthBlock[blockCount];
            }
            else {
                variableWidthBlocks = new VariableWidthBlock[blockCount];
            }
            for (int i = 0; i < blockCount; i++) {
                Slice[] values = lengths.generate(random, POSITIONS, nullPercent);
                Object block = buildBlock(layout, values, POSITIONS, totalBytes(values), new Object[1]);
                if (layout != Layout.VARIABLE_WIDTH) {
                    stridedBlocks[i] = (StridedVariableWidthBlock) block;
                }
                else {
                    variableWidthBlocks[i] = (VariableWidthBlock) block;
                }
            }
            blockIndexes = random.ints(POSITION_STREAM, 0, blockCount).toArray();
            positions = random.ints(POSITION_STREAM, 0, POSITIONS).toArray();
        }

        int nextWindow()
        {
            int start = window;
            window = (start + POSITIONS) & (POSITION_STREAM - 1);
            return start;
        }
    }

    private static Object buildBlock(Layout layout, Slice[] values, int expectedEntries, int expectedBytes, Object[] builderHolder)
    {
        if (layout != Layout.VARIABLE_WIDTH) {
            StridedVariableWidthBlockBuilder builder = new StridedVariableWidthBlockBuilder(expectedEntries, expectedBytes);
            builderHolder[0] = builder;
            for (Slice value : values) {
                if (value == null) {
                    builder.appendNull();
                }
                else {
                    builder.writeEntry(value);
                }
            }
            return builder.buildValueBlock();
        }
        VariableWidthBlockBuilder builder = new VariableWidthBlockBuilder(null, expectedEntries, expectedBytes);
        builderHolder[0] = builder;
        for (Slice value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                builder.writeEntry(value);
            }
        }
        return builder.buildValueBlock();
    }

    private static int totalBytes(Slice[] values)
    {
        int totalBytes = 0;
        for (Slice value : values) {
            if (value != null) {
                totalBytes += value.length();
            }
        }
        return totalBytes;
    }

    @Test
    public void testBenchmarks()
    {
        BenchmarkStridedVariableWidthBlock benchmark = new BenchmarkStridedVariableWidthBlock();
        for (Lengths lengths : Lengths.values()) {
            for (int nullPercent : new int[] {0, 10}) {
                BenchmarkData variableWidth = new BenchmarkData(Layout.VARIABLE_WIDTH, lengths, nullPercent);
                variableWidth.setup();
                ManyBlocksData manyVariableWidth = new ManyBlocksData(Layout.VARIABLE_WIDTH, lengths, nullPercent);
                manyVariableWidth.setup(4);
                for (Layout layout : new Layout[] {Layout.STRIDED}) {
                    BenchmarkData strided = new BenchmarkData(layout, lengths, nullPercent);
                    strided.setup();
                    // every call of a benchmark reads other positions, so both layouts are called as often
                    variableWidth.nextBlock = 0;
                    variableWidth.nextSelection = 0;
                    variableWidth.window = 0;

                    assertThat(benchmark.sequentialRead(strided)).isEqualTo(benchmark.sequentialRead(variableWidth));
                    assertThat(benchmark.randomRead(strided)).isEqualTo(benchmark.randomRead(variableWidth));
                    assertThat(benchmark.randomHash(strided)).isEqualTo(benchmark.randomHash(variableWidth));
                    variableWidth.window = 0;
                    strided.window = 0;
                    assertThat(benchmark.randomHashBatch(strided)).isEqualTo(benchmark.randomHashBatch(variableWidth));
                    assertThat(benchmark.filterEquals(strided)).isEqualTo(benchmark.filterEquals(variableWidth));
                    assertThat(benchmark.unguardedReadOfSamePositions(strided)).isEqualTo(benchmark.unguardedReadOfSamePositions(variableWidth));
                    assertThat(benchmark.unguardedReadOfStream(strided)).isEqualTo(benchmark.unguardedReadOfStream(variableWidth));
                    assertSameValues(benchmark.build(strided), benchmark.build(variableWidth));
                    assertSameValues(benchmark.copyPositions(strided), benchmark.copyPositions(variableWidth));
                    assertSameValues(benchmark.appendPositions(strided), benchmark.appendPositions(variableWidth));
                    assertSameValues(benchmark.deserialize(strided), benchmark.deserialize(variableWidth));
                    assertThat(benchmark.serialize(strided)).isGreaterThan(0);
                    assertThat(benchmark.serialize(variableWidth)).isGreaterThan(0);

                    ManyBlocksData manyStrided = new ManyBlocksData(layout, lengths, nullPercent);
                    manyStrided.setup(4);
                    manyVariableWidth.window = 0;
                    assertThat(benchmark.randomHashInManyBlocks(manyStrided)).isEqualTo(benchmark.randomHashInManyBlocks(manyVariableWidth));
                }
            }
        }
    }

    private static void assertSameValues(Object strided, Object variableWidth)
    {
        StridedVariableWidthBlock actual = (StridedVariableWidthBlock) strided;
        VariableWidthBlock expected = (VariableWidthBlock) variableWidth;
        assertThat(actual.getPositionCount()).isEqualTo(expected.getPositionCount());
        for (int position = 0; position < actual.getPositionCount(); position++) {
            assertThat(actual.isNull(position)).isEqualTo(expected.isNull(position));
            assertThat(actual.getSlice(position)).isEqualTo(expected.getSlice(position));
        }
    }

    static void main()
            throws Exception
    {
        benchmark(BenchmarkStridedVariableWidthBlock.class)
                .withOptions(optionsBuilder -> optionsBuilder.addProfiler(GCProfiler.class))
                .run();
    }
}
