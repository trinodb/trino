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
package io.trino.plugin.varada.storage;

import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.warmup.transform.BlockTransformerFactory;
import io.trino.plugin.varada.juffer.BlockPosHolder;
import io.trino.plugin.varada.juffer.BufferAllocator;
import io.trino.plugin.varada.juffer.WarmUpElementAllocationParams;
import io.trino.plugin.varada.storage.engine.StorageEngine;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.storage.engine.StubsStorageEngineConstants;
import io.trino.plugin.varada.storage.juffers.ReadJuffersWarmUpElement;
import io.trino.plugin.varada.storage.juffers.WriteJuffersWarmUpElement;
import io.trino.plugin.varada.storage.read.fill.BigIntArrayBlockFiller;
import io.trino.plugin.varada.storage.read.fill.BlockFiller;
import io.trino.plugin.varada.storage.read.fill.BooleanArrayBlockFiller;
import io.trino.plugin.varada.storage.read.fill.IntArrayBlockFiller;
import io.trino.plugin.varada.storage.write.WarmupElementStats;
import io.trino.plugin.varada.storage.write.appenders.ArrayBlockAppender;
import io.trino.plugin.varada.storage.write.appenders.VariableLengthStringBlockAppender;
import io.trino.plugin.warp.gen.constants.JbufType;
import io.trino.plugin.warp.gen.constants.QueryResultType;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.ShortBuffer;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("rawtypes")
public class ArrayReadWriteTest
{
    private StorageEngineConstants storageEngineConstants;
    private BufferAllocator bufferAllocator;
    private ArrayBlockAppender appender;
    private BlockFiller filler;
    private ReadJuffersWarmUpElement juffersWE;

    @BeforeEach
    public void before()
    {
        storageEngineConstants = new StubsStorageEngineConstants();

        ByteBuffer nullBuffer = ByteBuffer.allocate(1000);
        ByteBuffer recordBuffer = ByteBuffer.allocate(1000);
        ByteBuffer crcBuffer = ByteBuffer.allocate(1000);
        ByteBuffer extendedBuffer = ByteBuffer.allocate(1000);
        ByteBuffer chunksMapBuffer = ByteBuffer.allocate(1000);
        IntBuffer varlnmdBuffer = IntBuffer.allocate(1000);
        bufferAllocator = mock(BufferAllocator.class);
        when(bufferAllocator.ids2NullBuff(any())).thenReturn(nullBuffer);
        when(bufferAllocator.memorySegment2NullBuff(any())).thenReturn(nullBuffer);
        when(bufferAllocator.ids2RecBuff(any())).thenReturn(recordBuffer);
        when(bufferAllocator.memorySegment2RecBuff(any())).thenReturn(recordBuffer);
        when(bufferAllocator.memorySegment2CrcBuff(any())).thenReturn(crcBuffer);
        when(bufferAllocator.memorySegment2VarlenMdBuff(any())).thenReturn(varlnmdBuffer);
        when(bufferAllocator.memorySegment2ExtRecsBuff(any())).thenReturn(extendedBuffer);
        when(bufferAllocator.memorySegment2ChunksMapBuff(any())).thenReturn(chunksMapBuffer);
    }

    static Stream<Arguments> params()
    {
        return Stream.of(arguments(false), arguments(true));
    }

    @ParameterizedTest
    @MethodSource("params")
    public void testBooleanArray(boolean addNulls)
            throws IOException
    {
        ArrayType arrayType = new ArrayType(BooleanType.BOOLEAN);
        initialize(storageEngineConstants, bufferAllocator, RecTypeCode.REC_TYPE_ARRAY_BOOLEAN, arrayType);
        filler = new BooleanArrayBlockFiller(storageEngineConstants);

        boolean[][] values = new boolean[2][1];
        values[0] = new boolean[] {true, false, true};
        values[1] = new boolean[] {true, true, true, true, true};
        Block block = prepareBooleanBlock(values, arrayType, addNulls);
        BlockPosHolder blockPosHolder = new BlockPosHolder(block, arrayType, 0, 2);
        int jufferPos = 0;
        WarmupElementStats warmupElementStats = new WarmupElementStats(0, null, null);
        WarmUpElement warmUpElement = mock(WarmUpElement.class);
        when(warmUpElement.getRecTypeCode()).thenReturn(RecTypeCode.REC_TYPE_ARRAY_BOOLEAN);
        when(warmUpElement.getWarmUpType()).thenReturn(WarmUpType.WARM_UP_TYPE_DATA);
        appender.appendWithoutDictionary(jufferPos, blockPosHolder, false, warmUpElement, warmupElementStats);

        prepareBuffersForRead();

        ArrayBlock fillBlock = (ArrayBlock) filler.fillBlock(juffersWE, QueryResultType.QUERY_RESULT_TYPE_RAW, 2, RecTypeCode.REC_TYPE_ARRAY_BOOLEAN, 1, false);
        assertThat(fillBlock.getPositionCount()).isEqualTo(2);
        ByteArrayBlock rowBlock = (ByteArrayBlock) fillBlock.getUnderlyingValueBlock().getArray(block.getUnderlyingValuePosition(0));
//
        assertThat(rowBlock.getPositionCount()).isEqualTo(3 + (addNulls ? 1 : 0));
        assertThat(TinyintType.TINYINT.getByte(rowBlock, 0)).isEqualTo((byte) 1);
        assertThat(TinyintType.TINYINT.getByte(rowBlock, 1)).isEqualTo((byte) 0);
        assertThat(TinyintType.TINYINT.getByte(rowBlock, 2)).isEqualTo((byte) 1);
        assertThat(!addNulls || rowBlock.isNull(3)).isEqualTo(true);

        ByteArrayBlock row2Block = (ByteArrayBlock) fillBlock.getUnderlyingValueBlock().getArray(block.getUnderlyingValuePosition(1));
        assertThat(row2Block.getPositionCount()).isEqualTo(5 + (addNulls ? 1 : 0));
        IntStream.range(0, 5).forEach((index) -> assertThat(TinyintType.TINYINT.getByte(row2Block, index)).isEqualTo((byte) 1));
        assertThat(!addNulls || row2Block.isNull(5)).isEqualTo(true);
        assertThat(fillBlock).isNotNull();
    }

    @ParameterizedTest
    @MethodSource("params")
    public void testTimestampArray(boolean addNulls)
            throws IOException
    {
        ArrayType arrayType = new ArrayType(TimestampType.createTimestampType(5));
        initialize(storageEngineConstants, bufferAllocator, RecTypeCode.REC_TYPE_ARRAY_TIMESTAMP, arrayType);
        filler = new BigIntArrayBlockFiller(storageEngineConstants);

        long[][] values = new long[2][1];
        values[0] = new long[] {100L, 200L};
        values[1] = new long[] {1000L, 1000L, 1000L};
        Block block = prepareLongBlock(values, arrayType, addNulls);
        BlockPosHolder blockPosHolder = new BlockPosHolder(block, arrayType, 0, 2);
        int jufferPos = 0;
        WarmupElementStats warmupElementStats = new WarmupElementStats(0, null, null);
        WarmUpElement warmUpElement = mock(WarmUpElement.class);
        when(warmUpElement.getRecTypeCode()).thenReturn(RecTypeCode.REC_TYPE_ARRAY_TIMESTAMP);
        when(warmUpElement.getWarmUpType()).thenReturn(WarmUpType.WARM_UP_TYPE_DATA);
        appender.appendWithoutDictionary(jufferPos, blockPosHolder, false, warmUpElement, warmupElementStats);

        prepareBuffersForRead();

        ArrayBlock fillBlock = (ArrayBlock) filler.fillBlock(juffersWE, QueryResultType.QUERY_RESULT_TYPE_RAW, 2, RecTypeCode.REC_TYPE_ARRAY_BOOLEAN, 1, false);
        assertThat(fillBlock.getPositionCount()).isEqualTo(2);

        LongArrayBlock rowBlock = (LongArrayBlock) fillBlock.getUnderlyingValueBlock().getArray(block.getUnderlyingValuePosition(0));
        assertThat(rowBlock.getPositionCount()).isEqualTo(2 + (addNulls ? 1 : 0));
        assertThat(BigintType.BIGINT.getLong(rowBlock, 0)).isEqualTo(100L);
        assertThat(BigintType.BIGINT.getLong(rowBlock, 1)).isEqualTo(200L);
        assertThat(!addNulls || rowBlock.isNull(2)).isEqualTo(true);

        LongArrayBlock row2Block = (LongArrayBlock) fillBlock.getUnderlyingValueBlock().getArray(block.getUnderlyingValuePosition(1));
        assertThat(row2Block.getPositionCount()).isEqualTo(3 + (addNulls ? 1 : 0));
        IntStream.range(0, 3).forEach((index) -> assertThat(BigintType.BIGINT.getLong(row2Block, index)).isEqualTo(1000));
        assertThat(!addNulls || row2Block.isNull(3)).isEqualTo(true);
        assertThat(fillBlock).isNotNull();
    }

    @ParameterizedTest
    @MethodSource("params")
    public void testDateArray(boolean addNulls)
            throws IOException
    {
        ArrayType arrayType = new ArrayType(DateType.DATE);
        initialize(storageEngineConstants, bufferAllocator, RecTypeCode.REC_TYPE_ARRAY_DATE, arrayType);
        filler = new IntArrayBlockFiller(storageEngineConstants);

        int[][] values = new int[2][1];
        values[0] = new int[] {100, 200};
        values[1] = new int[] {1000, 1000, 1000};
        Block block = prepareIntBlock(values, arrayType, addNulls);
        BlockPosHolder blockPosHolder = new BlockPosHolder(block, arrayType, 0, 2);
        int jufferPos = 0;
        WarmupElementStats warmupElementStats = new WarmupElementStats(0, null, null);
        WarmUpElement warmUpElement = mock(WarmUpElement.class);
        when(warmUpElement.getRecTypeCode()).thenReturn(RecTypeCode.REC_TYPE_ARRAY_DATE);
        when(warmUpElement.getWarmUpType()).thenReturn(WarmUpType.WARM_UP_TYPE_DATA);
        appender.appendWithoutDictionary(jufferPos, blockPosHolder, false, warmUpElement, warmupElementStats);

        prepareBuffersForRead();

        ArrayBlock fillBlock = (ArrayBlock) filler.fillBlock(juffersWE, QueryResultType.QUERY_RESULT_TYPE_RAW, 2, RecTypeCode.REC_TYPE_ARRAY_DATE, 1, false);
        assertThat(fillBlock.getPositionCount()).isEqualTo(2);

        IntArrayBlock rowBlock = (IntArrayBlock) fillBlock.getUnderlyingValueBlock().getArray(block.getUnderlyingValuePosition(0));
        assertThat(rowBlock.getPositionCount()).isEqualTo(2 + (addNulls ? 1 : 0));
        assertThat(IntegerType.INTEGER.getInt(rowBlock, 0)).isEqualTo(100);
        assertThat(IntegerType.INTEGER.getInt(rowBlock, 1)).isEqualTo(200);
        assertThat(!addNulls || rowBlock.isNull(2)).isEqualTo(true);

        IntArrayBlock row2Block = (IntArrayBlock) fillBlock.getUnderlyingValueBlock().getArray(block.getUnderlyingValuePosition(1));
        assertThat(row2Block.getPositionCount()).isEqualTo(3 + (addNulls ? 1 : 0));
        IntStream.range(0, 3).forEach((index) -> assertThat(IntegerType.INTEGER.getInt(row2Block, index)).isEqualTo(1000));
        assertThat(!addNulls || row2Block.isNull(3)).isEqualTo(true);
        assertThat(fillBlock).isNotNull();
    }

    @ParameterizedTest
    @MethodSource("params")
    public void testIntegerArray(boolean addNulls)
            throws IOException
    {
        ArrayType arrayType = new ArrayType(IntegerType.INTEGER);
        initialize(storageEngineConstants, bufferAllocator, RecTypeCode.REC_TYPE_ARRAY_TIMESTAMP, arrayType);
        filler = new IntArrayBlockFiller(storageEngineConstants);

        int[][] values = new int[1][1];
        values[0] = new int[] {1000, 1000, 1000};
        Block block = prepareIntBlock(values, arrayType, addNulls);
        BlockPosHolder blockPosHolder = new BlockPosHolder(block, arrayType, 0, 1);
        int jufferPos = 0;
        WarmupElementStats warmupElementStats = new WarmupElementStats(0, null, null);
        WarmUpElement warmUpElement = mock(WarmUpElement.class);
        when(warmUpElement.getRecTypeCode()).thenReturn(RecTypeCode.REC_TYPE_ARRAY_INT);
        when(warmUpElement.getWarmUpType()).thenReturn(WarmUpType.WARM_UP_TYPE_DATA);
        appender.appendWithoutDictionary(jufferPos, blockPosHolder, false, warmUpElement, warmupElementStats);

        prepareBuffersForRead();

        ArrayBlock fillBlock = (ArrayBlock) filler.fillBlock(juffersWE, QueryResultType.QUERY_RESULT_TYPE_RAW, 1, RecTypeCode.REC_TYPE_ARRAY_BOOLEAN, 1, false);
        assertThat(fillBlock.getPositionCount()).isEqualTo(1);

        IntArrayBlock rowBlock = (IntArrayBlock) fillBlock.getUnderlyingValueBlock().getArray(block.getUnderlyingValuePosition(0));
        assertThat(rowBlock.getPositionCount()).isEqualTo(3 + (addNulls ? 1 : 0));
        IntStream.range(0, 3).forEach((index) -> assertThat(IntegerType.INTEGER.getInt(rowBlock, index)).isEqualTo(1000));
        assertThat(!addNulls || rowBlock.isNull(3)).isEqualTo(true);
        assertThat(fillBlock).isNotNull();
    }

    private void initialize(StorageEngineConstants storageEngineConstants, BufferAllocator bufferAllocator, RecTypeCode arrayTypeCode, ArrayType arrayType)
    {
        WarmUpElementAllocationParams warmUpElementAllocationParams = new WarmUpElementAllocationParams(arrayTypeCode, storageEngineConstants.getVarcharMaxLen(), 1000, 0, 1000, true, false, null);
        WriteJuffersWarmUpElement juffersWE = new WriteJuffersWarmUpElement(mock(StorageEngine.class), storageEngineConstants, bufferAllocator, new MemorySegment[JbufType.values().length], 0, warmUpElementAllocationParams);
        juffersWE.createBuffers(false);
        BlockTransformerFactory blockTransformerFactory = new BlockTransformerFactory();
        appender = new ArrayBlockAppender(blockTransformerFactory, juffersWE,
                new VariableLengthStringBlockAppender(juffersWE, storageEngineConstants, storageEngineConstants.getVarcharMaxLen(), arrayType, storageEngineConstants.getVarcharMaxLen()),
                arrayType);
        this.juffersWE = new ReadJuffersWarmUpElement(bufferAllocator, true, false);
        this.juffersWE.createBuffers(arrayTypeCode, storageEngineConstants.getVarcharMaxLen(), false, new long[JbufType.values().length]);
    }

    private void prepareBuffersForRead()
    {
        // prepare buffers for loop
        ByteBuffer recBuff = (ByteBuffer) juffersWE.getRecordBuffer();
        recBuff.position(0);
        ByteBuffer nullBuff = juffersWE.getNullBuffer();
        nullBuff.position(0);
        ShortBuffer lenBuff = nullBuff.asShortBuffer();

        // set the lengths in the null buffer and fix the reocrd buffer to be without the lengths
        // NOTE: this is probably the ugliest code ever, but its here since this test assumes wrongly the record/null buffer have
        // the same format in read and write flows
        int recPos = 0;
        int newRecPos = 0;
        int lenPos = 0;
        int length = Byte.toUnsignedInt(recBuff.get(recPos));
        while (length > 0) {
            lenBuff.put(lenPos, (short) length);
            lenPos++;

            recPos++; // for previously reading the length
            for (int i = 0; i < length; i++) {
                recBuff.put(newRecPos, recBuff.get(recPos));
                recPos++;
                newRecPos++;
            }

            length = Byte.toUnsignedInt(recBuff.get(recPos));
        }
        // end of ugly code

        // reset all buffers
        recBuff.position(0);
        nullBuff.position(0);
    }

    private Block prepareBooleanBlock(boolean[][] values, ArrayType arrayType, boolean addNull)
    {
        ArrayBlockBuilder output = arrayType.createBlockBuilder(null, values.length);

        for (boolean[] value : values) {
            byte[] rowValues = new byte[value.length + (addNull ? 1 : 0)];
            boolean[] valueIsNull = new boolean[value.length + (addNull ? 1 : 0)];
            if (addNull) {
                valueIsNull[value.length] = true;
            }
            for (int index = 0; index < value.length; index++) {
                rowValues[index] = value[index] ? (byte) 1 : (byte) 0;
            }
            ByteArrayBlock rowBlock = new ByteArrayBlock(rowValues.length, addNull ? Optional.of(valueIsNull) : Optional.empty(), rowValues);
            arrayType.writeObject(output, rowBlock);
        }
        return output.build();
    }

    private Block prepareLongBlock(long[][] values, ArrayType arrayType, boolean addNull)
    {
        ArrayBlockBuilder output = arrayType.createBlockBuilder(null, values.length);

        for (long[] value : values) {
            int newLength = value.length + (addNull ? 1 : 0);
            long[] rowValues = Arrays.copyOf(value, newLength);
            boolean[] valueIsNull = new boolean[newLength];
            if (addNull) {
                valueIsNull[value.length] = true;
            }
            LongArrayBlock rowBlock = new LongArrayBlock(newLength, addNull ? Optional.of(valueIsNull) : Optional.empty(), rowValues);
            arrayType.writeObject(output, rowBlock);
        }
        return output.build();
    }

    private Block prepareIntBlock(int[][] values, ArrayType arrayType, boolean addNull)
    {
        ArrayBlockBuilder output = arrayType.createBlockBuilder(null, values.length);

        for (int[] value : values) {
            int newLength = value.length + (addNull ? 1 : 0);
            int[] rowValues = Arrays.copyOf(value, newLength);
            boolean[] valueIsNull = new boolean[newLength];
            if (addNull) {
                valueIsNull[value.length] = true;
            }
            IntArrayBlock rowBlock = new IntArrayBlock(newLength, addNull ? Optional.of(valueIsNull) : Optional.empty(), rowValues);
            arrayType.writeObject(output, rowBlock);
        }
        return output.build();
    }
}
