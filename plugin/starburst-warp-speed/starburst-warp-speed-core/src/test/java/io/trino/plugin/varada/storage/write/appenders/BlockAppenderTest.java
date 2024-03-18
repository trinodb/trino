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
package io.trino.plugin.varada.storage.write.appenders;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.varada.TestingTxService;
import io.trino.plugin.varada.configuration.DictionaryConfiguration;
import io.trino.plugin.varada.dictionary.AttachDictionaryService;
import io.trino.plugin.varada.dictionary.DictionaryCacheService;
import io.trino.plugin.varada.dictionary.WriteDictionary;
import io.trino.plugin.varada.dispatcher.model.DictionaryKey;
import io.trino.plugin.varada.dispatcher.model.SchemaTableColumn;
import io.trino.plugin.varada.juffer.BlockPosHolder;
import io.trino.plugin.varada.juffer.BufferAllocator;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.storage.engine.StubsStorageEngineConstants;
import io.trino.plugin.varada.storage.juffers.CrcJuffer;
import io.trino.plugin.varada.storage.juffers.ExtendedJuffer;
import io.trino.plugin.varada.storage.juffers.NullWriteJuffer;
import io.trino.plugin.varada.storage.juffers.VarlenMdJuffer;
import io.trino.plugin.varada.storage.juffers.WriteJuffersWarmUpElement;
import io.trino.plugin.varada.storage.write.WarmupElementStats;
import io.trino.plugin.varada.type.TypeUtils;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.spi.block.Block;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public abstract class BlockAppenderTest
{
    WriteJuffersWarmUpElement writeJuffersWarmUpElement;
    int jufferPos;
    DictionaryKey dictionaryKey;
    DictionaryCacheService dictionaryCacheService;

    BlockAppender blockAppender;

    BufferAllocator bufferAllocator;
    StorageEngineConstants storageEngineConstants;

    @BeforeEach
    public void beforeEach()
    {
        writeJuffersWarmUpElement = mock(WriteJuffersWarmUpElement.class);
        NullWriteJuffer nullJuffer = mock(NullWriteJuffer.class);
        when(writeJuffersWarmUpElement.getNullBuffer()).thenReturn(ByteBuffer.allocate(100));
        when(nullJuffer.getWrappedBuffer()).thenReturn(ByteBuffer.allocate(100));
        when(writeJuffersWarmUpElement.getNullJuffer()).thenReturn(nullJuffer);
        ExtendedJuffer extendedJuffer = mock(ExtendedJuffer.class);
        when(writeJuffersWarmUpElement.getExtRecordBuffer()).thenReturn(ByteBuffer.allocate(100));
        when(extendedJuffer.getWrappedBuffer()).thenReturn(ByteBuffer.allocate(100));
        when(writeJuffersWarmUpElement.getExtRecordJuffer()).thenReturn(extendedJuffer);
        VarlenMdJuffer varlenMdJuffer = mock(VarlenMdJuffer.class);
        when(writeJuffersWarmUpElement.getVarlenMdBuffer()).thenReturn(IntBuffer.allocate(100));
        when(varlenMdJuffer.getWrappedBuffer()).thenReturn(IntBuffer.allocate(100));
        when(writeJuffersWarmUpElement.getVarlenMdJuffer()).thenReturn(varlenMdJuffer);
        bufferAllocator = mock(BufferAllocator.class);
        when(bufferAllocator.memorySegment2CrcBuff(any())).thenReturn(ByteBuffer.allocate(100));
        CrcJuffer crcJuffer = spy(new CrcJuffer(bufferAllocator));
        MemorySegment[] segments = new MemorySegment[10];
        for (int i = 0; i < 10; i++) {
            segments[i] = Arena.global().allocate(100);
        }
        crcJuffer.createBuffer(segments, false);
        when(crcJuffer.getWrappedBuffer()).thenReturn(ByteBuffer.allocate(100));
        when(writeJuffersWarmUpElement.getRecBuffSize()).thenReturn(100);
        when(writeJuffersWarmUpElement.getCrcBuffer()).thenReturn(ByteBuffer.allocate(100));
        when(writeJuffersWarmUpElement.getCrcJuffer()).thenReturn(crcJuffer);
        jufferPos = 0;
        dictionaryKey = new DictionaryKey(new SchemaTableColumn(new SchemaTableName("schema", "table"), "columns"), "1234", 12);
        dictionaryCacheService = new DictionaryCacheService(new DictionaryConfiguration(), TestingTxService.createMetricsManager(), mock(AttachDictionaryService.class));
        storageEngineConstants = new StubsStorageEngineConstants();
    }

    @ParameterizedTest
    @MethodSource("params")
    abstract void writeWithoutDictionary(Block block, Type blockType, WarmupElementStats expectedResult);

    @ParameterizedTest
    @MethodSource("params")
    abstract void writeWithDictionary(Block block, Type blockType, WarmupElementStats expectedResult);

    Optional<WriteDictionary> getWriteDictionary(RecTypeCode recTypeCode)
    {
        return Optional.of(dictionaryCacheService.computeWriteIfAbsent(dictionaryKey, recTypeCode));
    }

    RecTypeCode getRecTypeCode(Type blockType)
    {
        int recTypeLength = TypeUtils.getTypeLength(blockType, storageEngineConstants.getVarcharMaxLen());
        return TypeUtils.convertToRecTypeCode(blockType, recTypeLength, storageEngineConstants.getFixedLengthStringLimit());
    }

    void runTest(Block block, Type blockType, WarmupElementStats expectedResult, Optional<WriteDictionary> writeDictionary)
    {
        BlockPosHolder blockPosHolder = new BlockPosHolder(block, blockType, 0, block.getPositionCount());
        WarmupElementStats warmupElementStats = new WarmupElementStats(0, null, null);
        blockAppender.append(jufferPos, blockPosHolder, false, writeDictionary, null, warmupElementStats);
        assertThat(warmupElementStats).isEqualTo(expectedResult);
    }

    public static VariableWidthBlockBuilder buildVarcharBlockBuilder(List<Slice> values, Type type)
    {
        {
            VariableWidthBlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, values.size() + 1, values.size() + 1);
            for (Slice value : values) {
                type.writeSlice(blockBuilder, value);
            }
            return blockBuilder;
        }
    }

    protected static Slice getMaxSliceValue(List<Slice> values)
    {
        return values.stream()
                .max(Slice::compareTo)
                .orElse(null);
    }

    protected static Slice getMinSliceValue(List<Slice> values)
    {
        return values.stream()
                .min(Slice::compareTo)
                .orElse(null);
    }

    /**
     * generate a list of size @listSize each value is a random value of max length @maxLength
     */
    public static List<Slice> generateRandomSliceList(int maxLength)
    {
        String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        Random random = new Random();
        List<Slice> randomSliceList = new ArrayList<>();

        final int listSize = 10;
        for (int i = 0; i < listSize; i++) {
            int stringLength = random.nextInt(maxLength) + 1; // Ensure a non-zero length
            StringBuilder sb = new StringBuilder(stringLength);

            for (int j = 0; j < stringLength; j++) {
                int randomIndex = random.nextInt(characters.length());
                char randomChar = characters.charAt(randomIndex);
                sb.append(randomChar);
            }

            randomSliceList.add(Slices.utf8Slice(sb.toString()));
        }

        return randomSliceList;
    }
}
