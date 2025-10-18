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
package io.trino.lance.file.v2.reader;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.lance.file.LanceDataSource;
import io.trino.lance.file.v2.encoding.BlockDecoder;
import io.trino.lance.file.v2.encoding.LanceEncoding;
import io.trino.lance.file.v2.encoding.MiniBlockDecoder;
import io.trino.lance.file.v2.metadata.DiskRange;
import io.trino.lance.file.v2.metadata.MiniBlockLayout;
import io.trino.lance.file.v2.metadata.RepDefLayer;
import io.trino.lance.file.v2.reader.RepetitionIndex.RepIndexBlock;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.spi.block.Block;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.lance.file.v2.metadata.RepDefLayer.NULLABLE_ITEM;
import static io.trino.lance.file.v2.reader.IntArrayBufferAdapter.INT_ARRAY_BUFFER_ADAPTER;
import static java.lang.Math.toIntExact;

public class MiniBlockPageReader
        implements PageReader
{
    public static final int MINIBLOCK_ALIGNMENT = 8;

    private final LanceDataSource dataSource;
    private final Optional<LanceEncoding> repetitionEncoding;
    private final Optional<LanceEncoding> definitionEncoding;
    private final Optional<LanceEncoding> dictionaryEncoding;
    private final Optional<Long> numDictionaryItems;
    private final Optional<Block> dictionaryBlock;
    private final LanceEncoding valueEncoding;
    private final int repetitionIndexDepth;
    private final List<RepIndexBlock> repetitionIndex;
    private final List<RepDefLayer> layers;
    private final int maxVisibleDefinition;
    private final long numBuffers;
    private final long numRows;
    private final List<ChunkMetadata> chunks;
    private final BufferAdapter valueBufferAdapter;
    private final DataValuesBuffer valuesBuffer;
    private final DataValuesBuffer<int[]> repetitionBuffer;
    private final DataValuesBuffer<int[]> definitionBuffer;
    // memory usage for current miniblock page
    private final LocalMemoryContext memoryUsage;

    private long levelOffset;

    public MiniBlockPageReader(LanceDataSource dataSource,
            Type type,
            MiniBlockLayout layout,
            List<DiskRange> bufferOffsets,
            long numRows,
            AggregatedMemoryContext memoryContext)
    {
        this.dataSource = dataSource;
        this.repetitionEncoding = layout.repetitionEncoding();
        this.definitionEncoding = layout.definitionEncoding();
        this.dictionaryEncoding = layout.dictionaryEncoding();
        this.numDictionaryItems = layout.numDictionaryItems();
        this.valueEncoding = layout.valueEncoding();
        this.repetitionIndexDepth = layout.repIndexDepth();
        this.layers = layout.layers();
        this.maxVisibleDefinition = layers.stream().takeWhile(layer -> !layer.isList()).mapToInt(RepDefLayer::numDefLevels).sum();
        this.numBuffers = layout.numBuffers();
        this.numRows = numRows;
        try {
            // build chunk meta
            // bufferOffsets[0] = chunk metadata buffer
            // bufferOffset[1] = value buffer
            DiskRange chunkMetadataBuf = bufferOffsets.get(0);
            DiskRange valueBuf = bufferOffsets.get(1);
            Slice chunkMetadataSlice = dataSource.readFully(chunkMetadataBuf.position(), toIntExact(chunkMetadataBuf.length()));
            int numWords = chunkMetadataSlice.length() / 2;
            ImmutableList.Builder<ChunkMetadata> chunkMetadataBuilder = ImmutableList.builder();
            long count = 0;
            long offset = valueBuf.position();
            for (int i = 0; i < numWords; i++) {
                int word = chunkMetadataSlice.getUnsignedShort(i * 2);
                int logNumValues = word & 0xF;
                int dividedBytes = word >>> 4;
                int chunkSizeBytes = (dividedBytes + 1) * MINIBLOCK_ALIGNMENT;
                long numValues = i < numWords - 1 ? 1 << logNumValues : layout.numItems() - count;
                count += numValues;

                chunkMetadataBuilder.add(new ChunkMetadata(numValues, chunkSizeBytes, offset));
                offset += chunkSizeBytes;
            }
            this.chunks = chunkMetadataBuilder.build();
            // load dictionary
            if (dictionaryEncoding.isPresent()) {
                DiskRange dictionaryRange = bufferOffsets.get(2);
                Slice dictionarySlice = dataSource.readFully(dictionaryRange.position(), toIntExact(dictionaryRange.length()));
                ValueBlock dictionary = dictionaryEncoding.get().decodeBlock(dictionarySlice, toIntExact(numDictionaryItems.get()));
                // if a block is nullable, we append null to the end of dictionary
                if (layers.stream().anyMatch(layer -> (layer == NULLABLE_ITEM))) {
                    dictionaryBlock = Optional.of(dictionary.copyWithAppendedNull());
                }
                else {
                    dictionaryBlock = Optional.of(dictionary);
                }
            }
            else {
                dictionaryBlock = Optional.empty();
            }
            // load repetition index
            if (repetitionIndexDepth > 0) {
                DiskRange repetitionIndexRange = bufferOffsets.getLast();
                verify(repetitionIndexRange.length() % 8 == 0);
                Slice repetitionIndexSlice = dataSource.readFully(repetitionIndexRange.position(), toIntExact(repetitionIndexRange.length()));
                repetitionIndex = RepetitionIndex.from(repetitionIndexSlice, repetitionIndexDepth);
            }
            else {
                repetitionIndex = RepetitionIndex.defaultIndex(chunks);
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        valueBufferAdapter = valueEncoding.getBufferAdapter();
        valuesBuffer = new DataValuesBuffer(valueBufferAdapter);
        repetitionBuffer = new DataValuesBuffer<>(INT_ARRAY_BUFFER_ADAPTER);
        definitionBuffer = new DataValuesBuffer<>(INT_ARRAY_BUFFER_ADAPTER);
        memoryUsage = memoryContext.newLocalMemoryContext(MiniBlockPageReader.class.getSimpleName());

        levelOffset = 0;
    }

    public static int padding(int n)
    {
        return (MINIBLOCK_ALIGNMENT - (n & (MINIBLOCK_ALIGNMENT - 1))) & (MINIBLOCK_ALIGNMENT - 1);
    }

    @Override
    public DecodedPage decodeRanges(List<Range> ranges)
    {
        valuesBuffer.reset();
        repetitionBuffer.reset();
        definitionBuffer.reset();
        levelOffset = 0;

        for (Range range : ranges) {
            long rowsNeeded = range.length();
            boolean needPreamble = false;

            // find first chunk that has row >= range.start
            int blockIndex = Collections.binarySearch(repetitionIndex, range.start(), (block, key) -> Long.compare(((RepIndexBlock) block).firstRow(), (long) key));
            if (blockIndex >= 0) {
                while (blockIndex > 0 && repetitionIndex.get(blockIndex - 1).firstRow() == range.start()) {
                    blockIndex--;
                }
            }
            else {
                blockIndex = -(blockIndex + 1) - 1;
            }

            long toSkip = range.start() - repetitionIndex.get(blockIndex).firstRow();
            while (rowsNeeded > 0 || needPreamble) {
                RepIndexBlock chunkIndexBlock = repetitionIndex.get(blockIndex);
                long rowsAvailable = chunkIndexBlock.startCount() - toSkip;

                // handle preamble only blocks (rowsAvailable == 0)
                if (rowsAvailable == 0 && toSkip == 0) {
                    if (chunkIndexBlock.hasPreamble() && needPreamble) {
                        readChunk(chunks.get(blockIndex), chunkIndexBlock, toSkip, 0, PreambleAction.TAKE, false);
                        if (chunkIndexBlock.startCount() > 0 || blockIndex == repetitionIndex.size() - 1) {
                            needPreamble = false;
                        }
                    }
                    blockIndex++;
                    continue;
                }

                if (rowsAvailable == 0 && toSkip > 0) {
                    toSkip -= chunkIndexBlock.startCount();
                    blockIndex++;
                    continue;
                }

                long rowsToTake = Math.min(rowsNeeded, rowsAvailable);
                rowsNeeded -= rowsToTake;

                boolean takeTrailer = false;
                PreambleAction preambleAction;
                if (chunkIndexBlock.hasPreamble()) {
                    if (needPreamble) {
                        preambleAction = PreambleAction.TAKE;
                    }
                    else {
                        preambleAction = PreambleAction.SKIP;
                    }
                }
                else {
                    preambleAction = PreambleAction.ABSENT;
                }
                long fullRowsToTake = rowsToTake;

                if (rowsToTake == rowsAvailable && chunkIndexBlock.hasTrailer()) {
                    takeTrailer = true;
                    needPreamble = true;
                    fullRowsToTake--;
                }
                else {
                    needPreamble = false;
                }
                readChunk(chunks.get(blockIndex), chunkIndexBlock, toSkip, fullRowsToTake, preambleAction, takeTrailer);

                toSkip = 0;
                blockIndex++;
            }
        }
        memoryUsage.setBytes(getRetainedBytes());
        return valuesBuffer.createDecodedPage(definitionBuffer.getMergedValues(), repetitionBuffer.getMergedValues(), layers, dictionaryBlock);
    }

    private long getRetainedBytes()
    {
        long retainedBytes = 0;
        if (dictionaryBlock.isPresent()) {
            retainedBytes += dictionaryBlock.get().getRetainedSizeInBytes();
        }
        retainedBytes += valuesBuffer.getRetainedBytes() + repetitionBuffer.getRetainedBytes() + definitionBuffer.getRetainedBytes();
        return retainedBytes;
    }

    public static SelectedRanges mapRange(Range rowRange, int[] rep, int[] def, int maxRepetitionLevel, int maxVisibleDefinition, int numItems, PreambleAction preambleAction)
    {
        if (rep == null) {
            // if there is no repetition, item and level range are the same as row range
            return new SelectedRanges(rowRange, rowRange);
        }

        int itemsInPreamble = 0;
        int firstRowStart = -1;
        switch (preambleAction) {
            case SKIP, TAKE: {
                if (def != null) {
                    for (int i = 0; i < rep.length; i++) {
                        if (rep[i] == maxRepetitionLevel) {
                            firstRowStart = i;
                            break;
                        }
                        if (def[i] <= maxVisibleDefinition) {
                            itemsInPreamble++;
                        }
                    }
                }
                else {
                    for (int i = 0; i < rep.length; i++) {
                        if (rep[i] == maxRepetitionLevel) {
                            firstRowStart = i;
                            break;
                        }
                    }
                    itemsInPreamble = Math.min(firstRowStart, rep.length);
                }
                // chunk is entirely preamble
                if (firstRowStart == -1) {
                    return new SelectedRanges(new Range(0, numItems), new Range(0, rep.length));
                }
                break;
            }
            case ABSENT: {
                firstRowStart = 0;
                break;
            }
        }

        // handle preamble only blocks
        if (rowRange.start() == rowRange.end()) {
            return new SelectedRanges(new Range(0, itemsInPreamble), new Range(0, firstRowStart));
        }

        // we are reading at least 1 full row if we reach here
        verify(rowRange.length() > 0);
        int rowsSeen = 0;
        int newStart = firstRowStart;
        int newLevelsStart = firstRowStart;

        if (def != null) {
            long leadInvisSeen = 0;
            if (rowRange.start() > 0) {
                if (def[firstRowStart] > maxVisibleDefinition) {
                    leadInvisSeen += 1;
                }
                for (int i = firstRowStart + 1; i < def.length; i++) {
                    if (rep[i] == maxRepetitionLevel) {
                        rowsSeen++;
                        if (rowsSeen == rowRange.start()) {
                            newStart = i - toIntExact(leadInvisSeen);
                            newLevelsStart = i;
                            break;
                        }
                        if (def[i] > maxVisibleDefinition) {
                            leadInvisSeen++;
                        }
                    }
                }
            }

            rowsSeen++;
            long newEnd = Long.MAX_VALUE;
            long newLevelsEnd = rep.length;
            long trailInvisSeen = def[newLevelsStart] > maxVisibleDefinition ? 1 : 0;

            for (int i = newLevelsStart + 1; i < def.length; i++) {
                int repLevel = rep[i];
                int defLevel = def[i];
                if (repLevel == maxRepetitionLevel) {
                    rowsSeen++;
                    if (rowsSeen == rowRange.end() + 1) {
                        newEnd = i - leadInvisSeen - trailInvisSeen;
                        newLevelsEnd = i;
                        break;
                    }
                    if (defLevel > maxVisibleDefinition) {
                        trailInvisSeen++;
                    }
                }
            }
            if (newEnd == Long.MAX_VALUE) {
                newLevelsEnd = rep.length;
                newEnd = rep.length - (leadInvisSeen + trailInvisSeen);
            }
            verify(newEnd != Long.MAX_VALUE);
            if (preambleAction == PreambleAction.TAKE) {
                newLevelsStart = 0;
                newStart = 0;
            }

            return new SelectedRanges(new Range(newStart, newEnd), new Range(newLevelsStart, newLevelsEnd));
        }
        else {
            if (rowRange.start() > 0) {
                for (int i = firstRowStart + 1; i < rep.length; i++) {
                    if (rep[i] == maxRepetitionLevel) {
                        rowsSeen++;
                        if (rowsSeen == rowRange.start()) {
                            newStart = i;
                            break;
                        }
                    }
                }
            }
            long end = rep.length;
            if (rowRange.end() < numItems) {
                for (int i = toIntExact(firstRowStart + newStart + 1); i < rep.length; i++) {
                    if (rep[i] == maxRepetitionLevel) {
                        rowsSeen++;
                        if (rowsSeen == rowRange.end()) {
                            end = i;
                            break;
                        }
                    }
                }
            }
            if (preambleAction == PreambleAction.TAKE) {
                newStart = 0;
            }
            return new SelectedRanges(new Range(newStart, end), new Range(newStart, end));
        }
    }

    public void readChunk(ChunkMetadata chunk, RepIndexBlock chunkIndex, long rowsToSkip, long rowsToTake, PreambleAction preambleAction, boolean takeTrailer)
    {
        try {
            ChunkReader chunkReader = new ChunkReader(dataSource.readFully(chunk.offsetBytes(), toIntExact(chunk.chunkSizeBytes())), toIntExact(chunk.numValues()), valueBufferAdapter);

            SelectedRanges selectedRanges = mapRange(new Range(rowsToSkip, rowsToSkip + rowsToTake + (takeTrailer ? 1 : 0)),
                    chunkReader.readRepetitionLevels(),
                    chunkReader.readDefinitionLevels(),
                    toIntExact(layers.stream().filter(RepDefLayer::isList).count()),
                    maxVisibleDefinition,
                    chunkReader.getNumValues(),
                    preambleAction);
            Range itemRange = selectedRanges.itemRange();
            Range levelRange = selectedRanges.levelRange();

            if (!repetitionEncoding.isEmpty()) {
                if (repetitionBuffer.isEmpty() && levelOffset > 0) {
                    repetitionBuffer.append(new int[toIntExact(levelOffset)]);
                }
                repetitionBuffer.append(Arrays.copyOfRange(chunkReader.readRepetitionLevels(), toIntExact(levelRange.start()), toIntExact(levelRange.end())));
            }

            if (!definitionEncoding.isEmpty()) {
                if (definitionBuffer.isEmpty() && levelOffset > 0) {
                    definitionBuffer.append(new int[toIntExact(levelOffset)]);
                }
                definitionBuffer.append(Arrays.copyOfRange(chunkReader.readDefinitionLevels(), toIntExact(levelRange.start()), toIntExact(levelRange.end())));
            }

            levelOffset += levelRange.length();
            chunkReader.readValues(toIntExact(itemRange.start()), toIntExact(itemRange.length()));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private int[] loadLevels(LanceEncoding encoding, Slice slice, int numLevels)
    {
        int[] levels = new int[numLevels];
        BlockDecoder<short[]> levelsDecoder = encoding.getBlockDecoder();
        short[] buffer = new short[numLevels];
        levelsDecoder.init(slice, numLevels);
        levelsDecoder.read(0, buffer, 0, numLevels);
        for (int i = 0; i < numLevels; i++) {
            levels[i] = buffer[i] & 0xFFFF;
        }
        return levels;
    }

    public long getNumRows()
    {
        return numRows;
    }

    public enum PreambleAction
    {
        TAKE, SKIP, ABSENT
    }

    public record SelectedRanges(Range itemRange, Range levelRange)
    {
        @Override
        public int hashCode()
        {
            return Objects.hash(itemRange, levelRange);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            SelectedRanges other = (SelectedRanges) obj;
            return Objects.equals(itemRange, other.itemRange) && Objects.equals(levelRange, other.levelRange);
        }
    }

    public class ChunkReader<T>
    {
        private final BufferAdapter<T> bufferAdapter;
        private final List<Slice> buffers;
        private final MiniBlockDecoder<T> valueDecoder;
        private final int numValues;
        private final int[] repetitions;
        private final int[] definitions;

        public ChunkReader(Slice chunk, int numValues, BufferAdapter<T> bufferAdapter)
        {
            this.bufferAdapter = bufferAdapter;
            this.numValues = numValues;
            // decode header
            int offset = 0;
            int numLevels = chunk.getUnsignedShort(offset);
            offset += 2;

            int repetitionSize = 0;
            if (repetitionEncoding.isPresent()) {
                repetitionSize = toIntExact(chunk.getUnsignedShort(offset));
                offset += 2;
            }

            int definitionSize = 0;
            if (definitionEncoding.isPresent()) {
                definitionSize = toIntExact(chunk.getUnsignedShort(offset));
                offset += 2;
            }

            int[] bufferSizes = new int[toIntExact(numBuffers)];
            for (int i = 0; i < numBuffers; i++) {
                bufferSizes[i] = chunk.getUnsignedShort(offset);
                offset += 2;
            }
            offset += padding(offset);

            // load repetition/definition levels
            if (repetitionEncoding.isPresent()) {
                repetitions = loadLevels(repetitionEncoding.get(), chunk.slice(offset, repetitionSize), numLevels);
                offset += repetitionSize;
                offset += padding(offset);
            }
            else {
                repetitions = null;
            }
            if (definitionEncoding.isPresent()) {
                definitions = loadLevels(definitionEncoding.get(), chunk.slice(offset, definitionSize), numLevels);
                offset += definitionSize;
                offset += padding(offset);
            }
            else {
                definitions = null;
            }

            // load data buffers
            ImmutableList.Builder<Slice> builder = ImmutableList.builder();
            for (int i = 0; i < bufferSizes.length; i++) {
                int size = bufferSizes[i];
                builder.add(chunk.slice(offset, size));
                offset += size;
                offset += padding(offset);
            }
            buffers = builder.build();
            valueDecoder = valueEncoding.getMiniBlockDecoder();
            valueDecoder.init(buffers, numValues);
        }

        public void readValues(int offset, int count)
        {
            T output = bufferAdapter.createBuffer(count);
            valueDecoder.read(offset, output, 0, count);
            valuesBuffer.append(output);
        }

        public int[] readDefinitionLevels()
        {
            return definitions;
        }

        public int[] readRepetitionLevels()
        {
            return repetitions;
        }

        public int getNumValues()
        {
            return numValues;
        }
    }
}
