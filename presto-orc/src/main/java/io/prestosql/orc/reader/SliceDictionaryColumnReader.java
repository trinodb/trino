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
package io.prestosql.orc.reader;

import io.airlift.slice.Slice;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.orc.OrcColumn;
import io.prestosql.orc.OrcCorruptionException;
import io.prestosql.orc.metadata.ColumnEncoding;
import io.prestosql.orc.metadata.ColumnMetadata;
import io.prestosql.orc.stream.BooleanInputStream;
import io.prestosql.orc.stream.ByteArrayInputStream;
import io.prestosql.orc.stream.InputStreamSource;
import io.prestosql.orc.stream.InputStreamSources;
import io.prestosql.orc.stream.LongInputStream;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.DictionaryBlock;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.block.VariableWidthBlock;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.prestosql.orc.metadata.Stream.StreamKind.DATA;
import static io.prestosql.orc.metadata.Stream.StreamKind.DICTIONARY_DATA;
import static io.prestosql.orc.metadata.Stream.StreamKind.LENGTH;
import static io.prestosql.orc.metadata.Stream.StreamKind.PRESENT;
import static io.prestosql.orc.reader.ReaderUtils.minNonNullValueSize;
import static io.prestosql.orc.reader.SliceColumnReader.computeTruncatedLength;
import static io.prestosql.orc.stream.MissingInputStreamSource.missingStreamSource;
import static java.lang.Math.toIntExact;
import static java.util.Arrays.fill;
import static java.util.Objects.requireNonNull;

public class SliceDictionaryColumnReader
        implements ColumnReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SliceDictionaryColumnReader.class).instanceSize();

    private static final byte[] EMPTY_DICTIONARY_DATA = new byte[0];
    // add one extra entry for null after strip/rowGroup dictionary
    private static final int[] EMPTY_DICTIONARY_OFFSETS = new int[2];

    private final OrcColumn column;
    private final int maxCodePointCount;
    private final boolean isCharType;

    private int readOffset;
    private int nextBatchSize;

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    @Nullable
    private BooleanInputStream presentStream;

    private InputStreamSource<ByteArrayInputStream> dictionaryDataStreamSource = missingStreamSource(ByteArrayInputStream.class);
    private boolean dictionaryOpen;
    private int dictionarySize;
    private int[] dictionaryLength = new int[0];
    private byte[] dictionaryData = EMPTY_DICTIONARY_DATA;
    private int[] dictionaryOffsetVector = EMPTY_DICTIONARY_OFFSETS;

    private VariableWidthBlock dictionaryBlock = new VariableWidthBlock(1, wrappedBuffer(EMPTY_DICTIONARY_DATA), EMPTY_DICTIONARY_OFFSETS, Optional.of(new boolean[] {true}));
    private byte[] currentDictionaryData = EMPTY_DICTIONARY_DATA;

    private InputStreamSource<LongInputStream> dictionaryLengthStreamSource = missingStreamSource(LongInputStream.class);

    private InputStreamSource<LongInputStream> dataStreamSource = missingStreamSource(LongInputStream.class);
    @Nullable
    private LongInputStream dataStream;

    private boolean rowGroupOpen;

    private int[] nonNullValueTemp = new int[0];
    private int[] nonNullPositionList = new int[0];

    private final LocalMemoryContext systemMemoryContext;

    public SliceDictionaryColumnReader(OrcColumn column, LocalMemoryContext systemMemoryContext, int maxCodePointCount, boolean isCharType)
    {
        this.maxCodePointCount = maxCodePointCount;
        this.isCharType = isCharType;
        this.column = requireNonNull(column, "column is null");
        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
    }

    @Override
    public void prepareNextRead(int batchSize)
    {
        readOffset += nextBatchSize;
        nextBatchSize = batchSize;
    }

    @Override
    public Block readBlock()
            throws IOException
    {
        if (!rowGroupOpen) {
            openRowGroup();
        }

        if (readOffset > 0) {
            if (presentStream != null) {
                // skip ahead the present bit reader, but count the set bits
                // and use this as the skip size for the length reader
                readOffset = presentStream.countBitsSet(readOffset);
            }
            if (readOffset > 0) {
                if (dataStream == null) {
                    throw new OrcCorruptionException(column.getOrcDataSourceId(), "Value is not null but data stream is missing");
                }
                dataStream.skip(readOffset);
            }
        }

        Block block;
        if (dataStream == null) {
            if (presentStream == null) {
                throw new OrcCorruptionException(column.getOrcDataSourceId(), "Value is null but present stream is missing");
            }
            presentStream.skip(nextBatchSize);
            block = readAllNullsBlock();
        }
        else if (presentStream == null) {
            block = readNonNullBlock();
        }
        else {
            boolean[] isNull = new boolean[nextBatchSize];
            int nullCount = presentStream.getUnsetBits(nextBatchSize, isNull);
            if (nullCount == 0) {
                block = readNonNullBlock();
            }
            else if (nullCount != nextBatchSize) {
                block = readNullBlock(isNull, nextBatchSize - nullCount);
            }
            else {
                block = readAllNullsBlock();
            }
        }

        readOffset = 0;
        nextBatchSize = 0;
        return block;
    }

    private RunLengthEncodedBlock readAllNullsBlock()
    {
        return new RunLengthEncodedBlock(new VariableWidthBlock(1, EMPTY_SLICE, new int[2], Optional.of(new boolean[] {true})), nextBatchSize);
    }

    private Block readNonNullBlock()
            throws IOException
    {
        verifyNotNull(dataStream);
        int[] values = new int[nextBatchSize];
        dataStream.next(values, nextBatchSize);
        return new DictionaryBlock(nextBatchSize, dictionaryBlock, values);
    }

    private Block readNullBlock(boolean[] isNull, int nonNullCount)
            throws IOException
    {
        verifyNotNull(dataStream);
        int minNonNullValueSize = minNonNullValueSize(nonNullCount);
        if (nonNullValueTemp.length < minNonNullValueSize) {
            nonNullValueTemp = new int[minNonNullValueSize];
            nonNullPositionList = new int[minNonNullValueSize];
            systemMemoryContext.setBytes(sizeOf(nonNullValueTemp) + sizeOf(nonNullPositionList));
        }

        dataStream.next(nonNullValueTemp, nonNullCount);

        int nonNullPosition = 0;
        for (int i = 0; i < isNull.length; i++) {
            nonNullPositionList[nonNullPosition] = i;
            if (!isNull[i]) {
                nonNullPosition++;
            }
        }

        int[] result = new int[isNull.length];
        fill(result, dictionarySize);

        for (int i = 0; i < nonNullPosition; i++) {
            result[nonNullPositionList[i]] = nonNullValueTemp[i];
        }

        return new DictionaryBlock(nextBatchSize, dictionaryBlock, result);
    }

    private void setDictionaryBlockData(byte[] dictionaryData, int[] dictionaryOffsets, int positionCount)
    {
        verify(positionCount > 0);
        // only update the block if the array changed to prevent creation of new Block objects, since
        // the engine currently uses identity equality to test if dictionaries are the same
        if (currentDictionaryData != dictionaryData) {
            boolean[] isNullVector = new boolean[positionCount];
            isNullVector[positionCount - 1] = true;
            dictionaryOffsets[positionCount] = dictionaryOffsets[positionCount - 1];
            dictionaryBlock = new VariableWidthBlock(positionCount, wrappedBuffer(dictionaryData), dictionaryOffsets, Optional.of(isNullVector));
            currentDictionaryData = dictionaryData;
        }
    }

    private void openRowGroup()
            throws IOException
    {
        // read the dictionary
        if (!dictionaryOpen) {
            if (dictionarySize > 0) {
                // resize the dictionary lengths array if necessary
                if (dictionaryLength.length < dictionarySize) {
                    dictionaryLength = new int[dictionarySize];
                }

                // read the lengths
                LongInputStream lengthStream = dictionaryLengthStreamSource.openStream();
                if (lengthStream == null) {
                    throw new OrcCorruptionException(column.getOrcDataSourceId(), "Dictionary is not empty but dictionary length stream is missing");
                }
                lengthStream.next(dictionaryLength, dictionarySize);

                long dataLength = 0;
                for (int i = 0; i < dictionarySize; i++) {
                    dataLength += dictionaryLength[i];
                }

                // we must always create a new dictionary array because the previous dictionary may still be referenced
                dictionaryData = new byte[toIntExact(dataLength)];
                // add one extra entry for null
                dictionaryOffsetVector = new int[dictionarySize + 2];

                // read dictionary values
                ByteArrayInputStream dictionaryDataStream = dictionaryDataStreamSource.openStream();
                readDictionary(dictionaryDataStream, dictionarySize, dictionaryLength, 0, dictionaryData, dictionaryOffsetVector, maxCodePointCount, isCharType);
            }
            else {
                dictionaryData = EMPTY_DICTIONARY_DATA;
                dictionaryOffsetVector = EMPTY_DICTIONARY_OFFSETS;
            }
        }
        dictionaryOpen = true;

        setDictionaryBlockData(dictionaryData, dictionaryOffsetVector, dictionarySize + 1);

        presentStream = presentStreamSource.openStream();
        dataStream = dataStreamSource.openStream();

        rowGroupOpen = true;
    }

    // Reads dictionary into data and offsetVector
    private static void readDictionary(
            ByteArrayInputStream dictionaryDataStream,
            int dictionarySize,
            int[] dictionaryLengthVector,
            int offsetVectorOffset,
            byte[] data,
            int[] offsetVector,
            int maxCodePointCount,
            boolean isCharType)
            throws IOException
    {
        Slice slice = wrappedBuffer(data);

        // initialize the offset if necessary;
        // otherwise, use the previous offset
        if (offsetVectorOffset == 0) {
            offsetVector[0] = 0;
        }

        // truncate string and update offsets
        for (int i = 0; i < dictionarySize; i++) {
            int offsetIndex = offsetVectorOffset + i;
            int offset = offsetVector[offsetIndex];
            int length = dictionaryLengthVector[i];

            int truncatedLength;
            if (length > 0) {
                // read data without truncation
                dictionaryDataStream.next(data, offset, offset + length);

                // adjust offsets with truncated length
                truncatedLength = computeTruncatedLength(slice, offset, length, maxCodePointCount, isCharType);
                verify(truncatedLength >= 0);
            }
            else {
                truncatedLength = 0;
            }
            offsetVector[offsetIndex + 1] = offsetVector[offsetIndex] + truncatedLength;
        }
    }

    @Override
    public void startStripe(ZoneId timeZone, InputStreamSources dictionaryStreamSources, ColumnMetadata<ColumnEncoding> encoding)
    {
        dictionaryDataStreamSource = dictionaryStreamSources.getInputStreamSource(column, DICTIONARY_DATA, ByteArrayInputStream.class);
        dictionaryLengthStreamSource = dictionaryStreamSources.getInputStreamSource(column, LENGTH, LongInputStream.class);
        dictionarySize = encoding.get(column.getColumnId()).getDictionarySize();
        dictionaryOpen = false;

        presentStreamSource = missingStreamSource(BooleanInputStream.class);
        dataStreamSource = missingStreamSource(LongInputStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(column, PRESENT, BooleanInputStream.class);
        dataStreamSource = dataStreamSources.getInputStreamSource(column, DATA, LongInputStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(column)
                .toString();
    }

    @Override
    public void close()
    {
        systemMemoryContext.close();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE;
    }
}
