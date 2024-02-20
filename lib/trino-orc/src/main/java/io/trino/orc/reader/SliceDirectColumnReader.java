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
package io.trino.orc.reader;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.orc.OrcColumn;
import io.trino.orc.OrcCorruptionException;
import io.trino.orc.metadata.ColumnEncoding;
import io.trino.orc.metadata.ColumnMetadata;
import io.trino.orc.stream.BooleanInputStream;
import io.trino.orc.stream.ByteArrayInputStream;
import io.trino.orc.stream.InputStreamSource;
import io.trino.orc.stream.InputStreamSources;
import io.trino.orc.stream.LongInputStream;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.VariableWidthBlock;
import jakarta.annotation.Nullable;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.trino.orc.metadata.Stream.StreamKind.DATA;
import static io.trino.orc.metadata.Stream.StreamKind.LENGTH;
import static io.trino.orc.metadata.Stream.StreamKind.PRESENT;
import static io.trino.orc.reader.ReaderUtils.convertLengthVectorToOffsetVector;
import static io.trino.orc.reader.ReaderUtils.unpackLengthNulls;
import static io.trino.orc.reader.SliceColumnReader.computeTruncatedLength;
import static io.trino.orc.stream.MissingInputStreamSource.missingStreamSource;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SliceDirectColumnReader
        implements ColumnReader
{
    private static final int INSTANCE_SIZE = instanceSize(SliceDirectColumnReader.class);
    private static final int ONE_GIGABYTE = toIntExact(DataSize.of(1, GIGABYTE).toBytes());

    private final int maxCodePointCount;
    private final boolean isCharType;
    private final OrcColumn column;

    private int readOffset;
    private int nextBatchSize;

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    @Nullable
    private BooleanInputStream presentStream;

    private InputStreamSource<LongInputStream> lengthStreamSource = missingStreamSource(LongInputStream.class);
    @Nullable
    private LongInputStream lengthStream;

    private InputStreamSource<ByteArrayInputStream> dataByteSource = missingStreamSource(ByteArrayInputStream.class);
    @Nullable
    private ByteArrayInputStream dataStream;

    private boolean rowGroupOpen;

    public SliceDirectColumnReader(OrcColumn column, int maxCodePointCount, boolean isCharType)
    {
        this.maxCodePointCount = maxCodePointCount;
        this.isCharType = isCharType;

        this.column = requireNonNull(column, "column is null");
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
                if (lengthStream == null) {
                    throw new OrcCorruptionException(column.getOrcDataSourceId(), "Value is not null but length stream is missing");
                }
                long dataSkipSize = lengthStream.sum(readOffset);
                if (dataSkipSize > 0) {
                    if (dataStream == null) {
                        throw new OrcCorruptionException(column.getOrcDataSourceId(), "Value is not null but data stream is missing");
                    }
                    dataStream.skip(dataSkipSize);
                }
            }
        }

        if (lengthStream == null) {
            if (presentStream == null) {
                throw new OrcCorruptionException(column.getOrcDataSourceId(), "Value is null but present stream is missing");
            }
            presentStream.skip(nextBatchSize);
            Block nullValueBlock = readAllNullsBlock();
            readOffset = 0;
            nextBatchSize = 0;
            return nullValueBlock;
        }

        // create new isNullVector and offsetVector for VariableWidthBlock
        boolean[] isNullVector = null;

        // We will use the offsetVector as the buffer to read the length values from lengthStream,
        // and the length values will be converted in-place to an offset vector.
        int[] offsetVector = new int[nextBatchSize + 1];

        if (presentStream == null) {
            lengthStream.next(offsetVector, nextBatchSize);
        }
        else {
            isNullVector = new boolean[nextBatchSize];
            int nullCount = presentStream.getUnsetBits(nextBatchSize, isNullVector);
            if (nullCount == nextBatchSize) {
                // all nulls
                Block nullValueBlock = readAllNullsBlock();
                readOffset = 0;
                nextBatchSize = 0;
                return nullValueBlock;
            }

            if (lengthStream == null) {
                throw new OrcCorruptionException(column.getOrcDataSourceId(), "Value is not null but length stream is missing");
            }
            if (nullCount == 0) {
                isNullVector = null;
                lengthStream.next(offsetVector, nextBatchSize);
            }
            else {
                lengthStream.next(offsetVector, nextBatchSize - nullCount);
                unpackLengthNulls(offsetVector, isNullVector, nextBatchSize - nullCount);
            }
        }

        // Calculate the total length for all entries. Note that the values in the offsetVector are still length values now.
        long totalLength = 0;
        for (int i = 0; i < nextBatchSize; i++) {
            totalLength += offsetVector[i];
        }

        int currentBatchSize = nextBatchSize;
        readOffset = 0;
        nextBatchSize = 0;
        if (totalLength == 0) {
            return new VariableWidthBlock(currentBatchSize, EMPTY_SLICE, offsetVector, Optional.ofNullable(isNullVector));
        }
        if (totalLength > ONE_GIGABYTE) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR,
                    format("Values in column \"%s\" are too large to process for Trino. %s column values are larger than 1GB [%s]", column.getPath(), nextBatchSize, column.getOrcDataSourceId()));
        }
        if (dataStream == null) {
            throw new OrcCorruptionException(column.getOrcDataSourceId(), "Value is not null but data stream is missing");
        }

        // allocate enough space to read
        byte[] data = new byte[toIntExact(totalLength)];
        Slice slice = Slices.wrappedBuffer(data);

        if (maxCodePointCount < 0) {
            // unbounded, simply read all data in on shot
            dataStream.next(data, 0, data.length);
            convertLengthVectorToOffsetVector(offsetVector);
        }
        else {
            // We do the following operations together in the for loop:
            // * truncate strings
            // * convert original length values in offsetVector into truncated offset values
            int currentLength = offsetVector[0];
            offsetVector[0] = 0;
            for (int i = 1; i <= currentBatchSize; i++) {
                int nextLength = offsetVector[i];
                if (isNullVector != null && isNullVector[i - 1]) {
                    checkState(currentLength == 0, "Corruption in slice direct stream: length is non-zero for null entry");
                    offsetVector[i] = offsetVector[i - 1];
                    currentLength = nextLength;
                    continue;
                }
                int offset = offsetVector[i - 1];

                // read data without truncation
                dataStream.next(data, offset, offset + currentLength);

                // adjust offsetVector with truncated length
                int truncatedLength = computeTruncatedLength(slice, offset, currentLength, maxCodePointCount, isCharType);
                verify(truncatedLength >= 0);
                offsetVector[i] = offset + truncatedLength;

                currentLength = nextLength;
            }
        }

        // this can lead to over-retention but unlikely to happen given truncation rarely happens
        return new VariableWidthBlock(currentBatchSize, slice, offsetVector, Optional.ofNullable(isNullVector));
    }

    private Block readAllNullsBlock()
    {
        return RunLengthEncodedBlock.create(new VariableWidthBlock(1, EMPTY_SLICE, new int[2], Optional.of(new boolean[] {true})), nextBatchSize);
    }

    private void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();
        lengthStream = lengthStreamSource.openStream();
        dataStream = dataByteSource.openStream();

        rowGroupOpen = true;
    }

    @Override
    public void startStripe(ZoneId fileTimeZone, InputStreamSources dictionaryStreamSources, ColumnMetadata<ColumnEncoding> encoding)
    {
        presentStreamSource = missingStreamSource(BooleanInputStream.class);
        lengthStreamSource = missingStreamSource(LongInputStream.class);
        dataByteSource = missingStreamSource(ByteArrayInputStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        lengthStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(column, PRESENT, BooleanInputStream.class);
        lengthStreamSource = dataStreamSources.getInputStreamSource(column, LENGTH, LongInputStream.class);
        dataByteSource = dataStreamSources.getInputStreamSource(column, DATA, ByteArrayInputStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        lengthStream = null;
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
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE;
    }
}
