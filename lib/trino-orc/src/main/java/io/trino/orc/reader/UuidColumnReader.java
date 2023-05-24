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

import io.airlift.units.DataSize;
import io.trino.orc.OrcColumn;
import io.trino.orc.OrcCorruptionException;
import io.trino.orc.metadata.ColumnEncoding;
import io.trino.orc.metadata.ColumnMetadata;
import io.trino.orc.stream.BooleanInputStream;
import io.trino.orc.stream.ByteArrayInputStream;
import io.trino.orc.stream.InputStreamSource;
import io.trino.orc.stream.InputStreamSources;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.Int128ArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;

import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.time.ZoneId;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.trino.orc.metadata.Stream.StreamKind.DATA;
import static io.trino.orc.metadata.Stream.StreamKind.PRESENT;
import static io.trino.orc.stream.MissingInputStreamSource.missingStreamSource;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class UuidColumnReader
        implements ColumnReader
{
    private static final int INSTANCE_SIZE = instanceSize(UuidColumnReader.class);
    private static final int ONE_GIGABYTE = toIntExact(DataSize.of(1, GIGABYTE).toBytes());

    private static final VarHandle LONG_ARRAY_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);
    private final OrcColumn column;

    private int readOffset;
    private int nextBatchSize;

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    @Nullable
    private BooleanInputStream presentStream;

    private InputStreamSource<ByteArrayInputStream> dataByteSource = missingStreamSource(ByteArrayInputStream.class);
    @Nullable
    private ByteArrayInputStream dataStream;

    private boolean rowGroupOpen;

    public UuidColumnReader(OrcColumn column)
    {
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
            skipToReadOffset();
            readOffset = 0;
        }

        if (dataStream == null) {
            if (presentStream == null) {
                throw new OrcCorruptionException(column.getOrcDataSourceId(), "Value is null but present stream is missing");
            }
            // since dataStream is null, all values are null
            presentStream.skip(nextBatchSize);
            Block nullValueBlock = createAllNullsBlock();
            nextBatchSize = 0;
            return nullValueBlock;
        }

        boolean[] isNullVector = null;
        int nullCount = 0;
        if (presentStream != null) {
            isNullVector = new boolean[nextBatchSize];
            nullCount = presentStream.getUnsetBits(nextBatchSize, isNullVector);
            if (nullCount == nextBatchSize) {
                // all nulls
                Block nullValueBlock = createAllNullsBlock();
                nextBatchSize = 0;
                return nullValueBlock;
            }

            if (nullCount == 0) {
                isNullVector = null;
            }
        }

        int numberOfLongValues = toIntExact(nextBatchSize * 2L);
        int totalByteLength = toIntExact((long) numberOfLongValues * Long.BYTES);

        int currentBatchSize = nextBatchSize;
        nextBatchSize = 0;
        if (totalByteLength == 0) {
            return new Int128ArrayBlock(currentBatchSize, Optional.empty(), new long[0]);
        }
        if (totalByteLength > ONE_GIGABYTE) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR,
                    format("Values in column \"%s\" are too large to process for Trino. %s column values are larger than 1GB [%s]", column.getPath(), nextBatchSize, column.getOrcDataSourceId()));
        }
        if (dataStream == null) {
            throw new OrcCorruptionException(column.getOrcDataSourceId(), "Value is not null but data stream is missing");
        }

        if (isNullVector == null) {
            long[] values = readNonNullLongs(numberOfLongValues);
            return new Int128ArrayBlock(currentBatchSize, Optional.empty(), values);
        }

        int nonNullCount = currentBatchSize - nullCount;
        long[] values = readNullableLongs(isNullVector, nonNullCount);
        return new Int128ArrayBlock(currentBatchSize, Optional.of(isNullVector), values);
    }

    @Override
    public void startStripe(ZoneId fileTimeZone, InputStreamSources dictionaryStreamSources, ColumnMetadata<ColumnEncoding> encoding)
    {
        presentStreamSource = missingStreamSource(BooleanInputStream.class);
        dataByteSource = missingStreamSource(ByteArrayInputStream.class);

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
        dataByteSource = dataStreamSources.getInputStreamSource(column, DATA, ByteArrayInputStream.class);

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
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE;
    }

    private void skipToReadOffset()
            throws IOException
    {
        int dataReadOffset = readOffset;
        if (presentStream != null) {
            // skip ahead the present bit reader, but count the set bits
            // and use this as the skip size for the dataStream
            dataReadOffset = presentStream.countBitsSet(readOffset);
        }
        if (dataReadOffset > 0) {
            if (dataStream == null) {
                throw new OrcCorruptionException(column.getOrcDataSourceId(), "Value is not null but data stream is missing");
            }
            // dataReadOffset deals with positions. Each position is 2 longs in the dataStream.
            long dataSkipSize = dataReadOffset * 2L * Long.BYTES;

            dataStream.skip(dataSkipSize);
        }
    }

    private long[] readNullableLongs(boolean[] isNullVector, int nonNullCount)
            throws IOException
    {
        byte[] data = new byte[nonNullCount * 2 * Long.BYTES];

        dataStream.next(data, 0, data.length);

        int[] offsets = new int[isNullVector.length];
        int offsetPosition = 0;
        for (int i = 0; i < isNullVector.length; i++) {
            offsets[i] = Math.min(offsetPosition * 2 * Long.BYTES, data.length - Long.BYTES * 2);
            offsetPosition += isNullVector[i] ? 0 : 1;
        }

        long[] values = new long[isNullVector.length * 2];

        for (int i = 0; i < isNullVector.length; i++) {
            int isNonNull = isNullVector[i] ? 0 : 1;
            values[i * 2] = (long) LONG_ARRAY_HANDLE.get(data, offsets[i]) * isNonNull;
            values[i * 2 + 1] = (long) LONG_ARRAY_HANDLE.get(data, offsets[i] + Long.BYTES) * isNonNull;
        }
        return values;
    }

    private long[] readNonNullLongs(int valueCount)
            throws IOException
    {
        byte[] data = new byte[valueCount * Long.BYTES];

        dataStream.next(data, 0, data.length);

        long[] values = new long[valueCount];
        for (int i = 0; i < valueCount; i++) {
            values[i] = (long) LONG_ARRAY_HANDLE.get(data, i * Long.BYTES);
        }
        return values;
    }

    private Block createAllNullsBlock()
    {
        return RunLengthEncodedBlock.create(new Int128ArrayBlock(1, Optional.of(new boolean[] {true}), new long[2]), nextBatchSize);
    }

    private void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();
        dataStream = dataByteSource.openStream();

        rowGroupOpen = true;
    }
}
