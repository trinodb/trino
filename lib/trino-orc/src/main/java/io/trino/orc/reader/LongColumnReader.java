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

import io.trino.memory.context.LocalMemoryContext;
import io.trino.orc.OrcColumn;
import io.trino.orc.OrcCorruptionException;
import io.trino.orc.metadata.ColumnEncoding;
import io.trino.orc.metadata.ColumnMetadata;
import io.trino.orc.stream.BooleanInputStream;
import io.trino.orc.stream.InputStreamSource;
import io.trino.orc.stream.InputStreamSources;
import io.trino.orc.stream.LongInputStream;
import io.trino.spi.block.Block;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ShortArrayBlock;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.Type;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verifyNotNull;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.orc.metadata.Stream.StreamKind.DATA;
import static io.trino.orc.metadata.Stream.StreamKind.PRESENT;
import static io.trino.orc.reader.ReaderUtils.minNonNullValueSize;
import static io.trino.orc.reader.ReaderUtils.unpackIntNulls;
import static io.trino.orc.reader.ReaderUtils.unpackLongNulls;
import static io.trino.orc.reader.ReaderUtils.unpackShortNulls;
import static io.trino.orc.reader.ReaderUtils.verifyStreamType;
import static io.trino.orc.stream.MissingInputStreamSource.missingStreamSource;
import static java.util.Objects.requireNonNull;

public class LongColumnReader
        implements ColumnReader
{
    private static final int INSTANCE_SIZE = instanceSize(LongColumnReader.class);

    private final Type type;
    private final OrcColumn column;

    private int readOffset;
    private int nextBatchSize;

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    @Nullable
    private BooleanInputStream presentStream;

    private InputStreamSource<LongInputStream> dataStreamSource = missingStreamSource(LongInputStream.class);
    @Nullable
    private LongInputStream dataStream;

    private boolean rowGroupOpen;

    // only one of the three arrays will be used
    private short[] shortNonNullValueTemp = new short[0];
    private int[] intNonNullValueTemp = new int[0];
    private long[] longNonNullValueTemp = new long[0];

    private final LocalMemoryContext memoryContext;

    public LongColumnReader(Type type, OrcColumn column, LocalMemoryContext memoryContext)
            throws OrcCorruptionException
    {
        requireNonNull(type, "type is null");
        verifyStreamType(column, type, t -> t instanceof BigintType || t instanceof IntegerType || t instanceof SmallintType || t instanceof DateType || t instanceof TimeType);
        this.type = type;

        this.column = requireNonNull(column, "column is null");
        this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
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
                // and use this as the skip size for the data reader
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
            block = RunLengthEncodedBlock.create(type, null, nextBatchSize);
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
                block = RunLengthEncodedBlock.create(type, null, nextBatchSize);
            }
        }

        readOffset = 0;
        nextBatchSize = 0;

        return block;
    }

    private Block readNonNullBlock()
            throws IOException
    {
        verifyNotNull(dataStream);
        if (type instanceof BigintType) {
            long[] values = new long[nextBatchSize];
            dataStream.next(values, nextBatchSize);
            return new LongArrayBlock(nextBatchSize, Optional.empty(), values);
        }
        if (type instanceof TimeType) {
            long[] values = new long[nextBatchSize];
            dataStream.next(values, nextBatchSize);
            maybeTransformValues(values, nextBatchSize);
            return new LongArrayBlock(nextBatchSize, Optional.empty(), values);
        }
        if (type instanceof IntegerType || type instanceof DateType) {
            int[] values = new int[nextBatchSize];
            dataStream.next(values, nextBatchSize);
            return new IntArrayBlock(nextBatchSize, Optional.empty(), values);
        }
        if (type instanceof SmallintType) {
            short[] values = new short[nextBatchSize];
            dataStream.next(values, nextBatchSize);
            return new ShortArrayBlock(nextBatchSize, Optional.empty(), values);
        }
        throw new VerifyError("Unsupported type " + type);
    }

    protected void maybeTransformValues(long[] values, int nextBatchSize) {}

    private Block readNullBlock(boolean[] isNull, int nonNullCount)
            throws IOException
    {
        if (type instanceof BigintType) {
            return longReadNullBlock(isNull, nonNullCount);
        }
        if (type instanceof IntegerType || type instanceof DateType) {
            return intReadNullBlock(isNull, nonNullCount);
        }
        if (type instanceof SmallintType) {
            return shortReadNullBlock(isNull, nonNullCount);
        }
        throw new VerifyError("Unsupported type " + type);
    }

    private Block longReadNullBlock(boolean[] isNull, int nonNullCount)
            throws IOException
    {
        verifyNotNull(dataStream);
        int minNonNullValueSize = minNonNullValueSize(nonNullCount);
        if (longNonNullValueTemp.length < minNonNullValueSize) {
            longNonNullValueTemp = new long[minNonNullValueSize];
            memoryContext.setBytes(sizeOf(longNonNullValueTemp));
        }

        dataStream.next(longNonNullValueTemp, nonNullCount);

        long[] result = unpackLongNulls(longNonNullValueTemp, isNull);

        return new LongArrayBlock(nextBatchSize, Optional.of(isNull), result);
    }

    private Block intReadNullBlock(boolean[] isNull, int nonNullCount)
            throws IOException
    {
        verifyNotNull(dataStream);
        int minNonNullValueSize = minNonNullValueSize(nonNullCount);
        if (intNonNullValueTemp.length < minNonNullValueSize) {
            intNonNullValueTemp = new int[minNonNullValueSize];
            memoryContext.setBytes(sizeOf(intNonNullValueTemp));
        }

        dataStream.next(intNonNullValueTemp, nonNullCount);

        int[] result = unpackIntNulls(intNonNullValueTemp, isNull);

        return new IntArrayBlock(nextBatchSize, Optional.of(isNull), result);
    }

    private Block shortReadNullBlock(boolean[] isNull, int nonNullCount)
            throws IOException
    {
        verifyNotNull(dataStream);
        int minNonNullValueSize = minNonNullValueSize(nonNullCount);
        if (shortNonNullValueTemp.length < minNonNullValueSize) {
            shortNonNullValueTemp = new short[minNonNullValueSize];
            memoryContext.setBytes(sizeOf(shortNonNullValueTemp));
        }

        dataStream.next(shortNonNullValueTemp, nonNullCount);

        short[] result = unpackShortNulls(shortNonNullValueTemp, isNull);

        return new ShortArrayBlock(nextBatchSize, Optional.of(isNull), result);
    }

    private void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();
        dataStream = dataStreamSource.openStream();

        rowGroupOpen = true;
    }

    @Override
    public void startStripe(ZoneId fileTimeZone, InputStreamSources dictionaryStreamSources, ColumnMetadata<ColumnEncoding> encoding)
    {
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
        memoryContext.close();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE;
    }
}
