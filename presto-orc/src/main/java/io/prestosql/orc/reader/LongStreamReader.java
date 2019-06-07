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

import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.orc.OrcCorruptionException;
import io.prestosql.orc.StreamDescriptor;
import io.prestosql.orc.metadata.ColumnEncoding;
import io.prestosql.orc.stream.BooleanInputStream;
import io.prestosql.orc.stream.InputStreamSource;
import io.prestosql.orc.stream.InputStreamSources;
import io.prestosql.orc.stream.LongInputStream;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.IntArrayBlock;
import io.prestosql.spi.block.LongArrayBlock;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.block.ShortArrayBlock;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.prestosql.orc.metadata.Stream.StreamKind.DATA;
import static io.prestosql.orc.metadata.Stream.StreamKind.PRESENT;
import static io.prestosql.orc.reader.ReaderUtils.minNonNullValueSize;
import static io.prestosql.orc.reader.ReaderUtils.unpackIntNulls;
import static io.prestosql.orc.reader.ReaderUtils.unpackLongNulls;
import static io.prestosql.orc.reader.ReaderUtils.unpackShortNulls;
import static io.prestosql.orc.reader.ReaderUtils.verifyStreamType;
import static io.prestosql.orc.stream.MissingInputStreamSource.missingStreamSource;
import static java.util.Objects.requireNonNull;

public class LongStreamReader
        implements StreamReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(LongStreamReader.class).instanceSize();

    private final Type type;
    private final StreamDescriptor streamDescriptor;

    private int readOffset;
    private int nextBatchSize;

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    @Nullable
    private BooleanInputStream presentStream;
    private boolean[] nullVector = new boolean[0];

    private InputStreamSource<LongInputStream> dataStreamSource = missingStreamSource(LongInputStream.class);
    @Nullable
    private LongInputStream dataStream;

    private boolean rowGroupOpen;

    // only one of the three arrays will be used
    private short[] shortNonNullValueTemp = new short[0];
    private int[] intNonNullValueTemp = new int[0];
    private long[] longNonNullValueTemp = new long[0];

    private final LocalMemoryContext systemMemoryContext;

    public LongStreamReader(Type type, StreamDescriptor streamDescriptor, LocalMemoryContext systemMemoryContext)
            throws OrcCorruptionException
    {
        requireNonNull(type, "type is null");
        verifyStreamType(streamDescriptor, type, t -> t instanceof BigintType || t instanceof IntegerType || t instanceof SmallintType || t instanceof DateType);
        this.type = type;

        this.streamDescriptor = requireNonNull(streamDescriptor, "stream is null");
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
                // and use this as the skip size for the data reader
                readOffset = presentStream.countBitsSet(readOffset);
            }
            if (readOffset > 0) {
                if (dataStream == null) {
                    throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but data stream is missing");
                }
                dataStream.skip(readOffset);
            }
        }

        Block block;
        if (dataStream == null) {
            if (presentStream == null) {
                throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is null but present stream is missing");
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
        verify(dataStream != null);
        if (type instanceof BigintType) {
            long[] values = new long[nextBatchSize];
            dataStream.next(values, nextBatchSize);
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
        verify(dataStream != null);
        int minNonNullValueSize = minNonNullValueSize(nonNullCount);
        if (longNonNullValueTemp.length < minNonNullValueSize) {
            longNonNullValueTemp = new long[minNonNullValueSize];
            systemMemoryContext.setBytes(sizeOf(longNonNullValueTemp));
        }

        dataStream.next(longNonNullValueTemp, nonNullCount);

        long[] result = unpackLongNulls(longNonNullValueTemp, isNull);

        return new LongArrayBlock(nextBatchSize, Optional.of(isNull), result);
    }

    private Block intReadNullBlock(boolean[] isNull, int nonNullCount)
            throws IOException
    {
        verify(dataStream != null);
        int minNonNullValueSize = minNonNullValueSize(nonNullCount);
        if (intNonNullValueTemp.length < minNonNullValueSize) {
            intNonNullValueTemp = new int[minNonNullValueSize];
            systemMemoryContext.setBytes(sizeOf(intNonNullValueTemp));
        }

        dataStream.next(intNonNullValueTemp, nonNullCount);

        int[] result = unpackIntNulls(intNonNullValueTemp, isNull);

        return new IntArrayBlock(nextBatchSize, Optional.of(isNull), result);
    }

    private Block shortReadNullBlock(boolean[] isNull, int nonNullCount)
            throws IOException
    {
        verify(dataStream != null);
        int minNonNullValueSize = minNonNullValueSize(nonNullCount);
        if (shortNonNullValueTemp.length < minNonNullValueSize) {
            shortNonNullValueTemp = new short[minNonNullValueSize];
            systemMemoryContext.setBytes(sizeOf(shortNonNullValueTemp));
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
    public void startStripe(ZoneId timeZone, InputStreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
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
        presentStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, PRESENT, BooleanInputStream.class);
        dataStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, DATA, LongInputStream.class);

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
                .addValue(streamDescriptor)
                .toString();
    }

    @Override
    public void close()
    {
        systemMemoryContext.close();
        nullVector = null;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(nullVector);
    }
}
