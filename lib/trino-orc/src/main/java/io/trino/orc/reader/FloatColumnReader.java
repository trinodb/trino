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
import io.trino.orc.stream.FloatInputStream;
import io.trino.orc.stream.InputStreamSource;
import io.trino.orc.stream.InputStreamSources;
import io.trino.spi.block.Block;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
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
import static io.trino.orc.reader.ReaderUtils.verifyStreamType;
import static io.trino.orc.stream.MissingInputStreamSource.missingStreamSource;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static java.lang.Double.doubleToRawLongBits;
import static java.lang.Float.intBitsToFloat;
import static java.util.Objects.requireNonNull;

public class FloatColumnReader
        implements ColumnReader
{
    private static final int INSTANCE_SIZE = instanceSize(FloatColumnReader.class);

    private final Type type;

    private final OrcColumn column;

    private int readOffset;
    private int nextBatchSize;

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    @Nullable
    private BooleanInputStream presentStream;

    private InputStreamSource<FloatInputStream> dataStreamSource = missingStreamSource(FloatInputStream.class);
    @Nullable
    private FloatInputStream dataStream;

    private boolean rowGroupOpen;

    private int[] nonNullValueTemp = new int[0];

    private final LocalMemoryContext memoryContext;

    public FloatColumnReader(Type type, OrcColumn column, LocalMemoryContext memoryContext)
            throws OrcCorruptionException
    {
        this.type = requireNonNull(type, "type is null");
        verifyStreamType(column, type, t -> t == REAL || t == DOUBLE);

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
        int[] values = new int[nextBatchSize];
        dataStream.next(values, nextBatchSize);
        if (type == REAL) {
            return new IntArrayBlock(nextBatchSize, Optional.empty(), values);
        }
        if (type == DOUBLE) {
            return new LongArrayBlock(nextBatchSize, Optional.empty(), convertToLongArray(values));
        }
        throw new VerifyError("Unsupported type " + type);
    }

    private Block readNullBlock(boolean[] isNull, int nonNullCount)
            throws IOException
    {
        verifyNotNull(dataStream);
        int minNonNullValueSize = minNonNullValueSize(nonNullCount);
        if (nonNullValueTemp.length < minNonNullValueSize) {
            nonNullValueTemp = new int[minNonNullValueSize];
            memoryContext.setBytes(sizeOf(nonNullValueTemp));
        }

        dataStream.next(nonNullValueTemp, nonNullCount);

        int[] result = ReaderUtils.unpackIntNulls(nonNullValueTemp, isNull);
        if (type == REAL) {
            return new IntArrayBlock(isNull.length, Optional.of(isNull), result);
        }
        if (type == DOUBLE) {
            return new LongArrayBlock(isNull.length, Optional.of(isNull), convertToLongArray(result));
        }
        throw new VerifyError("Unsupported type " + type);
    }

    private static long[] convertToLongArray(int[] intValues)
    {
        long[] values = new long[intValues.length];
        for (int i = 0; i < intValues.length; i++) {
            values[i] = doubleToRawLongBits(intBitsToFloat(intValues[i]));
        }
        return values;
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
        dataStreamSource = missingStreamSource(FloatInputStream.class);

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
        dataStreamSource = dataStreamSources.getInputStreamSource(column, DATA, FloatInputStream.class);

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
