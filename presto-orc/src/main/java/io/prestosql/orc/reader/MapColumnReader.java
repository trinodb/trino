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

import com.google.common.io.Closer;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.orc.OrcBlockFactory;
import io.prestosql.orc.OrcColumn;
import io.prestosql.orc.OrcCorruptionException;
import io.prestosql.orc.metadata.ColumnEncoding;
import io.prestosql.orc.metadata.ColumnMetadata;
import io.prestosql.orc.stream.BooleanInputStream;
import io.prestosql.orc.stream.InputStreamSource;
import io.prestosql.orc.stream.InputStreamSources;
import io.prestosql.orc.stream.LongInputStream;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.Type;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.ZoneId;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.prestosql.orc.metadata.Stream.StreamKind.LENGTH;
import static io.prestosql.orc.metadata.Stream.StreamKind.PRESENT;
import static io.prestosql.orc.reader.ColumnReaders.createColumnReader;
import static io.prestosql.orc.reader.ReaderUtils.convertLengthVectorToOffsetVector;
import static io.prestosql.orc.reader.ReaderUtils.unpackLengthNulls;
import static io.prestosql.orc.reader.ReaderUtils.verifyStreamType;
import static io.prestosql.orc.stream.MissingInputStreamSource.missingStreamSource;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class MapColumnReader
        implements ColumnReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MapColumnReader.class).instanceSize();

    private final MapType type;
    private final OrcColumn column;
    private final OrcBlockFactory blockFactory;

    private final ColumnReader keyColumnReader;
    private final ColumnReader valueColumnReader;

    private int readOffset;
    private int nextBatchSize;

    @Nonnull
    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    @Nullable
    private BooleanInputStream presentStream;

    @Nonnull
    private InputStreamSource<LongInputStream> lengthStreamSource = missingStreamSource(LongInputStream.class);
    @Nullable
    private LongInputStream lengthStream;

    private boolean rowGroupOpen;

    public MapColumnReader(Type type, OrcColumn column, AggregatedMemoryContext systemMemoryContext, OrcBlockFactory blockFactory)
            throws OrcCorruptionException
    {
        requireNonNull(type, "type is null");
        verifyStreamType(column, type, MapType.class::isInstance);
        this.type = (MapType) type;

        this.column = requireNonNull(column, "column is null");
        this.blockFactory = requireNonNull(blockFactory, "blockFactory is null");
        this.keyColumnReader = createColumnReader(this.type.getKeyType(), column.getNestedColumns().get(0), systemMemoryContext, blockFactory);
        this.valueColumnReader = createColumnReader(this.type.getValueType(), column.getNestedColumns().get(1), systemMemoryContext, blockFactory);
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
                if (lengthStream == null) {
                    throw new OrcCorruptionException(column.getOrcDataSourceId(), "Value is not null but data stream is not present");
                }
                long entrySkipSize = lengthStream.sum(readOffset);
                keyColumnReader.prepareNextRead(toIntExact(entrySkipSize));
                valueColumnReader.prepareNextRead(toIntExact(entrySkipSize));
            }
        }

        // We will use the offsetVector as the buffer to read the length values from lengthStream,
        // and the length values will be converted in-place to an offset vector.
        int[] offsetVector = new int[nextBatchSize + 1];
        boolean[] nullVector = null;

        if (presentStream == null) {
            if (lengthStream == null) {
                throw new OrcCorruptionException(column.getOrcDataSourceId(), "Value is not null but data stream is not present");
            }
            lengthStream.next(offsetVector, nextBatchSize);
        }
        else {
            nullVector = new boolean[nextBatchSize];
            int nullValues = presentStream.getUnsetBits(nextBatchSize, nullVector);
            if (nullValues != nextBatchSize) {
                if (lengthStream == null) {
                    throw new OrcCorruptionException(column.getOrcDataSourceId(), "Value is not null but data stream is not present");
                }
                lengthStream.next(offsetVector, nextBatchSize - nullValues);
                unpackLengthNulls(offsetVector, nullVector, nextBatchSize - nullValues);
            }
        }

        // Calculate the entryCount. Note that the values in the offsetVector are still length values now.
        int entryCount = 0;
        for (int i = 0; i < offsetVector.length - 1; i++) {
            entryCount += offsetVector[i];
        }

        Block keys;
        Block values;
        if (entryCount > 0) {
            keyColumnReader.prepareNextRead(entryCount);
            valueColumnReader.prepareNextRead(entryCount);
            keys = keyColumnReader.readBlock();
            values = blockFactory.createBlock(entryCount, valueColumnReader::readBlock, true);
        }
        else {
            keys = type.getKeyType().createBlockBuilder(null, 0).build();
            values = type.getValueType().createBlockBuilder(null, 1).build();
        }

        Block[] keyValueBlock = createKeyValueBlock(nextBatchSize, keys, values, offsetVector);

        convertLengthVectorToOffsetVector(offsetVector);

        readOffset = 0;
        nextBatchSize = 0;

        return type.createBlockFromKeyValue(Optional.ofNullable(nullVector), offsetVector, keyValueBlock[0], keyValueBlock[1]);
    }

    private static Block[] createKeyValueBlock(int positionCount, Block keys, Block values, int[] lengths)
    {
        if (!hasNull(keys)) {
            return new Block[] {keys, values};
        }

        //
        // Map entries with a null key are skipped in the Hive ORC reader, so skip them here also
        //

        IntArrayList nonNullPositions = new IntArrayList(keys.getPositionCount());

        int position = 0;
        for (int mapIndex = 0; mapIndex < positionCount; mapIndex++) {
            int length = lengths[mapIndex];
            for (int entryIndex = 0; entryIndex < length; entryIndex++) {
                if (keys.isNull(position)) {
                    // key is null, so remove this entry from the map
                    lengths[mapIndex]--;
                }
                else {
                    nonNullPositions.add(position);
                }
                position++;
            }
        }

        Block newKeys = keys.copyPositions(nonNullPositions.elements(), 0, nonNullPositions.size());
        Block newValues = values.copyPositions(nonNullPositions.elements(), 0, nonNullPositions.size());
        return new Block[] {newKeys, newValues};
    }

    private static boolean hasNull(Block keys)
    {
        for (int position = 0; position < keys.getPositionCount(); position++) {
            if (keys.isNull(position)) {
                return true;
            }
        }
        return false;
    }

    private void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();
        lengthStream = lengthStreamSource.openStream();

        rowGroupOpen = true;
    }

    @Override
    public void startStripe(ZoneId timeZone, InputStreamSources dictionaryStreamSources, ColumnMetadata<ColumnEncoding> encoding)
            throws IOException
    {
        presentStreamSource = missingStreamSource(BooleanInputStream.class);
        lengthStreamSource = missingStreamSource(LongInputStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        lengthStream = null;

        rowGroupOpen = false;

        keyColumnReader.startStripe(timeZone, dictionaryStreamSources, encoding);
        valueColumnReader.startStripe(timeZone, dictionaryStreamSources, encoding);
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
            throws IOException
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(column, PRESENT, BooleanInputStream.class);
        lengthStreamSource = dataStreamSources.getInputStreamSource(column, LENGTH, LongInputStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        lengthStream = null;

        rowGroupOpen = false;

        keyColumnReader.startRowGroup(dataStreamSources);
        valueColumnReader.startRowGroup(dataStreamSources);
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
        try (Closer closer = Closer.create()) {
            closer.register(keyColumnReader::close);
            closer.register(valueColumnReader::close);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + keyColumnReader.getRetainedSizeInBytes() + valueColumnReader.getRetainedSizeInBytes();
    }
}
