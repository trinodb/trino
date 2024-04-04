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

import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.orc.OrcBlockFactory;
import io.trino.orc.OrcColumn;
import io.trino.orc.OrcCorruptionException;
import io.trino.orc.OrcReader.FieldMapperFactory;
import io.trino.orc.metadata.ColumnEncoding;
import io.trino.orc.metadata.ColumnMetadata;
import io.trino.orc.stream.BooleanInputStream;
import io.trino.orc.stream.ByteInputStream;
import io.trino.orc.stream.InputStreamSource;
import io.trino.orc.stream.InputStreamSources;
import io.trino.spi.block.Block;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.block.LazyBlockLoader;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.orc.OrcReader.fullyProjectedLayout;
import static io.trino.orc.metadata.Stream.StreamKind.DATA;
import static io.trino.orc.metadata.Stream.StreamKind.PRESENT;
import static io.trino.orc.reader.ColumnReaders.createColumnReader;
import static io.trino.orc.reader.ReaderUtils.toNotNullSupressedBlock;
import static io.trino.orc.reader.ReaderUtils.verifyStreamType;
import static io.trino.orc.stream.MissingInputStreamSource.missingStreamSource;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Objects.requireNonNull;

// Use row blocks to represent union objects when reading
public class UnionColumnReader
        implements ColumnReader
{
    private static final int INSTANCE_SIZE = instanceSize(UnionColumnReader.class);

    private final OrcColumn column;
    private final OrcBlockFactory blockFactory;

    private final RowType type;
    private final List<ColumnReader> fieldReaders;

    private int readOffset;
    private int nextBatchSize;

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    private InputStreamSource<ByteInputStream> dataStreamSource = missingStreamSource(ByteInputStream.class);
    @Nullable
    private BooleanInputStream presentStream;
    @Nullable
    private ByteInputStream dataStream;

    private boolean rowGroupOpen;

    UnionColumnReader(Type type, OrcColumn column, AggregatedMemoryContext memoryContext, OrcBlockFactory blockFactory, FieldMapperFactory fieldMapperFactory)
            throws OrcCorruptionException
    {
        requireNonNull(type, "type is null");
        verifyStreamType(column, type, RowType.class::isInstance);
        this.type = (RowType) type;

        this.column = requireNonNull(column, "column is null");
        this.blockFactory = requireNonNull(blockFactory, "blockFactory is null");

        ImmutableList.Builder<ColumnReader> fieldReadersBuilder = ImmutableList.builder();
        List<OrcColumn> fields = column.getNestedColumns();
        for (int i = 0; i < fields.size(); i++) {
            fieldReadersBuilder.add(createColumnReader(
                    type.getTypeParameters().get(i + 1),
                    fields.get(i),
                    fullyProjectedLayout(),
                    memoryContext,
                    blockFactory,
                    fieldMapperFactory));
        }
        fieldReaders = fieldReadersBuilder.build();
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
                readOffset = presentStream.countBitsSet(readOffset);
            }
            if (readOffset > 0) {
                if (dataStream == null) {
                    throw new OrcCorruptionException(column.getOrcDataSourceId(), "Value is not null but data stream is missing");
                }
                int[] readOffsets = new int[fieldReaders.size()];
                for (byte tag : dataStream.next(readOffset)) {
                    readOffsets[tag]++;
                }
                for (int i = 0; i < fieldReaders.size(); i++) {
                    fieldReaders.get(i).prepareNextRead(readOffsets[i]);
                }
            }
        }

        boolean[] nullVector = null;
        Block[] blocks;

        if (presentStream == null) {
            blocks = getBlocks(nextBatchSize, nextBatchSize, null);
        }
        else {
            nullVector = new boolean[nextBatchSize];
            int nullValues = presentStream.getUnsetBits(nextBatchSize, nullVector);
            if (nullValues != nextBatchSize) {
                blocks = getBlocks(nextBatchSize, nextBatchSize - nullValues, nullVector);
            }
            else {
                List<Type> typeParameters = type.getTypeParameters();
                blocks = new Block[typeParameters.size() + 1];
                blocks[0] = TINYINT.createBlockBuilder(null, 0).build();
                for (int i = 0; i < typeParameters.size(); i++) {
                    blocks[i + 1] = typeParameters.get(i).createBlockBuilder(null, 0).build();
                }
            }
        }

        verify(Arrays.stream(blocks)
                .mapToInt(Block::getPositionCount)
                .distinct()
                .count() == 1);

        Block rowBlock = RowBlock.fromNotNullSuppressedFieldBlocks(nextBatchSize, Optional.ofNullable(nullVector), blocks);

        readOffset = 0;
        nextBatchSize = 0;

        return rowBlock;
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
            throws IOException
    {
        presentStreamSource = missingStreamSource(BooleanInputStream.class);
        dataStreamSource = missingStreamSource(ByteInputStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        dataStream = null;

        rowGroupOpen = false;

        for (ColumnReader fieldReader : fieldReaders) {
            fieldReader.startStripe(fileTimeZone, dictionaryStreamSources, encoding);
        }
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
            throws IOException
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(column, PRESENT, BooleanInputStream.class);
        dataStreamSource = dataStreamSources.getInputStreamSource(column, DATA, ByteInputStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        dataStream = null;

        rowGroupOpen = false;

        for (ColumnReader fieldReader : fieldReaders) {
            fieldReader.startRowGroup(dataStreamSources);
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(column)
                .toString();
    }

    private Block[] getBlocks(int positionCount, int nonNullCount, boolean[] rowIsNull)
            throws IOException
    {
        if (dataStream == null) {
            throw new OrcCorruptionException(column.getOrcDataSourceId(), "Value is not null but data stream is missing");
        }

        Block[] blocks = new Block[fieldReaders.size() + 1];

        // read null suppressed tag column, and then remove the suppression
        byte[] tags = dataStream.next(nonNullCount);
        if (rowIsNull == null) {
            blocks[0] = new ByteArrayBlock(positionCount, Optional.empty(), tags);
        }
        else {
            blocks[0] = toNotNullSupressedBlock(positionCount, rowIsNull, new ByteArrayBlock(nonNullCount, Optional.empty(), tags));
        }

        // build a null vector for each field
        boolean[][] valueIsNull = new boolean[fieldReaders.size()][positionCount];
        for (boolean[] fieldIsNull : valueIsNull) {
            Arrays.fill(fieldIsNull, true);
        }
        int[] nonNullValueCount = new int[fieldReaders.size()];
        for (int position = 0; position < positionCount; position++) {
            if (rowIsNull != null && rowIsNull[position]) {
                byte tag = tags[position];
                valueIsNull[tag][position] = false;
                nonNullValueCount[tag]++;
            }
        }

        for (int i = 0; i < fieldReaders.size(); i++) {
            Type fieldType = type.getTypeParameters().get(i + 1);
            if (nonNullValueCount[i] > 0) {
                ColumnReader reader = fieldReaders.get(i);
                reader.prepareNextRead(nonNullValueCount[i]);
                LazyBlockLoader lazyBlockLoader = blockFactory.createLazyBlockLoader(reader::readBlock, true);
                boolean[] fieldIsNull = valueIsNull[i];
                blocks[i] = new LazyBlock(positionCount, () -> toNotNullSupressedBlock(positionCount, fieldIsNull, lazyBlockLoader.load()));
            }
            else {
                blocks[i + 1] = RunLengthEncodedBlock.create(
                        fieldType.createBlockBuilder(null, 1).appendNull().build(),
                        positionCount);
            }
        }
        return blocks;
    }

    @Override
    public void close()
    {
        try (Closer closer = Closer.create()) {
            for (ColumnReader structField : fieldReaders) {
                closer.register(structField::close);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long retainedSizeInBytes = INSTANCE_SIZE;
        for (ColumnReader structField : fieldReaders) {
            retainedSizeInBytes += structField.getRetainedSizeInBytes();
        }
        return retainedSizeInBytes;
    }
}
