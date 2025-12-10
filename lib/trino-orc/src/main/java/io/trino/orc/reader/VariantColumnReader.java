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

import com.google.common.io.Closer;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.orc.OrcColumn;
import io.trino.orc.OrcCorruptionException;
import io.trino.orc.metadata.ColumnEncoding;
import io.trino.orc.metadata.ColumnMetadata;
import io.trino.orc.stream.BooleanInputStream;
import io.trino.orc.stream.InputStreamSource;
import io.trino.orc.stream.InputStreamSources;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.VariantBlock;
import jakarta.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.orc.metadata.Stream.StreamKind.PRESENT;
import static io.trino.orc.reader.ReaderUtils.toNotNullSupressedBlock;
import static io.trino.orc.stream.MissingInputStreamSource.missingStreamSource;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.util.Objects.requireNonNull;

/**
 * Reads a variant column from ORC.
 * Variant is stored as a struct with two binary fields: metadata and value.
 */
public class VariantColumnReader
        implements ColumnReader
{
    private static final int INSTANCE_SIZE = instanceSize(VariantColumnReader.class);

    private final OrcColumn column;
    private final ColumnReader metadataReader;
    private final ColumnReader valueReader;

    private int readOffset;
    private int nextBatchSize;

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    @Nullable
    private BooleanInputStream presentStream;

    private boolean rowGroupOpen;

    public VariantColumnReader(OrcColumn column, AggregatedMemoryContext memoryContext)
            throws OrcCorruptionException
    {
        this.column = requireNonNull(column, "column is null");

        List<OrcColumn> nestedColumns = column.getNestedColumns();
        checkArgument(nestedColumns.size() == 2, "Variant column must have exactly 2 children (metadata, value), but found %s", nestedColumns.size());

        // Fields are ordered: metadata, value
        OrcColumn metadataColumn = nestedColumns.get(0);
        OrcColumn valueColumn = nestedColumns.get(1);

        this.metadataReader = new SliceColumnReader(VARBINARY, metadataColumn, memoryContext);
        this.valueReader = new SliceColumnReader(VARBINARY, valueColumn, memoryContext);
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
                // and use this as the skip size for the field readers
                readOffset = presentStream.countBitsSet(readOffset);
            }
            metadataReader.prepareNextRead(readOffset);
            valueReader.prepareNextRead(readOffset);
        }

        boolean[] nullVector = null;
        Block metadataBlock;
        Block valueBlock;

        if (presentStream == null) {
            metadataReader.prepareNextRead(nextBatchSize);
            valueReader.prepareNextRead(nextBatchSize);
            metadataBlock = metadataReader.readBlock();
            valueBlock = valueReader.readBlock();
        }
        else {
            nullVector = new boolean[nextBatchSize];
            int nullValues = presentStream.getUnsetBits(nextBatchSize, nullVector);
            if (nullValues != nextBatchSize) {
                int nonNullCount = nextBatchSize - nullValues;
                metadataReader.prepareNextRead(nonNullCount);
                valueReader.prepareNextRead(nonNullCount);

                Block rawMetadata = metadataReader.readBlock();
                Block rawValue = valueReader.readBlock();

                metadataBlock = toNotNullSupressedBlock(nextBatchSize, nullVector, rawMetadata);
                valueBlock = toNotNullSupressedBlock(nextBatchSize, nullVector, rawValue);
            }
            else {
                // All values are null
                metadataBlock = RunLengthEncodedBlock.create(VARBINARY.createBlockBuilder(null, 0).appendNull().build(), nextBatchSize);
                valueBlock = RunLengthEncodedBlock.create(VARBINARY.createBlockBuilder(null, 0).appendNull().build(), nextBatchSize);
            }
        }

        VariantBlock variantBlock = VariantBlock.create(nextBatchSize, metadataBlock, valueBlock, Optional.ofNullable(nullVector));

        readOffset = 0;
        nextBatchSize = 0;

        return variantBlock;
    }

    private void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();
        rowGroupOpen = true;
    }

    @Override
    public void startStripe(ZoneId fileTimeZone, InputStreamSources dictionaryStreamSources, ColumnMetadata<ColumnEncoding> encoding)
            throws IOException
    {
        presentStreamSource = missingStreamSource(BooleanInputStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;

        rowGroupOpen = false;

        metadataReader.startStripe(fileTimeZone, dictionaryStreamSources, encoding);
        valueReader.startStripe(fileTimeZone, dictionaryStreamSources, encoding);
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
            throws IOException
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(column, PRESENT, BooleanInputStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;

        rowGroupOpen = false;

        metadataReader.startRowGroup(dataStreamSources);
        valueReader.startRowGroup(dataStreamSources);
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
            closer.register(metadataReader::close);
            closer.register(valueReader::close);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + metadataReader.getRetainedSizeInBytes() + valueReader.getRetainedSizeInBytes();
    }
}
