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
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.orc.OrcBlockFactory;
import io.trino.orc.OrcColumn;
import io.trino.orc.OrcCorruptionException;
import io.trino.orc.OrcReader;
import io.trino.orc.OrcReader.FieldMapper;
import io.trino.orc.OrcReader.FieldMapperFactory;
import io.trino.orc.metadata.ColumnEncoding;
import io.trino.orc.metadata.ColumnMetadata;
import io.trino.orc.stream.BooleanInputStream;
import io.trino.orc.stream.InputStreamSource;
import io.trino.orc.stream.InputStreamSources;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.RowType;
import io.trino.spi.type.RowType.Field;
import io.trino.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verify;
import static io.trino.orc.metadata.Stream.StreamKind.PRESENT;
import static io.trino.orc.reader.ColumnReaders.createColumnReader;
import static io.trino.orc.reader.ReaderUtils.verifyStreamType;
import static io.trino.orc.stream.MissingInputStreamSource.missingStreamSource;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class StructColumnReader
        implements ColumnReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(StructColumnReader.class).instanceSize();

    private final OrcColumn column;
    private final OrcBlockFactory blockFactory;

    private final Map<String, ColumnReader> structFields;
    private final RowType type;
    private final ImmutableList<String> fieldNames;

    private int readOffset;
    private int nextBatchSize;

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    @Nullable
    private BooleanInputStream presentStream;

    private boolean rowGroupOpen;

    StructColumnReader(
            Type type,
            OrcColumn column,
            OrcReader.ProjectedLayout readLayout,
            AggregatedMemoryContext systemMemoryContext,
            OrcBlockFactory blockFactory,
            FieldMapperFactory fieldMapperFactory)
            throws OrcCorruptionException
    {
        requireNonNull(type, "type is null");
        verifyStreamType(column, type, RowType.class::isInstance);
        this.type = (RowType) type;

        this.column = requireNonNull(column, "column is null");
        this.blockFactory = requireNonNull(blockFactory, "blockFactory is null");

        FieldMapper fieldMapper = fieldMapperFactory.create(column);
        ImmutableList.Builder<String> fieldNames = ImmutableList.builder();
        ImmutableMap.Builder<String, ColumnReader> structFields = ImmutableMap.builder();
        for (Field field : this.type.getFields()) {
            String fieldName = field.getName()
                    .orElseThrow(() -> new IllegalArgumentException("ROW type does not have field names declared: " + type))
                    .toLowerCase(ENGLISH);
            fieldNames.add(fieldName);

            OrcColumn fieldStream = fieldMapper.get(fieldName);

            if (fieldStream != null) {
                OrcReader.ProjectedLayout fieldLayout = readLayout.getFieldLayout(fieldStream);
                if (fieldLayout != null) {
                    structFields.put(
                            fieldName,
                            createColumnReader(
                                    field.getType(),
                                    fieldStream,
                                    fieldLayout,
                                    systemMemoryContext,
                                    blockFactory,
                                    fieldMapperFactory));
                }
            }
        }
        this.fieldNames = fieldNames.build();
        this.structFields = structFields.buildOrThrow();
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
            for (ColumnReader structField : structFields.values()) {
                structField.prepareNextRead(readOffset);
            }
        }

        boolean[] nullVector = null;
        Block[] blocks;

        if (presentStream == null) {
            blocks = getBlocksForType(nextBatchSize);
        }
        else {
            nullVector = new boolean[nextBatchSize];
            int nullValues = presentStream.getUnsetBits(nextBatchSize, nullVector);
            if (nullValues != nextBatchSize) {
                blocks = getBlocksForType(nextBatchSize - nullValues);
            }
            else {
                List<Type> typeParameters = type.getTypeParameters();
                blocks = new Block[typeParameters.size()];
                for (int i = 0; i < typeParameters.size(); i++) {
                    blocks[i] = typeParameters.get(i).createBlockBuilder(null, 0).build();
                }
            }
        }

        verify(Arrays.stream(blocks)
                .mapToInt(Block::getPositionCount)
                .distinct()
                .count() == 1);

        // Struct is represented as a row block
        Block rowBlock = RowBlock.fromFieldBlocks(nextBatchSize, Optional.ofNullable(nullVector), blocks);

        readOffset = 0;
        nextBatchSize = 0;

        return rowBlock;
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

        for (ColumnReader structField : structFields.values()) {
            structField.startStripe(fileTimeZone, dictionaryStreamSources, encoding);
        }
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

        for (ColumnReader structField : structFields.values()) {
            structField.startRowGroup(dataStreamSources);
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(column)
                .toString();
    }

    private Block[] getBlocksForType(int positionCount)
    {
        Block[] blocks = new Block[fieldNames.size()];

        for (int i = 0; i < fieldNames.size(); i++) {
            String fieldName = fieldNames.get(i);

            ColumnReader columnReader = structFields.get(fieldName);
            if (columnReader != null) {
                columnReader.prepareNextRead(positionCount);
                blocks[i] = blockFactory.createBlock(positionCount, columnReader::readBlock, true);
            }
            else {
                blocks[i] = RunLengthEncodedBlock.create(type.getFields().get(i).getType(), null, positionCount);
            }
        }
        return blocks;
    }

    @Override
    public void close()
    {
        try (Closer closer = Closer.create()) {
            for (ColumnReader structField : structFields.values()) {
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
        for (ColumnReader structField : structFields.values()) {
            retainedSizeInBytes += structField.getRetainedSizeInBytes();
        }
        return retainedSizeInBytes;
    }
}
