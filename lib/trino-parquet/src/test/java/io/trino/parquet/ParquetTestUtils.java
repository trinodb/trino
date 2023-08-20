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
package io.trino.parquet;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Booleans;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.reader.ParquetReader;
import io.trino.parquet.writer.ParquetSchemaConverter;
import io.trino.parquet.writer.ParquetWriter;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.MessageColumnIO;
import org.joda.time.DateTimeZone;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.parquet.ParquetTypeUtils.constructField;
import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.parquet.ParquetTypeUtils.getParquetEncoding;
import static io.trino.parquet.ParquetTypeUtils.lookupColumnByName;
import static io.trino.spi.block.ArrayBlock.fromElementBlock;
import static io.trino.spi.block.MapBlock.fromKeyValueBlock;
import static io.trino.spi.block.RowBlock.fromFieldBlocks;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static java.lang.Math.toIntExact;
import static java.util.Collections.nCopies;
import static org.joda.time.DateTimeZone.UTC;

public class ParquetTestUtils
{
    private static final Random RANDOM = new Random(42);
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();

    private ParquetTestUtils() {}

    public static Slice writeParquetFile(ParquetWriterOptions writerOptions, List<Type> types, List<String> columnNames, List<io.trino.spi.Page> inputPages)
            throws IOException
    {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ParquetWriter writer = createParquetWriter(outputStream, writerOptions, types, columnNames);

        for (io.trino.spi.Page inputPage : inputPages) {
            checkArgument(types.size() == inputPage.getChannelCount());
            writer.write(inputPage);
        }
        writer.close();
        return Slices.wrappedBuffer(outputStream.toByteArray());
    }

    public static ParquetWriter createParquetWriter(OutputStream outputStream, ParquetWriterOptions writerOptions, List<Type> types, List<String> columnNames)
    {
        checkArgument(types.size() == columnNames.size());
        ParquetSchemaConverter schemaConverter = new ParquetSchemaConverter(types, columnNames, false, false);
        return new ParquetWriter(
                outputStream,
                schemaConverter.getMessageType(),
                schemaConverter.getPrimitiveTypes(),
                writerOptions,
                CompressionCodec.SNAPPY,
                "test-version",
                Optional.of(DateTimeZone.getDefault()),
                Optional.empty());
    }

    public static ParquetReader createParquetReader(
            ParquetDataSource input,
            ParquetMetadata parquetMetadata,
            AggregatedMemoryContext memoryContext,
            List<Type> types,
            List<String> columnNames)
            throws IOException
    {
        org.apache.parquet.hadoop.metadata.FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
        MessageColumnIO messageColumnIO = getColumnIO(fileMetaData.getSchema(), fileMetaData.getSchema());
        ImmutableList.Builder<Field> columnFields = ImmutableList.builder();
        for (int i = 0; i < types.size(); i++) {
            columnFields.add(constructField(
                    types.get(i),
                    lookupColumnByName(messageColumnIO, columnNames.get(i)))
                    .orElseThrow());
        }
        long nextStart = 0;
        ImmutableList.Builder<Long> blockStartsBuilder = ImmutableList.builder();
        for (BlockMetaData block : parquetMetadata.getBlocks()) {
            blockStartsBuilder.add(nextStart);
            nextStart += block.getRowCount();
        }
        List<Long> blockStarts = blockStartsBuilder.build();
        return new ParquetReader(
                Optional.ofNullable(fileMetaData.getCreatedBy()),
                columnFields.build(),
                parquetMetadata.getBlocks(),
                blockStarts,
                input,
                UTC,
                memoryContext,
                new ParquetReaderOptions(),
                exception -> {
                    throwIfUnchecked(exception);
                    return new RuntimeException(exception);
                },
                Optional.empty(),
                nCopies(blockStarts.size(), Optional.empty()),
                Optional.empty());
    }

    public static List<io.trino.spi.Page> generateInputPages(List<Type> types, int positionsPerPage, int pageCount)
    {
        ImmutableList.Builder<io.trino.spi.Page> pagesBuilder = ImmutableList.builder();
        for (int i = 0; i < pageCount; i++) {
            List<Block> blocks = types.stream()
                    .map(type -> generateBlock(type, positionsPerPage))
                    .collect(toImmutableList());
            pagesBuilder.add(new Page(blocks.toArray(Block[]::new)));
        }
        return pagesBuilder.build();
    }

    public static List<Integer> generateGroupSizes(int positionsCount)
    {
        int maxGroupSize = 17;
        int offset = 0;
        ImmutableList.Builder<Integer> groupsBuilder = ImmutableList.builder();
        while (offset < positionsCount) {
            int remaining = positionsCount - offset;
            int groupSize = Math.min(RANDOM.nextInt(maxGroupSize) + 1, remaining);
            groupsBuilder.add(groupSize);
            offset += groupSize;
        }
        return groupsBuilder.build();
    }

    public static Block createRowBlock(Optional<boolean[]> rowIsNull, int positionCount)
    {
        int fieldPositionCount = rowIsNull.map(nulls -> toIntExact(Booleans.asList(nulls).stream().filter(isNull -> !isNull).count()))
                .orElse(positionCount);
        int fieldCount = 4;
        Block[] fieldBlocks = new Block[fieldCount];
        // no nulls block
        fieldBlocks[0] = new LongArrayBlock(fieldPositionCount, Optional.empty(), new long[fieldPositionCount]);
        // no nulls with mayHaveNull block
        fieldBlocks[1] = new LongArrayBlock(fieldPositionCount, Optional.of(new boolean[fieldPositionCount]), new long[fieldPositionCount]);
        // all nulls block
        boolean[] allNulls = new boolean[fieldPositionCount];
        Arrays.fill(allNulls, false);
        fieldBlocks[2] = new LongArrayBlock(fieldPositionCount, Optional.of(allNulls), new long[fieldPositionCount]);
        // random nulls block
        fieldBlocks[3] = createLongsBlockWithRandomNulls(fieldPositionCount);

        return fromFieldBlocks(positionCount, rowIsNull, fieldBlocks);
    }

    public static Block createArrayBlock(Optional<boolean[]> valueIsNull, int positionCount)
    {
        int[] arrayOffset = generateOffsets(valueIsNull, positionCount);
        return fromElementBlock(positionCount, valueIsNull, arrayOffset, createLongsBlockWithRandomNulls(arrayOffset[positionCount]));
    }

    public static Block createMapBlock(Optional<boolean[]> mapIsNull, int positionCount)
    {
        int[] offsets = generateOffsets(mapIsNull, positionCount);
        int entriesCount = offsets[positionCount];
        Block keyBlock = new LongArrayBlock(entriesCount, Optional.empty(), new long[entriesCount]);
        Block valueBlock = createLongsBlockWithRandomNulls(entriesCount);
        return fromKeyValueBlock(mapIsNull, offsets, keyBlock, valueBlock, new MapType(BIGINT, BIGINT, TYPE_OPERATORS));
    }

    public static int[] generateOffsets(Optional<boolean[]> valueIsNull, int positionCount)
    {
        int maxCardinality = 7; // array length or map size at the current position
        int[] offsets = new int[positionCount + 1];
        for (int position = 0; position < positionCount; position++) {
            if (valueIsNull.isPresent() && valueIsNull.get()[position]) {
                offsets[position + 1] = offsets[position];
            }
            else {
                offsets[position + 1] = offsets[position] + RANDOM.nextInt(maxCardinality);
            }
        }
        return offsets;
    }

    private static Block createLongsBlockWithRandomNulls(int positionCount)
    {
        boolean[] valueIsNull = new boolean[positionCount];
        for (int i = 0; i < positionCount; i++) {
            valueIsNull[i] = RANDOM.nextBoolean();
        }
        return new LongArrayBlock(positionCount, Optional.of(valueIsNull), new long[positionCount]);
    }

    private static Block generateBlock(Type type, int positions)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, positions);
        for (int i = 0; i < positions; i++) {
            writeNativeValue(type, blockBuilder, (long) i);
        }
        return blockBuilder.build();
    }

    public static DictionaryPage toTrinoDictionaryPage(org.apache.parquet.column.page.DictionaryPage dictionary)
    {
        try {
            return new DictionaryPage(
                    Slices.wrappedBuffer(dictionary.getBytes().toByteArray()),
                    dictionary.getDictionarySize(),
                    getParquetEncoding(dictionary.getEncoding()));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
