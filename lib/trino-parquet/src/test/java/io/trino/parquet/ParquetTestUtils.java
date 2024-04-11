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
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.predicate.TupleDomainParquetPredicate;
import io.trino.parquet.reader.ParquetReader;
import io.trino.parquet.reader.RowGroupInfo;
import io.trino.parquet.writer.ParquetSchemaConverter;
import io.trino.parquet.writer.ParquetWriter;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.joda.time.DateTimeZone;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.parquet.ParquetTypeUtils.constructField;
import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.parquet.ParquetTypeUtils.getParquetEncoding;
import static io.trino.parquet.ParquetTypeUtils.lookupColumnByName;
import static io.trino.parquet.predicate.PredicateUtils.buildPredicate;
import static io.trino.parquet.predicate.PredicateUtils.getFilteredRowGroups;
import static io.trino.spi.block.ArrayBlock.fromElementBlock;
import static io.trino.spi.block.MapBlock.fromKeyValueBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static java.util.Locale.ENGLISH;
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
        return createParquetReader(input, parquetMetadata, new ParquetReaderOptions(), memoryContext, types, columnNames, TupleDomain.all());
    }

    public static ParquetReader createParquetReader(
            ParquetDataSource input,
            ParquetMetadata parquetMetadata,
            ParquetReaderOptions options,
            AggregatedMemoryContext memoryContext,
            List<Type> types,
            List<String> columnNames,
            TupleDomain<String> predicate)
            throws IOException
    {
        org.apache.parquet.hadoop.metadata.FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
        MessageType fileSchema = fileMetaData.getSchema();
        MessageColumnIO messageColumnIO = getColumnIO(fileSchema, fileSchema);
        ImmutableList.Builder<Column> columnFields = ImmutableList.builder();
        for (int i = 0; i < types.size(); i++) {
            columnFields.add(new Column(
                    messageColumnIO.getName(),
                    constructField(
                            types.get(i),
                            lookupColumnByName(messageColumnIO, columnNames.get(i)))
                            .orElseThrow()));
        }
        Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, fileSchema);
        TupleDomain<ColumnDescriptor> parquetTupleDomain = predicate.transformKeys(
                columnName -> descriptorsByPath.get(ImmutableList.of(columnName.toLowerCase(ENGLISH))));
        TupleDomainParquetPredicate parquetPredicate = buildPredicate(fileSchema, parquetTupleDomain, descriptorsByPath, UTC);
        List<RowGroupInfo> rowGroups = getFilteredRowGroups(
                0,
                input.getEstimatedSize(),
                input,
                parquetMetadata.getBlocks(),
                ImmutableList.of(parquetTupleDomain),
                ImmutableList.of(parquetPredicate),
                descriptorsByPath,
                UTC,
                1000,
                options);
        return new ParquetReader(
                Optional.ofNullable(fileMetaData.getCreatedBy()),
                columnFields.build(),
                rowGroups,
                input,
                UTC,
                memoryContext,
                options,
                exception -> {
                    throwIfUnchecked(exception);
                    return new RuntimeException(exception);
                },
                Optional.of(parquetPredicate),
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

    public static List<io.trino.spi.Page> generateInputPages(List<Type> types, int positionsPerPage, List<?> data)
    {
        ImmutableList.Builder<io.trino.spi.Page> pagesBuilder = ImmutableList.builder();
        for (int i = 0; i < data.size(); i += positionsPerPage) {
            int index = i;
            List<Block> blocks = types.stream()
                    .map(type -> generateBlock(type, data.subList(index, index + positionsPerPage)))
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

    public static RowBlock createRowBlock(Optional<boolean[]> rowIsNull, int positionCount)
    {
        // TODO test with nested null fields and without nulls
        Block[] fieldBlocks = new Block[4];
        // no nulls block
        fieldBlocks[0] = new LongArrayBlock(positionCount, rowIsNull, new long[positionCount]);
        // no nulls with mayHaveNull block
        fieldBlocks[1] = new LongArrayBlock(positionCount, rowIsNull.or(() -> Optional.of(new boolean[positionCount])), new long[positionCount]);
        // all nulls block
        boolean[] allNulls = new boolean[positionCount];
        Arrays.fill(allNulls, true);
        fieldBlocks[2] = new LongArrayBlock(positionCount, Optional.of(allNulls), new long[positionCount]);
        // random nulls block
        boolean[] valueIsNull = rowIsNull.map(boolean[]::clone).orElseGet(() -> new boolean[positionCount]);
        for (int i = 0; i < positionCount; i++) {
            valueIsNull[i] |= RANDOM.nextBoolean();
        }
        fieldBlocks[3] = new LongArrayBlock(positionCount, Optional.of(valueIsNull), new long[positionCount]);

        return RowBlock.fromNotNullSuppressedFieldBlocks(positionCount, rowIsNull, fieldBlocks);
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

    private static <T> Block generateBlock(Type type, List<T> data)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, data.size());
        for (T value : data) {
            writeNativeValue(type, blockBuilder, value);
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
