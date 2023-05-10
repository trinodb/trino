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
import io.trino.parquet.writer.ParquetSchemaConverter;
import io.trino.parquet.writer.ParquetWriter;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import org.apache.parquet.format.CompressionCodec;
import org.joda.time.DateTimeZone;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.TypeUtils.writeNativeValue;

public class ParquetTestUtils
{
    private ParquetTestUtils() {}

    public static Slice writeParquetFile(ParquetWriterOptions writerOptions, List<Type> types, List<String> columnNames, List<io.trino.spi.Page> inputPages)
            throws IOException
    {
        checkArgument(types.size() == columnNames.size());
        ParquetSchemaConverter schemaConverter = new ParquetSchemaConverter(types, columnNames, false, false);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ParquetWriter writer = new ParquetWriter(
                outputStream,
                schemaConverter.getMessageType(),
                schemaConverter.getPrimitiveTypes(),
                writerOptions,
                CompressionCodec.SNAPPY,
                "test-version",
                false,
                Optional.of(DateTimeZone.getDefault()),
                Optional.empty());

        for (io.trino.spi.Page inputPage : inputPages) {
            checkArgument(types.size() == inputPage.getChannelCount());
            writer.write(inputPage);
        }
        writer.close();
        return Slices.wrappedBuffer(outputStream.toByteArray());
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

    private static Block generateBlock(Type type, int positions)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, positions);
        for (int i = 0; i < positions; i++) {
            writeNativeValue(type, blockBuilder, (long) i);
        }
        return blockBuilder.build();
    }
}
