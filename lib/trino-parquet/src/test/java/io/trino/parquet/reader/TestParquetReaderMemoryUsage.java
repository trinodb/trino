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
package io.trino.parquet.reader;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.spi.Page;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.type.Type;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTestUtils.createParquetReader;
import static io.trino.parquet.ParquetTestUtils.generateInputPages;
import static io.trino.parquet.ParquetTestUtils.writeParquetFile;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TestParquetReaderMemoryUsage
{
    @Test
    public void testColumnReaderMemoryUsage()
            throws IOException
    {
        // Write a file with 100 rows per row-group
        List<String> columnNames = ImmutableList.of("columnA", "columnB");
        List<Type> types = ImmutableList.of(INTEGER, BIGINT);

        ParquetDataSource dataSource = new TestingParquetDataSource(
                writeParquetFile(
                        ParquetWriterOptions.builder()
                                .setMaxBlockSize(DataSize.ofBytes(1000))
                                .build(),
                        types,
                        columnNames,
                        generateInputPages(types, 100, 5)),
                new ParquetReaderOptions());
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        assertThat(parquetMetadata.getBlocks().size()).isGreaterThan(1);
        // Verify file has only non-dictionary encodings as dictionary memory usage is already tested in TestFlatColumnReader#testMemoryUsage
        parquetMetadata.getBlocks().forEach(block -> {
            block.getColumns()
                    .forEach(columnChunkMetaData -> assertThat(columnChunkMetaData.getEncodingStats().hasDictionaryEncodedPages()).isFalse());
            assertThat(block.getRowCount()).isEqualTo(100);
        });

        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        ParquetReader reader = createParquetReader(dataSource, parquetMetadata, memoryContext, types, columnNames);

        Page page = reader.nextPage();
        assertThat(page.getBlock(0)).isInstanceOf(LazyBlock.class);
        assertThat(memoryContext.getBytes()).isEqualTo(0);
        page.getBlock(0).getLoadedBlock();
        // Memory usage due to reading data and decoding parquet page of 1st block
        long initialMemoryUsage = memoryContext.getBytes();
        assertThat(initialMemoryUsage).isGreaterThan(0);

        // Memory usage due to decoding parquet page of 2nd block
        page.getBlock(1).getLoadedBlock();
        long currentMemoryUsage = memoryContext.getBytes();
        assertThat(currentMemoryUsage).isGreaterThan(initialMemoryUsage);

        // Memory usage does not change until next row group (1 page per row-group)
        long rowGroupRowCount = parquetMetadata.getBlocks().get(0).getRowCount();
        int rowsRead = page.getPositionCount();
        while (rowsRead < rowGroupRowCount) {
            rowsRead += reader.nextPage().getPositionCount();
            if (rowsRead <= rowGroupRowCount) {
                assertThat(memoryContext.getBytes()).isEqualTo(currentMemoryUsage);
            }
        }

        // New row-group should release memory from old column readers, while using some memory for data read
        reader.nextPage();
        assertThat(memoryContext.getBytes()).isBetween(1L, currentMemoryUsage - 1);

        reader.close();
        assertThat(memoryContext.getBytes()).isEqualTo(0);
    }
}
