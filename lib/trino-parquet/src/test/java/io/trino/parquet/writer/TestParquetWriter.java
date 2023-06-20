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
package io.trino.parquet.writer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import io.airlift.units.DataSize;
import io.trino.parquet.DataPage;
import io.trino.parquet.DiskRange;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.reader.ChunkedInputStream;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.PageReader;
import io.trino.parquet.reader.TestingParquetDataSource;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import org.apache.parquet.VersionParser;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.PrimitiveType;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTestUtils.generateInputPages;
import static io.trino.parquet.ParquetTestUtils.writeParquetFile;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TinyintType.TINYINT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.assertj.core.api.Assertions.assertThat;

public class TestParquetWriter
{
    @Test
    public void testCreatedByIsParsable()
            throws VersionParser.VersionParseException, IOException
    {
        String createdBy = ParquetWriter.formatCreatedBy("test-version");
        // createdBy must start with "parquet-mr" to make Apache Hive perform timezone conversion on INT96 timestamps correctly
        // when hive.parquet.timestamp.skip.conversion is set to true.
        // Apache Hive 3.2 and above enable hive.parquet.timestamp.skip.conversion by default
        assertThat(createdBy).startsWith("parquet-mr");
        VersionParser.ParsedVersion version = VersionParser.parse(createdBy);
        assertThat(version).isNotNull();
        assertThat(version.application).isEqualTo("parquet-mr-trino");
        assertThat(version.version).isEqualTo("test-version");
        assertThat(version.appBuildHash).isEqualTo("n/a");
    }

    @Test
    public void testWrittenPageSize()
            throws IOException
    {
        List<String> columnNames = ImmutableList.of("columnA", "columnB");
        List<Type> types = ImmutableList.of(INTEGER, BIGINT);

        // Write a file with many small input pages and parquet max page size of 20Kb
        ParquetDataSource dataSource = new TestingParquetDataSource(
                writeParquetFile(
                        ParquetWriterOptions.builder()
                                .setMaxPageSize(DataSize.ofBytes(20 * 1024))
                                .build(),
                        types,
                        columnNames,
                        generateInputPages(types, 100, 1000)),
                new ParquetReaderOptions());
        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        assertThat(parquetMetadata.getBlocks().size()).isEqualTo(1);
        assertThat(parquetMetadata.getBlocks().get(0).getRowCount()).isEqualTo(100 * 1000);

        ColumnChunkMetaData chunkMetaData = parquetMetadata.getBlocks().get(0).getColumns().get(0);
        DiskRange range = new DiskRange(chunkMetaData.getStartingPos(), chunkMetaData.getTotalSize());
        Map<Integer, ChunkedInputStream> chunkReader = dataSource.planRead(ImmutableListMultimap.of(0, range), newSimpleAggregatedMemoryContext());

        PageReader pageReader = PageReader.createPageReader(
                chunkReader.get(0),
                chunkMetaData,
                new ColumnDescriptor(new String[] {"columna"}, new PrimitiveType(REQUIRED, INT32, "columna"), 0, 0),
                null,
                Optional.empty());

        pageReader.readDictionaryPage();
        assertThat(pageReader.hasNext()).isTrue();
        int pagesRead = 0;
        DataPage dataPage;
        while (pageReader.hasNext()) {
            dataPage = pageReader.readPage();
            pagesRead++;
            if (!pageReader.hasNext()) {
                break; // skip last page size validation
            }
            assertThat(dataPage.getValueCount()).isBetween(4500, 5500);
        }
        assertThat(pagesRead).isGreaterThan(10);
    }

    @Test
    public void testColumnReordering()
            throws IOException
    {
        List<String> columnNames = ImmutableList.of("columnA", "columnB", "columnC", "columnD");
        List<Type> types = ImmutableList.of(BIGINT, TINYINT, INTEGER, DecimalType.createDecimalType(12));

        // Write a file with many row groups
        ParquetDataSource dataSource = new TestingParquetDataSource(
                writeParquetFile(
                        ParquetWriterOptions.builder()
                                .setMaxBlockSize(DataSize.ofBytes(20 * 1024))
                                .build(),
                        types,
                        columnNames,
                        generateInputPages(types, 100, 100)),
                new ParquetReaderOptions());

        ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
        assertThat(parquetMetadata.getBlocks().size()).isGreaterThanOrEqualTo(10);
        for (BlockMetaData blockMetaData : parquetMetadata.getBlocks()) {
            // Verify that the columns are stored in the same order as the metadata
            List<Long> offsets = blockMetaData.getColumns().stream()
                    .map(ColumnChunkMetaData::getFirstDataPageOffset)
                    .collect(toImmutableList());
            assertThat(offsets).isSorted();
        }
    }
}
