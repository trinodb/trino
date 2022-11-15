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
package io.trino.plugin.iceberg;

import io.airlift.slice.Slice;
import io.trino.Session;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.local.LocalInputFile;
import io.trino.orc.FileOrcDataSource;
import io.trino.orc.OrcDataSource;
import io.trino.orc.OrcReader;
import io.trino.orc.OrcReaderOptions;
import io.trino.orc.metadata.OrcColumnId;
import io.trino.orc.metadata.statistics.StringStatistics;
import io.trino.orc.metadata.statistics.StripeStatistics;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.reader.MetadataReader;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.parquet.TrinoParquetDataSource;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterators.getOnlyElement;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.testing.TestingConnectorSession.SESSION;

public final class IcebergTestUtils
{
    private IcebergTestUtils()
    { }

    public static Session withSmallRowGroups(Session session)
    {
        return Session.builder(session)
                .setCatalogSessionProperty("iceberg", "orc_writer_max_stripe_rows", "10")
                .setCatalogSessionProperty("iceberg", "parquet_writer_page_size", "100B")
                .setCatalogSessionProperty("iceberg", "parquet_writer_block_size", "100B")
                .setCatalogSessionProperty("iceberg", "parquet_writer_batch_size", "10")
                .build();
    }

    public static boolean checkOrcFileSorting(String path, String sortColumnName)
    {
        return checkOrcFileSorting(() -> {
            try {
                return new FileOrcDataSource(new File(path), new OrcReaderOptions());
            }
            catch (FileNotFoundException e) {
                throw new UncheckedIOException(e);
            }
        }, sortColumnName);
    }

    public static boolean checkOrcFileSorting(TrinoFileSystemFactory fileSystemFactory, String path, String sortColumnName)
    {
        return checkOrcFileSorting(() -> {
            try {
                TrinoFileSystem fileSystem = fileSystemFactory.create(SESSION);
                return new TrinoOrcDataSource(fileSystem.newInputFile(path), new OrcReaderOptions(), new FileFormatDataSourceStats());
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }, sortColumnName);
    }

    private static boolean checkOrcFileSorting(Supplier<OrcDataSource> dataSourceSupplier, String sortColumnName)
    {
        OrcReaderOptions readerOptions = new OrcReaderOptions();
        try (OrcDataSource dataSource = dataSourceSupplier.get()) {
            OrcReader orcReader = OrcReader.createOrcReader(dataSource, readerOptions).orElseThrow();
            String previousMax = null;
            OrcColumnId sortColumnId = orcReader.getRootColumn().getNestedColumns().stream()
                    .filter(column -> column.getColumnName().equals(sortColumnName))
                    .collect(onlyElement())
                    .getColumnId();
            List<StripeStatistics> statistics = orcReader.getMetadata().getStripeStatsList().stream()
                    .map(Optional::orElseThrow)
                    .collect(toImmutableList());
            verify(statistics.size() > 1, "Test must produce at least two row groups");

            for (StripeStatistics stripeStatistics : statistics) {
                // TODO: This only works if the sort column is a String
                StringStatistics columnStatistics = stripeStatistics.getColumnStatistics().get(sortColumnId).getStringStatistics();

                Slice minValue = columnStatistics.getMin();
                Slice maxValue = columnStatistics.getMax();
                if (minValue == null || maxValue == null) {
                    throw new IllegalStateException("ORC files must produce min/max stripe statistics");
                }

                if (previousMax != null && previousMax.compareTo(minValue.toStringUtf8()) > 0) {
                    return false;
                }

                previousMax = maxValue.toStringUtf8();
            }

            return true;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static boolean checkParquetFileSorting(String path, String sortColumnName)
    {
        return checkParquetFileSorting(new LocalInputFile(new File(path)), sortColumnName);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static boolean checkParquetFileSorting(TrinoInputFile inputFile, String sortColumnName)
    {
        ParquetMetadata parquetMetadata;
        try {
            parquetMetadata = MetadataReader.readFooter(
                    new TrinoParquetDataSource(inputFile, new ParquetReaderOptions(), new FileFormatDataSourceStats()),
                    Optional.empty());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        Comparable previousMax = null;
        verify(parquetMetadata.getBlocks().size() > 1, "Test must produce at least two row groups");
        for (BlockMetaData blockMetaData : parquetMetadata.getBlocks()) {
            ColumnChunkMetaData columnMetadata = blockMetaData.getColumns().stream()
                    .filter(column -> getOnlyElement(column.getPath().iterator()).equalsIgnoreCase(sortColumnName))
                    .collect(onlyElement());
            if (previousMax != null) {
                if (previousMax.compareTo(columnMetadata.getStatistics().genericGetMin()) > 0) {
                    return false;
                }
            }
            previousMax = columnMetadata.getStatistics().genericGetMax();
        }
        return true;
    }
}
