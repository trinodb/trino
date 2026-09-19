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
package io.trino.plugin.hudi;

import io.trino.filesystem.local.LocalInputFile;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hudi.file.HudiBaseFile;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.testing.MaterializedResult;
import io.trino.testing.TestingConnectorSession;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.schema.MessageType;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.ObjIntConsumer;

import static io.trino.plugin.hudi.HudiPageSourceProvider.createPageSource;
import static io.trino.testing.MaterializedResult.materializeSourceDataStream;

/**
 * Drives {@link HudiPageSourceProvider#createPageSource} over a single base file, which is the only path on which
 * the connector enables predicate pushdown - a split carrying log files takes the merge path instead.
 * <p>
 * {@code createPageSource} is package-private, so anything exercising it directly has to live in this package. Three
 * suites do: {@link TestHudiPageSourceProviderTest} for column resolution, {@link TestHudiEvolvedColumnPredicates}
 * for type evolution, and {@link TestHudiSmokeTest} for timestamp precision. They had a writer, a reader, a dynamic
 * filter and a row-group counter each, byte-for-byte the same; keeping one copy is what stops the three from
 * drifting into testing subtly different page sources.
 */
final class TestingBaseFilePageSource
{
    private TestingBaseFilePageSource() {}

    /**
     * Reads the whole file through the page source the connector builds for a split with no log files.
     * <p>
     * The predicate is handed over as the split's, the dynamic filter separately, exactly as
     * {@code HudiPageSourceProvider} receives them, so both routes into {@code getCombinedPredicate} stay reachable
     * from a test.
     */
    static MaterializedResult read(
            Path baseFile,
            List<HiveColumnHandle> projection,
            TupleDomain<HiveColumnHandle> predicate,
            boolean useParquetColumnNames,
            DynamicFilter dynamicFilter)
            throws Exception
    {
        long fileSize = Files.size(baseFile);
        HudiSplit split = new HudiSplit(
                new HudiBaseFile(baseFile.toString(), baseFile.getFileName().toString(), fileSize, 0, 0, fileSize),
                List.of(),
                "000",
                predicate,
                List.of(),
                SplitWeight.standard());
        HudiSessionProperties sessionProperties = new HudiSessionProperties(
                new HudiConfig().setUseParquetColumnNames(useParquetColumnNames),
                new ParquetReaderConfig());
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(sessionProperties.getSessionProperties())
                .build();

        List<Type> types = projection.stream().map(HiveColumnHandle::getType).toList();
        try (ConnectorPageSource pageSource = createPageSource(
                session,
                projection,
                split,
                new LocalInputFile(baseFile.toFile()),
                baseFile.toString(),
                0L,
                fileSize,
                OptionalLong.of(fileSize),
                new FileFormatDataSourceStats(),
                ParquetReaderOptions.builder().build(),
                DateTimeZone.UTC,
                dynamicFilter,
                true)) {
            return materializeSourceDataStream(session, pageSource, types).toTestTypes();
        }
    }

    /**
     * Writes {@code rowCount} rows of {@code schema} to {@code baseFile} and returns how many row groups that took.
     * <p>
     * The writer flushes a row group whenever the buffered size passes {@code withRowGroupSize}, checked every
     * {@code parquet.page.size.row.check.min} records (100 by default), so the small sizes below are what actually
     * split the file. Callers assert on the returned count rather than on those knobs: with a single row group there
     * would be nothing to prune and every "still prunes" assertion would hold without proving anything.
     * <p>
     * {@code bloomFilterColumns} names the columns to build a parquet bloom filter for, which is what a Hudi writer
     * does for every column set in {@code parquet.bloom.filter.enabled#<column>}. Trino reads bloom filters by
     * default, so a column listed here is checked by {@code TupleDomainParquetPredicate.matches(BloomFilterStore,
     * int)} as well as by the statistics.
     */
    static int writeBaseFile(Path baseFile, MessageType schema, int rowCount, List<String> bloomFilterColumns, ObjIntConsumer<Group> fillRow)
            throws IOException
    {
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(new LocalOutputFile(baseFile))
                .withType(schema)
                .withConf(new PlainParquetConfiguration())
                .withRowGroupSize(1024L)
                .withPageSize(512);
        for (String column : bloomFilterColumns) {
            builder = builder.withBloomFilterEnabled(column, true);
        }
        try (ParquetWriter<Group> writer = builder.build()) {
            for (int row = 0; row < rowCount; row++) {
                Group group = groupFactory.newGroup();
                fillRow.accept(group, row);
                writer.write(group);
            }
        }
        return rowGroupCount(baseFile);
    }

    static int writeBaseFile(Path baseFile, MessageType schema, int rowCount, ObjIntConsumer<Group> fillRow)
            throws IOException
    {
        return writeBaseFile(baseFile, schema, rowCount, List.of(), fillRow);
    }

    static int rowGroupCount(Path path)
            throws IOException
    {
        try (ParquetFileReader reader = ParquetFileReader.open(new org.apache.parquet.io.LocalInputFile(path))) {
            return reader.getRowGroups().size();
        }
    }

    /**
     * A completed dynamic filter carrying {@code predicate}, the shape {@code getCombinedPredicate} intersects in.
     */
    static DynamicFilter dynamicFilterOn(TupleDomain<HiveColumnHandle> predicate)
    {
        return new DynamicFilter()
        {
            @Override
            public Set<ColumnHandle> getColumnsCovered()
            {
                return Set.copyOf(predicate.getDomains().orElseThrow().keySet());
            }

            @Override
            public CompletableFuture<?> isBlocked()
            {
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public boolean isComplete()
            {
                return true;
            }

            @Override
            public boolean isAwaitable()
            {
                return false;
            }

            @Override
            public TupleDomain<ColumnHandle> getCurrentPredicate()
            {
                return predicate.transformKeys(ColumnHandle.class::cast);
            }
        };
    }
}
