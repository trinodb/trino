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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multisets;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.iceberg.util.FileOperationUtils.FileOperation;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionStatisticsFile;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.OptionalLong;

import static com.google.common.collect.ImmutableMultiset.toImmutableMultiset;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.plugin.iceberg.IcebergTestUtils.loadTable;
import static io.trino.plugin.iceberg.util.FileOperationUtils.FileType.MANIFEST;
import static io.trino.plugin.iceberg.util.FileOperationUtils.FileType.METADATA_JSON;
import static io.trino.plugin.iceberg.util.FileOperationUtils.FileType.PARTITION_STATS;
import static io.trino.plugin.iceberg.util.FileOperationUtils.FileType.SNAPSHOT;
import static io.trino.plugin.iceberg.util.FileOperationUtils.Scope.METADATA_FILES;
import static io.trino.plugin.iceberg.util.FileOperationUtils.getOperations;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static org.apache.iceberg.FileFormat.AVRO;
import static org.assertj.core.api.Assertions.assertThat;

@Execution(ExecutionMode.SAME_THREAD)
public class TestIcebergPartitionStatisticsFileOperations
        extends AbstractTestQueryFramework
{
    private HiveMetastore metastore;
    private TrinoFileSystemFactory fileSystemFactory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("iceberg")
                .setSchema("test_schema")
                .build();

        QueryRunner queryRunner = DistributedQueryRunner.builder(session)
                // a single node, so that each file is read once
                .setWorkerCount(0)
                .build();

        Path dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data");
        dataDirectory.toFile().mkdirs();
        queryRunner.installPlugin(new TestingIcebergPlugin(dataDirectory));
        queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", ImmutableMap.<String, String>builder()
                .put("iceberg.split-manager-threads", "0")
                .put("iceberg.metadata-cache.enabled", "false")
                // tests that write a partition statistics file expect it to be used however few data files there are
                .put("iceberg.partition-statistics.min-data-files", "0")
                .buildOrThrow());

        metastore = getHiveMetastore(queryRunner);

        queryRunner.execute("CREATE SCHEMA test_schema");

        return queryRunner;
    }

    @BeforeAll
    public void initFileSystemFactory()
    {
        fileSystemFactory = getFileSystemFactory(getDistributedQueryRunner());
    }

    @Test
    public void testShowStatsUnfilteredUsesSnapshotSummaryWhenPartitionStatisticsExist()
            throws Exception
    {
        assertUpdate("CREATE TABLE test_stats_unfiltered (id integer, part integer) WITH (format_version = 3, partitioning = ARRAY['part'])");
        assertUpdate("INSERT INTO test_stats_unfiltered VALUES (1, 1), (2, 2)", 2);
        assertUpdate("INSERT INTO test_stats_unfiltered VALUES (3, 1), (4, 2)", 2);

        // No partition statistics file: every manifest is read
        assertFileSystemAccesses("SHOW STATS FOR test_stats_unfiltered", ImmutableMultiset.<FileOperation>builder()
                .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                .addCopies(new FileOperation(MANIFEST, "InputFile.newStream"), 2)
                .build());

        writePartitionStatistics("test_stats_unfiltered");

        // File present, no predicate and no position deletes: snapshot summary, one sampled manifest, file itself not read
        assertFileSystemAccesses("SHOW STATS FOR test_stats_unfiltered", ImmutableMultiset.<FileOperation>builder()
                .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                .add(new FileOperation(MANIFEST, "InputFile.newStream"))
                .build());
        assertThat(showStatsRowCount("test_stats_unfiltered")).isEqualTo(4.0);
        assertUpdate("DROP TABLE test_stats_unfiltered");
    }

    @Test
    public void testShowStatsUnfilteredWithDeletionVectorUsesPartitionStatistics()
            throws Exception
    {
        assertUpdate("CREATE TABLE test_stats_unfiltered_dv (id integer, part integer) WITH (format_version = 3, partitioning = ARRAY['part'])");
        assertUpdate("INSERT INTO test_stats_unfiltered_dv VALUES (1, 1), (2, 1), (3, 2)", 3);
        assertUpdate("INSERT INTO test_stats_unfiltered_dv VALUES (4, 1), (5, 2)", 2);
        // a deletion vector, so that a fallback to the manifest walk cannot go unnoticed in the row count
        assertUpdate("DELETE FROM test_stats_unfiltered_dv WHERE id = 2", 1);
        writePartitionStatistics("test_stats_unfiltered_dv");

        // the snapshot summary does not tell deletion vectors from position delete files, so the file is read instead
        assertFileSystemAccesses("SHOW STATS FOR test_stats_unfiltered_dv", ImmutableMultiset.<FileOperation>builder()
                .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                .add(new FileOperation(MANIFEST, "InputFile.newStream"))
                .add(new FileOperation(PARTITION_STATS, "InputFile.length"))
                .add(new FileOperation(PARTITION_STATS, "InputFile.newStream"))
                .build());
        assertThat(showStatsRowCount("test_stats_unfiltered_dv")).isEqualTo(4.0);
        assertUpdate("DROP TABLE test_stats_unfiltered_dv");
    }

    @Test
    public void testPartitionStatisticsDisabledBySessionProperty()
            throws Exception
    {
        assertUpdate("CREATE TABLE test_stats_disabled (id integer, part integer) WITH (format_version = 3, partitioning = ARRAY['part'])");
        assertUpdate("INSERT INTO test_stats_disabled VALUES (1, 1), (2, 1), (3, 2)", 3);
        assertUpdate("INSERT INTO test_stats_disabled VALUES (4, 1), (5, 2)", 2);
        // a deletion vector, so that a fallback to the manifest walk cannot go unnoticed in the row count
        assertUpdate("DELETE FROM test_stats_disabled WHERE id = 2", 1);
        writePartitionStatistics("test_stats_disabled");

        Session disabled = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "partition_statistics_enabled", "false")
                .build();
        // File present but disabled: every manifest is read, the file is not
        assertFileSystemAccesses(disabled, "SHOW STATS FOR (SELECT * FROM test_stats_disabled WHERE part = 1)", ImmutableMultiset.<FileOperation>builder()
                .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                .addCopies(new FileOperation(MANIFEST, "InputFile.newStream"), 2)
                .build());
        assertUpdate("DROP TABLE test_stats_disabled");
    }

    @Test
    public void testPartitionStatisticsDefaultThreshold()
            throws Exception
    {
        assertUpdate("CREATE TABLE test_stats_threshold (id integer, part integer) WITH (format_version = 3, partitioning = ARRAY['part'])");
        assertUpdate("INSERT INTO test_stats_threshold VALUES (1, 1), (2, 1), (3, 2)", 3);
        assertUpdate("INSERT INTO test_stats_threshold VALUES (4, 1), (5, 2)", 2);
        // a deletion vector, so that a fallback to the manifest walk cannot go unnoticed in the row count
        assertUpdate("DELETE FROM test_stats_threshold WHERE id = 2", 1);
        writePartitionStatistics("test_stats_threshold");

        // the test catalog lowers the threshold to 0; the default of 10000 is far above the 4 data files of this table
        Session defaultThreshold = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "partition_statistics_min_data_files", "10000")
                .build();
        assertFileSystemAccesses(defaultThreshold, "SHOW STATS FOR (SELECT * FROM test_stats_threshold WHERE part = 1)", ImmutableMultiset.<FileOperation>builder()
                .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                .addCopies(new FileOperation(MANIFEST, "InputFile.newStream"), 2)
                .build());
        // the unfiltered row count does not come from the snapshot summary either
        assertFileSystemAccesses(defaultThreshold, "SHOW STATS FOR test_stats_threshold", ImmutableMultiset.<FileOperation>builder()
                .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                .addCopies(new FileOperation(MANIFEST, "InputFile.newStream"), 2)
                .build());

        // one more than the 4 data files of this table
        Session aboveThreshold = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "partition_statistics_min_data_files", "5")
                .build();
        assertFileSystemAccesses(aboveThreshold, "SHOW STATS FOR (SELECT * FROM test_stats_threshold WHERE part = 1)", ImmutableMultiset.<FileOperation>builder()
                .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                .addCopies(new FileOperation(MANIFEST, "InputFile.newStream"), 2)
                .build());

        // exactly as many data files as the threshold
        Session exactThreshold = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "partition_statistics_min_data_files", "4")
                .build();
        assertFileSystemAccesses(
                exactThreshold,
                "SHOW STATS FOR (SELECT * FROM test_stats_threshold WHERE part = 1)",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .add(new FileOperation(MANIFEST, "InputFile.newStream"))
                        .add(new FileOperation(PARTITION_STATS, "InputFile.length"))
                        .add(new FileOperation(PARTITION_STATS, "InputFile.newStream"))
                        .build());

        // threshold 0 uses the file as well, now from the cache
        Session alwaysUse = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "partition_statistics_min_data_files", "0")
                .build();
        assertFileSystemAccesses(
                alwaysUse,
                "SHOW STATS FOR (SELECT * FROM test_stats_threshold WHERE part = 1)",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .add(new FileOperation(MANIFEST, "InputFile.newStream"))
                        .build());
        assertUpdate("DROP TABLE test_stats_threshold");
    }

    @ParameterizedTest
    @EnumSource(value = FileFormat.class, names = {"AVRO", "PARQUET"})
    public void testShowStatsFilteredUsesPartitionStatistics(FileFormat format)
            throws Exception
    {
        String table = "test_stats_filtered_" + format.name().toLowerCase(ENGLISH);
        assertUpdate("CREATE TABLE " + table + " (id integer, part integer) WITH (format_version = 3, partitioning = ARRAY['part'])");
        assertUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 1)", 2);
        assertUpdate("INSERT INTO " + table + " VALUES (3, 2)", 1);
        assertUpdate("INSERT INTO " + table + " VALUES (4, 3)", 1);
        assertUpdate("DELETE FROM " + table + " WHERE id = 2", 1);
        writePartitionStatistics(table, format);

        // first filtered query reads the partition statistics file and one sampled manifest
        assertFileSystemAccesses("SHOW STATS FOR (SELECT * FROM " + table + " WHERE part = 1)", ImmutableMultiset.<FileOperation>builder()
                .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                .add(new FileOperation(MANIFEST, "InputFile.newStream"))
                .addAll(partitionStatisticsReads(format))
                .build());
        assertThat(showStatsRowCount(table + " WHERE part = 1")).isEqualTo(1.0);

        // $path predicates cannot be answered from partition statistics
        assertFileSystemAccesses(
                "SHOW STATS FOR (SELECT * FROM " + table + " WHERE \"$path\" IS NOT NULL)",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .addCopies(new FileOperation(MANIFEST, "InputFile.newStream"), 3)
                        .build());

        // the parsed file is cached
        assertFileSystemAccesses("SHOW STATS FOR (SELECT * FROM " + table + " WHERE part = 2)", ImmutableMultiset.<FileOperation>builder()
                .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                .add(new FileOperation(MANIFEST, "InputFile.newStream"))
                .build());

        // without projected columns, no manifest is sampled
        assertFileSystemAccesses(
                "SHOW STATS FOR (SELECT 1 FROM " + table + " WHERE part = 3)",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .build());
        assertUpdate("DROP TABLE " + table);
    }

    @Test
    public void testPartitionStatisticsCacheIsBounded()
            throws Exception
    {
        assertUpdate("CREATE TABLE test_stats_cache_bound (id integer, part integer) WITH (format_version = 3, partitioning = ARRAY['part'])");
        assertUpdate("INSERT INTO test_stats_cache_bound VALUES (1, 1)", 1);
        assertUpdate("INSERT INTO test_stats_cache_bound VALUES (2, 1), (3, 1)", 2);
        assertUpdate("DELETE FROM test_stats_cache_bound WHERE id = 3", 1);
        writePartitionStatistics("test_stats_cache_bound");

        // the file fits in the cache, so only the first estimate reads it
        PartitionStatisticsReader reader = new PartitionStatisticsReader();
        assertMultisetsEqual(
                estimateRowCountFileOperations("test_stats_cache_bound", reader),
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .add(new FileOperation(PARTITION_STATS, "InputFile.length"))
                        .add(new FileOperation(PARTITION_STATS, "InputFile.newStream"))
                        .build());
        assertMultisetsEqual(
                estimateRowCountFileOperations("test_stats_cache_bound", reader),
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .build());

        // the file does not fit, so its rows are not cached, and each estimate reads every manifest instead; reading
        // stops at the first row over the budget and is not repeated
        PartitionStatisticsReader smallReader = new PartitionStatisticsReader(DataSize.ofBytes(1));
        Multiset<FileOperation> walk = ImmutableMultiset.<FileOperation>builder()
                .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                .addCopies(new FileOperation(MANIFEST, "InputFile.newStream"), 2)
                .build();
        assertMultisetsEqual(
                estimateRowCountFileOperations("test_stats_cache_bound", smallReader),
                ImmutableMultiset.<FileOperation>builder()
                        .addAll(walk)
                        .add(new FileOperation(PARTITION_STATS, "InputFile.length"))
                        .add(new FileOperation(PARTITION_STATS, "InputFile.newStream"))
                        .build());
        assertMultisetsEqual(estimateRowCountFileOperations("test_stats_cache_bound", smallReader), walk);
        assertUpdate("DROP TABLE test_stats_cache_bound");
    }

    @Test
    public void testPartitionStatisticsCorruptFileFallsBack()
            throws Exception
    {
        assertUpdate("CREATE TABLE test_stats_corrupt (id integer, part integer) WITH (format_version = 3, partitioning = ARRAY['part'])");
        assertUpdate("INSERT INTO test_stats_corrupt VALUES (1, 1), (2, 1), (3, 2)", 3);
        assertUpdate("INSERT INTO test_stats_corrupt VALUES (4, 1), (5, 2)", 2);
        assertUpdate("DELETE FROM test_stats_corrupt WHERE id = 2", 1);
        BaseTable table = loadTable("test_stats_corrupt", metastore, fileSystemFactory, "iceberg", "test_schema");
        PartitionStatisticsFile file = IcebergTestUtils.writePartitionStatistics(table, AVRO);
        fileSystemFactory.create(SESSION).newOutputFile(Location.of(file.path())).createOrOverwrite("not an avro file".getBytes(UTF_8));

        // the file cannot be read: the manifest walk answers (it ignores the deletion vector), after an attempt to read the file
        assertThat(showStatsRowCount("test_stats_corrupt WHERE part = 1")).isEqualTo(3.0);

        // the failure is remembered: the file is not read again
        assertFileSystemAccesses(
                "SHOW STATS FOR (SELECT * FROM test_stats_corrupt WHERE part = 2)",
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(METADATA_JSON, "InputFile.newStream"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.length"))
                        .add(new FileOperation(SNAPSHOT, "InputFile.newStream"))
                        .addCopies(new FileOperation(MANIFEST, "InputFile.newStream"), 2)
                        .build());
        assertUpdate("DROP TABLE test_stats_corrupt");
    }

    // estimates outside a query, so that each estimate can use its own reader
    private Multiset<FileOperation> estimateRowCountFileOperations(String tableName, PartitionStatisticsReader reader)
    {
        BaseTable table = loadTable(tableName, metastore, fileSystemFactory, "iceberg", "test_schema");
        // a predicate, so that the row count does not come from the snapshot summary
        IcebergColumnHandle part = IcebergColumnHandle.optional(ColumnIdentity.primitiveColumnIdentity(table.schema().findField("part").fieldId(), "part"))
                .columnType(INTEGER)
                .build();
        Multiset<FileOperation> before = getOperations(getDistributedQueryRunner().getSpans());
        TableStatisticsReader.makeTableStatistics(
                TESTING_TYPE_MANAGER,
                table,
                OptionalLong.of(table.currentSnapshot().snapshotId()),
                TupleDomain.all(),
                TupleDomain.withColumnDomains(ImmutableMap.of(part, Domain.singleValue(INTEGER, 1L))),
                ImmutableSet.of(),
                newDirectExecutorService(),
                reader,
                true,
                0);
        return ImmutableMultiset.copyOf(Multisets.difference(getOperations(getDistributedQueryRunner().getSpans()), before));
    }

    private double showStatsRowCount(String from)
    {
        return (double) computeActual("SHOW STATS FOR (SELECT * FROM " + from + ")").getMaterializedRows().stream()
                .filter(row -> row.getField(0) == null)
                .findFirst()
                .orElseThrow()
                .getField(4);
    }

    private void writePartitionStatistics(String tableName)
            throws IOException
    {
        writePartitionStatistics(tableName, AVRO);
    }

    private void writePartitionStatistics(String tableName, FileFormat format)
            throws IOException
    {
        IcebergTestUtils.writePartitionStatistics(loadTable(tableName, metastore, fileSystemFactory, "iceberg", "test_schema"), format);
    }

    // the library reads Avro files as a stream, and Parquet files with Trino's readers
    private static Multiset<FileOperation> partitionStatisticsReads(FileFormat format)
    {
        return switch (format) {
            case AVRO -> ImmutableMultiset.of(
                    new FileOperation(PARTITION_STATS, "InputFile.length"),
                    new FileOperation(PARTITION_STATS, "InputFile.newStream"));
            case PARQUET -> ImmutableMultiset.of(
                    new FileOperation(PARTITION_STATS, "InputFile.length"),
                    new FileOperation(PARTITION_STATS, "InputFile.newInput"));
            default -> throw new IllegalArgumentException("Unsupported format: " + format);
        };
    }

    private void assertFileSystemAccesses(@Language("SQL") String query, Multiset<FileOperation> expectedAccesses)
    {
        assertFileSystemAccesses(getSession(), query, expectedAccesses);
    }

    private synchronized void assertFileSystemAccesses(Session session, @Language("SQL") String query, Multiset<FileOperation> expectedAccesses)
    {
        getDistributedQueryRunner().executeWithPlan(session, query);
        assertMultisetsEqual(
                getOperations(getDistributedQueryRunner().getSpans()).stream()
                        .filter(METADATA_FILES)
                        .collect(toImmutableMultiset()),
                expectedAccesses);
    }
}
