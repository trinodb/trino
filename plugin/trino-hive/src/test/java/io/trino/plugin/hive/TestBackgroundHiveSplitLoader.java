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
package io.trino.plugin.hive;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import com.google.common.io.Resources;
import io.airlift.stats.CounterStat;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.cache.DefaultCachingHostAddressProvider;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.plugin.hive.HiveColumnHandle.ColumnType;
import io.trino.plugin.hive.fs.CachingDirectoryLister;
import io.trino.plugin.hive.fs.DirectoryLister;
import io.trino.plugin.hive.fs.TrinoFileStatus;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.util.HiveBucketing.HiveBucketFilter;
import io.trino.plugin.hive.util.InternalHiveSplitFactory;
import io.trino.plugin.hive.util.ValidWriteIdList;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitSource.ConnectorSplitBatch;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.io.Resources.getResource;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.hive.formats.HiveClassNames.SYMLINK_TEXT_INPUT_FORMAT_CLASS;
import static io.trino.hive.thrift.metastore.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static io.trino.plugin.hive.BackgroundHiveSplitLoader.BucketSplitInfo.createBucketSplitInfo;
import static io.trino.plugin.hive.BackgroundHiveSplitLoader.getBucketNumber;
import static io.trino.plugin.hive.BackgroundHiveSplitLoader.hasAttemptId;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveColumnHandle.pathColumnHandle;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_BUCKET_FILES;
import static io.trino.plugin.hive.HiveStorageFormat.AVRO;
import static io.trino.plugin.hive.HiveStorageFormat.CSV;
import static io.trino.plugin.hive.HiveStorageFormat.ORC;
import static io.trino.plugin.hive.HiveTableProperties.TRANSACTIONAL;
import static io.trino.plugin.hive.HiveTestUtils.SESSION;
import static io.trino.plugin.hive.HiveTestUtils.getHiveSession;
import static io.trino.plugin.hive.HiveTimestampPrecision.DEFAULT_PRECISION;
import static io.trino.plugin.hive.HiveType.HIVE_INT;
import static io.trino.plugin.hive.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.TableType.MANAGED_TABLE;
import static io.trino.plugin.hive.util.HiveBucketing.BucketingVersion.BUCKETING_V1;
import static io.trino.plugin.hive.util.HiveUtil.getRegularColumnHandles;
import static io.trino.plugin.hive.util.SerdeConstants.FOOTER_COUNT;
import static io.trino.plugin.hive.util.SerdeConstants.HEADER_COUNT;
import static io.trino.plugin.hive.util.SerdeConstants.SERIALIZATION_LIB;
import static io.trino.spi.predicate.TupleDomain.withColumnDomains;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestBackgroundHiveSplitLoader
{
    private static final int BUCKET_COUNT = 2;

    private static final Location LOCATION = Location.of("memory:///db_name/table_name/000000_0");
    private static final Location FILTERED_LOCATION = Location.of("memory:///db_name/table_name/000000_1");

    private static final TupleDomain<HiveColumnHandle> LOCATION_DOMAIN = withColumnDomains(Map.of(pathColumnHandle(), Domain.singleValue(VARCHAR, utf8Slice(LOCATION.toString()))));

    private static final List<Location> TEST_LOCATIONS = List.of(LOCATION, FILTERED_LOCATION);

    private static final List<Column> PARTITION_COLUMNS = List.of(new Column("partitionColumn", HIVE_INT, Optional.empty(), Map.of()));
    private static final List<HiveColumnHandle> BUCKET_COLUMN_HANDLES = List.of(createBaseColumn("col1", 0, HIVE_INT, INTEGER, ColumnType.REGULAR, Optional.empty()));

    private static final String TABLE_PATH = "memory:///db_name/table_name";
    private static final Table SIMPLE_TABLE = table(TABLE_PATH, List.of(), Optional.empty(), Map.of());
    private static final Table PARTITIONED_TABLE = table(TABLE_PATH, PARTITION_COLUMNS, Optional.of(new HiveBucketProperty(List.of("col1"), BUCKET_COUNT, List.of())), Map.of());

    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));

    @AfterAll
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    public void testNoPathFilter()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(TEST_LOCATIONS, TupleDomain.none());

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);

        assertThat(drain(hiveSplitSource).size()).isEqualTo(2);
    }

    @Test
    public void testCsv()
            throws Exception
    {
        FileEntry file = new FileEntry(LOCATION, DataSize.of(2, GIGABYTE).toBytes(), Instant.now(), Optional.empty());
        assertCsvSplitCount(file, Map.of(), 33);
        assertCsvSplitCount(file, Map.of(HEADER_COUNT, "1"), 33);
        assertCsvSplitCount(file, Map.of(HEADER_COUNT, "2"), 1);
        assertCsvSplitCount(file, Map.of(FOOTER_COUNT, "1"), 1);
        assertCsvSplitCount(file, Map.of(HEADER_COUNT, "1", FOOTER_COUNT, "1"), 1);
    }

    private void assertCsvSplitCount(FileEntry file, Map<String, String> tableProperties, int expectedSplitCount)
            throws Exception
    {
        Table table = table(
                TABLE_PATH,
                List.of(),
                Optional.empty(),
                Map.copyOf(tableProperties),
                StorageFormat.fromHiveStorageFormat(CSV));

        TrinoFileSystemFactory fileSystemFactory = new ListSingleFileFileSystemFactory(file);
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                fileSystemFactory,
                TupleDomain.all(),
                Optional.empty(),
                table,
                Optional.empty(),
                Optional.empty());

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);

        assertThat(drainSplits(hiveSplitSource).size()).isEqualTo(expectedSplitCount);
    }

    @Test
    public void testPathFilter()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                TEST_LOCATIONS,
                LOCATION_DOMAIN);

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);
        List<String> paths = drain(hiveSplitSource);
        assertThat(paths.size()).isEqualTo(1);
        assertThat(paths.get(0)).isEqualTo(LOCATION.toString());
    }

    @Test
    public void testPathFilterOneBucketMatchPartitionedTable()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                TEST_LOCATIONS,
                LOCATION_DOMAIN,
                Optional.of(new HiveBucketFilter(Set.of(0, 1))),
                PARTITIONED_TABLE,
                Optional.of(new HiveBucketHandle(BUCKET_COLUMN_HANDLES, BUCKETING_V1, BUCKET_COUNT, BUCKET_COUNT, List.of())));

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);
        List<String> paths = drain(hiveSplitSource);
        assertThat(paths.size()).isEqualTo(1);
        assertThat(paths.get(0)).isEqualTo(LOCATION.toString());
    }

    @Test
    public void testPathFilterBucketedPartitionedTable()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                TEST_LOCATIONS,
                LOCATION_DOMAIN,
                Optional.empty(),
                PARTITIONED_TABLE,
                Optional.of(
                        new HiveBucketHandle(
                                getRegularColumnHandles(PARTITIONED_TABLE, TESTING_TYPE_MANAGER, DEFAULT_PRECISION),
                                BUCKETING_V1,
                                BUCKET_COUNT,
                                BUCKET_COUNT,
                                List.of())));

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);
        List<String> paths = drain(hiveSplitSource);
        assertThat(paths.size()).isEqualTo(1);
        assertThat(paths.get(0)).isEqualTo(LOCATION.toString());
    }

    @Test
    public void testEmptyFileWithNoBlocks()
            throws Exception
    {
        TrinoFileSystemFactory fileSystemFactory = new MemoryFileSystemFactory();
        // create an empty file
        fileSystemFactory.create(ConnectorIdentity.ofUser("test")).newOutputFile(LOCATION).create().close();

        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                fileSystemFactory,
                TupleDomain.none(),
                Optional.empty(),
                SIMPLE_TABLE,
                Optional.empty(),
                Optional.empty());

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);

        List<HiveSplit> splits = drainSplits(hiveSplitSource);
        assertThat(splits.size()).isEqualTo(0);
    }

    @Test
    public void testNoHangIfPartitionIsOffline()
            throws IOException
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoaderOfflinePartitions();
        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);

        assertThatThrownBy(() -> drain(hiveSplitSource))
                .isInstanceOf(TrinoException.class)
                .hasMessage("OFFLINE");
        assertThatThrownBy(hiveSplitSource::isFinished)
                .isInstanceOf(TrinoException.class)
                .hasMessage("OFFLINE");
    }

    @Test
    @Timeout(30)
    public void testIncompleteDynamicFilterTimeout()
            throws Exception
    {
        CompletableFuture<?> isBlocked = new CompletableFuture<>();
        try {
            BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                    new DynamicFilter()
                    {
                        @Override
                        public Set<ColumnHandle> getColumnsCovered()
                        {
                            return Set.of();
                        }

                        @Override
                        public CompletableFuture<?> isBlocked()
                        {
                            return isBlocked;
                        }

                        @Override
                        public boolean isComplete()
                        {
                            return false;
                        }

                        @Override
                        public boolean isAwaitable()
                        {
                            return true;
                        }

                        @Override
                        public TupleDomain<ColumnHandle> getCurrentPredicate()
                        {
                            return TupleDomain.all();
                        }
                    },
                    new Duration(1, SECONDS));
            HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
            backgroundHiveSplitLoader.start(hiveSplitSource);

            assertThat(drain(hiveSplitSource).size()).isEqualTo(2);
            assertThat(hiveSplitSource.isFinished()).isTrue();
        }
        finally {
            isBlocked.complete(null);
        }
    }

    @Test
    public void testCachedDirectoryLister()
            throws Exception
    {
        CachingDirectoryLister cachingDirectoryLister = new CachingDirectoryLister(new Duration(5, TimeUnit.MINUTES), DataSize.of(100, KILOBYTE), List.of("test_dbname.test_table"));
        assertThat(cachingDirectoryLister.getRequestCount()).isEqualTo(0);

        int totalCount = 100;
        CountDownLatch firstVisit = new CountDownLatch(1);
        List<Future<List<HiveSplit>>> futures = new ArrayList<>();

        futures.add(executor.submit(() -> {
            BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(TEST_LOCATIONS, cachingDirectoryLister);
            HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
            backgroundHiveSplitLoader.start(hiveSplitSource);
            try {
                return drainSplits(hiveSplitSource);
            }
            finally {
                firstVisit.countDown();
            }
        }));

        for (int i = 0; i < totalCount - 1; i++) {
            futures.add(executor.submit(() -> {
                firstVisit.await();
                BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(TEST_LOCATIONS, cachingDirectoryLister);
                HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
                backgroundHiveSplitLoader.start(hiveSplitSource);
                return drainSplits(hiveSplitSource);
            }));
        }

        for (Future<List<HiveSplit>> future : futures) {
            assertThat(future.get().size()).isEqualTo(TEST_LOCATIONS.size());
        }
        assertThat(cachingDirectoryLister.getRequestCount()).isEqualTo(totalCount);
        assertThat(cachingDirectoryLister.getHitCount()).isEqualTo(totalCount - 1);
        assertThat(cachingDirectoryLister.getMissCount()).isEqualTo(1);
    }

    @Test
    public void testGetBucketNumber()
    {
        // legacy Presto naming pattern
        assertThat(getBucketNumber("20190526_072952_00009_fn7s5_bucket-00234")).isEqualTo(OptionalInt.of(234));
        assertThat(getBucketNumber("20190526_072952_00009_fn7s5_bucket-00234.txt")).isEqualTo(OptionalInt.of(234));
        assertThat(getBucketNumber("20190526_235847_87654_fn7s5_bucket-56789")).isEqualTo(OptionalInt.of(56789));

        // Hive
        assertThat(getBucketNumber("0234_0")).isEqualTo(OptionalInt.of(234));
        assertThat(getBucketNumber("000234_0")).isEqualTo(OptionalInt.of(234));
        assertThat(getBucketNumber("0234_99")).isEqualTo(OptionalInt.of(234));
        assertThat(getBucketNumber("0234_0.txt")).isEqualTo(OptionalInt.of(234));
        assertThat(getBucketNumber("0234_0_copy_1")).isEqualTo(OptionalInt.of(234));
        // starts with non-zero
        assertThat(getBucketNumber("234_99")).isEqualTo(OptionalInt.of(234));
        assertThat(getBucketNumber("1234_0_copy_1")).isEqualTo(OptionalInt.of(1234));

        // Hive ACID
        assertThat(getBucketNumber("bucket_1234")).isEqualTo(OptionalInt.of(1234));
        assertThat(getBucketNumber("bucket_01234")).isEqualTo(OptionalInt.of(1234));

        // not matching
        assertThat(getBucketNumber("0234.txt")).isEqualTo(OptionalInt.empty());
        assertThat(getBucketNumber("0234.txt")).isEqualTo(OptionalInt.empty());
    }

    @Test
    public void testGetAttemptId()
    {
        assertThat(hasAttemptId("bucket_00000")).isFalse();
        assertThat(hasAttemptId("bucket_00000_0")).isTrue();
        assertThat(hasAttemptId("bucket_00000_10")).isTrue();
        assertThat(hasAttemptId("bucket_00000_1000")).isTrue();
        assertThat(hasAttemptId("bucket_00000__1000")).isFalse();
        assertThat(hasAttemptId("bucket_00000_a")).isFalse();
        assertThat(hasAttemptId("bucket_00000_ad")).isFalse();
        assertThat(hasAttemptId("base_00000_00")).isFalse();
    }

    @Test
    @Timeout(60)
    public void testPropagateException()
            throws IOException
    {
        testPropagateException(false, 1);
        testPropagateException(true, 1);
        testPropagateException(false, 2);
        testPropagateException(true, 2);
        testPropagateException(false, 4);
        testPropagateException(true, 4);
    }

    private void testPropagateException(boolean error, int threads)
            throws IOException
    {
        AtomicBoolean iteratorUsedAfterException = new AtomicBoolean();

        TrinoFileSystemFactory fileSystemFactory = createTestingFileSystem(TEST_LOCATIONS);
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = new BackgroundHiveSplitLoader(
                SIMPLE_TABLE,
                new Iterator<>()
                {
                    private boolean threw;

                    @Override
                    public boolean hasNext()
                    {
                        iteratorUsedAfterException.compareAndSet(false, threw);
                        return !threw;
                    }

                    @Override
                    public HivePartitionMetadata next()
                    {
                        iteratorUsedAfterException.compareAndSet(false, threw);
                        threw = true;
                        if (error) {
                            throw new Error("loading error occurred");
                        }
                        throw new RuntimeException("loading error occurred");
                    }
                },
                TupleDomain.all(),
                DynamicFilter.EMPTY,
                new Duration(0, SECONDS),
                TESTING_TYPE_MANAGER,
                createBucketSplitInfo(Optional.empty(), Optional.empty()),
                SESSION,
                fileSystemFactory,
                new CachingDirectoryLister(new HiveConfig()),
                executor,
                threads,
                false,
                false,
                Optional.empty(),
                Optional.empty(),
                100);

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);

        assertThatThrownBy(() -> drain(hiveSplitSource))
                .hasMessageEndingWith("loading error occurred");

        assertThatThrownBy(hiveSplitSource::isFinished)
                .hasMessageEndingWith("loading error occurred");

        if (threads == 1) {
            assertThat(iteratorUsedAfterException.get()).isFalse();
        }
    }

    @Test
    public void testMultipleSplitsPerBucket()
            throws Exception
    {
        TrinoFileSystemFactory fileSystemFactory = new ListSingleFileFileSystemFactory(new FileEntry(LOCATION, DataSize.of(1, GIGABYTE).toBytes(), Instant.now(), Optional.empty()));
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                fileSystemFactory,
                TupleDomain.all(),
                Optional.empty(),
                SIMPLE_TABLE,
                Optional.of(new HiveBucketHandle(BUCKET_COLUMN_HANDLES, BUCKETING_V1, BUCKET_COUNT, BUCKET_COUNT, List.of())),
                Optional.empty());

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);

        assertThat(drainSplits(hiveSplitSource).size()).isEqualTo(17);
    }

    @Test
    public void testSplitsGenerationWithAbortedTransactions()
            throws Exception
    {
        TrinoFileSystemFactory fileSystemFactory = new MemoryFileSystemFactory();
        TrinoFileSystem fileSystem = fileSystemFactory.create(ConnectorIdentity.ofUser("test"));
        Location tableLocation = Location.of("memory:///my_table");

        Table table = table(
                tableLocation.toString(),
                List.of(),
                Optional.empty(),
                Map.of(
                        TRANSACTIONAL, "true",
                        "transactional_properties", "insert_only"));

        List<Location> fileLocations = List.of(
                tableLocation.appendPath("delta_0000001_0000001_0000/_orc_acid_version"),
                tableLocation.appendPath("delta_0000001_0000001_0000/bucket_00000"),
                tableLocation.appendPath("delta_0000002_0000002_0000/_orc_acid_version"),
                tableLocation.appendPath("delta_0000002_0000002_0000/bucket_00000"),
                tableLocation.appendPath("delta_0000003_0000003_0000/_orc_acid_version"),
                tableLocation.appendPath("delta_0000003_0000003_0000/bucket_00000"));

        for (Location fileLocation : fileLocations) {
            createOrcAcidFile(fileSystem, fileLocation);
        }

        // ValidWriteIdList is of format <currentTxn>$<schema>.<table>:<highWatermark>:<minOpenWriteId>::<AbortedTxns>
        // This writeId list has high watermark transaction=3 and aborted transaction=2
        String validWriteIdsList = format("4$%s.%s:3:9223372036854775807::2", table.getDatabaseName(), table.getTableName());

        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                fileSystemFactory,
                TupleDomain.none(),
                Optional.empty(),
                table,
                Optional.empty(),
                Optional.of(new ValidWriteIdList(validWriteIdsList)));

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);
        List<String> splits = drain(hiveSplitSource);
        assertThat(splits).contains(fileLocations.get(1).toString());
        assertThat(splits).contains(fileLocations.get(5).toString());
    }

    @Test
    public void testFullAcidTableWithOriginalFiles()
            throws Exception
    {
        TrinoFileSystemFactory fileSystemFactory = new MemoryFileSystemFactory();
        TrinoFileSystem fileSystem = fileSystemFactory.create(ConnectorIdentity.ofUser("test"));
        Location tableLocation = Location.of("memory:///my_table");

        Table table = table(
                tableLocation.toString(),
                List.of(),
                Optional.empty(),
                Map.of(TRANSACTIONAL, "true"));

        Location originalFile = tableLocation.appendPath("000000_1");
        try (OutputStream outputStream = fileSystem.newOutputFile(originalFile).create()) {
            outputStream.write("test".getBytes(UTF_8));
        }
        List<Location> fileLocations = List.of(
                tableLocation.appendPath("delta_0000002_0000002_0000/_orc_acid_version"),
                tableLocation.appendPath("delta_0000002_0000002_0000/bucket_00000"));
        for (Location fileLocation : fileLocations) {
            createOrcAcidFile(fileSystem, fileLocation);
        }

        // ValidWriteIdsList is of format <currentTxn>$<schema>.<table>:<highWatermark>:<minOpenWriteId>::<AbortedTxns>
        // This writeId list has high watermark transaction=3
        ValidWriteIdList validWriteIdsList = new ValidWriteIdList(format("4$%s.%s:3:9223372036854775807::", table.getDatabaseName(), table.getTableName()));

        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                fileSystemFactory,
                TupleDomain.all(),
                Optional.empty(),
                table,
                Optional.empty(),
                Optional.of(validWriteIdsList));
        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);
        List<String> splits = drain(hiveSplitSource);
        assertThat(splits).contains(originalFile.toString());
        assertThat(splits).contains(fileLocations.get(1).toString());
    }

    @Test
    public void testVersionValidationNoOrcAcidVersionFile()
            throws Exception
    {
        TrinoFileSystemFactory fileSystemFactory = new MemoryFileSystemFactory();
        TrinoFileSystem fileSystem = fileSystemFactory.create(ConnectorIdentity.ofUser("test"));
        Location tableLocation = Location.of("memory:///my_table");

        Table table = table(
                tableLocation.toString(),
                List.of(),
                Optional.empty(),
                Map.of(TRANSACTIONAL, "true"));

        List<Location> fileLocations = List.of(
                tableLocation.appendPath("000000_1"),
                // no /delta_0000002_0000002_0000/_orc_acid_version file
                tableLocation.appendPath("delta_0000002_0000002_0000/bucket_00000"));

        for (Location fileLocation : fileLocations) {
            createOrcAcidFile(fileSystem, fileLocation);
        }

        // ValidWriteIdsList is of format <currentTxn>$<schema>.<table>:<highWatermark>:<minOpenWriteId>::<AbortedTxns>
        // This writeId list has high watermark transaction=3
        ValidWriteIdList validWriteIdsList = new ValidWriteIdList(format("4$%s.%s:3:9223372036854775807::", table.getDatabaseName(), table.getTableName()));

        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                fileSystemFactory,
                TupleDomain.all(),
                Optional.empty(),
                table,
                Optional.empty(),
                Optional.of(validWriteIdsList));

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);

        // We should have it marked in all splits that further ORC ACID validation is required
        assertThat(drainSplits(hiveSplitSource)).extracting(HiveSplit::getAcidInfo)
                .allMatch(Optional::isPresent)
                .extracting(Optional::get)
                .noneMatch(AcidInfo::isOrcAcidVersionValidated);
    }

    @Test
    public void testVersionValidationOrcAcidVersionFileHasVersion2()
            throws Exception
    {
        TrinoFileSystemFactory fileSystemFactory = new MemoryFileSystemFactory();
        TrinoFileSystem fileSystem = fileSystemFactory.create(ConnectorIdentity.ofUser("test"));
        Location tableLocation = Location.of("memory:///my_table");

        Table table = table(
                tableLocation.toString(),
                List.of(),
                Optional.empty(),
                Map.of(TRANSACTIONAL, "true"));

        List<Location> fileLocations = List.of(
                tableLocation.appendPath("000000_1"), // _orc_acid_version does not exist, so it's assumed to be "ORC ACID version 0"
                tableLocation.appendPath("delta_0000002_0000002_0000/_orc_acid_version"),
                tableLocation.appendPath("delta_0000002_0000002_0000/bucket_00000"));

        for (Location fileLocation : fileLocations) {
            createOrcAcidFile(fileSystem, fileLocation, 2);
        }

        // ValidWriteIdsList is of format <currentTxn>$<schema>.<table>:<highWatermark>:<minOpenWriteId>::<AbortedTxns>
        // This writeId list has high watermark transaction=3
        ValidWriteIdList validWriteIdsList = new ValidWriteIdList(format("4$%s.%s:3:9223372036854775807::", table.getDatabaseName(), table.getTableName()));

        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                fileSystemFactory,
                TupleDomain.all(),
                Optional.empty(),
                table,
                Optional.empty(),
                Optional.of(validWriteIdsList));

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);

        // We should have it marked in all splits that NO further ORC ACID validation is required
        assertThat(drainSplits(hiveSplitSource)).extracting(HiveSplit::getAcidInfo)
                .allMatch(acidInfo -> acidInfo.isEmpty() || acidInfo.get().isOrcAcidVersionValidated());
    }

    @Test
    public void testVersionValidationOrcAcidVersionFileHasVersion1()
            throws Exception
    {
        TrinoFileSystemFactory fileSystemFactory = new MemoryFileSystemFactory();
        TrinoFileSystem fileSystem = fileSystemFactory.create(ConnectorIdentity.ofUser("test"));
        Location tableLocation = Location.of("memory:///my_table");

        Table table = table(
                tableLocation.toString(),
                List.of(),
                Optional.empty(),
                Map.of(TRANSACTIONAL, "true"));

        List<Location> fileLocations = List.of(
                tableLocation.appendPath("000000_1"),
                tableLocation.appendPath("delta_0000002_0000002_0000/_orc_acid_version"),
                tableLocation.appendPath("delta_0000002_0000002_0000/bucket_00000"));

        for (Location fileLocation : fileLocations) {
            // _orc_acid_version_exists but has version 1
            createOrcAcidFile(fileSystem, fileLocation, 1);
        }

        // ValidWriteIdsList is of format <currentTxn>$<schema>.<table>:<highWatermark>:<minOpenWriteId>::<AbortedTxns>
        // This writeId list has high watermark transaction=3
        ValidWriteIdList validWriteIdsList = new ValidWriteIdList(format("4$%s.%s:3:9223372036854775807::", table.getDatabaseName(), table.getTableName()));

        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                fileSystemFactory,
                TupleDomain.all(),
                Optional.empty(),
                table,
                Optional.empty(),
                Optional.of(validWriteIdsList));

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);

        // We should have it marked in all splits that further ORC ACID validation is required
        assertThat(drainSplits(hiveSplitSource)).extracting(HiveSplit::getAcidInfo)
                .allMatch(Optional::isPresent)
                .extracting(Optional::get)
                .noneMatch(AcidInfo::isOrcAcidVersionValidated);
    }

    @Test
    public void testValidateFileBuckets()
    {
        ListMultimap<Integer, TrinoFileStatus> bucketFiles = ArrayListMultimap.create();
        bucketFiles.put(1, null);
        bucketFiles.put(3, null);
        bucketFiles.put(4, null);
        bucketFiles.put(6, null);
        bucketFiles.put(9, null);

        assertTrinoExceptionThrownBy(() -> BackgroundHiveSplitLoader.validateFileBuckets(bucketFiles, 1, "tableName", "partitionName"))
                .hasErrorCode(HIVE_INVALID_BUCKET_FILES)
                .hasMessage("Hive table 'tableName' is corrupt. The highest bucket number in the directory (9) exceeds the bucket number range " +
                        "defined by the declared bucket count (1) for partition: partitionName");

        assertTrinoExceptionThrownBy(() -> BackgroundHiveSplitLoader.validateFileBuckets(bucketFiles, 5, "tableName", "partitionName"))
                .hasErrorCode(HIVE_INVALID_BUCKET_FILES)
                .hasMessage("Hive table 'tableName' is corrupt. The highest bucket number in the directory (9) exceeds the bucket number range " +
                        "defined by the declared bucket count (5) for partition: partitionName");

        assertTrinoExceptionThrownBy(() -> BackgroundHiveSplitLoader.validateFileBuckets(bucketFiles, 9, "tableName", "partitionName"))
                .hasErrorCode(HIVE_INVALID_BUCKET_FILES)
                .hasMessage("Hive table 'tableName' is corrupt. The highest bucket number in the directory (9) exceeds the bucket number range " +
                        "defined by the declared bucket count (9) for partition: partitionName");

        BackgroundHiveSplitLoader.validateFileBuckets(bucketFiles, 10, "tableName", "partitionName");
        BackgroundHiveSplitLoader.validateFileBuckets(bucketFiles, 20, "tableName", "partitionName");
        BackgroundHiveSplitLoader.validateFileBuckets(bucketFiles, 30, "tableName", "partitionName");
    }

    @Test
    public void testBuildManifestFileIterator()
            throws IOException
    {
        CachingDirectoryLister directoryLister = new CachingDirectoryLister(new Duration(0, TimeUnit.MINUTES), DataSize.ofBytes(0), List.of());
        Map<String, String> schema = ImmutableMap.<String, String>builder()
                .put(FILE_INPUT_FORMAT, SYMLINK_TEXT_INPUT_FORMAT_CLASS)
                .put(SERIALIZATION_LIB, AVRO.getSerde())
                .buildOrThrow();

        Location firstFilePath = Location.of("memory:///db_name/table_name/file1");
        Location secondFilePath = Location.of("memory:///db_name/table_name/file2");
        List<Location> locations = List.of(firstFilePath, secondFilePath);

        InternalHiveSplitFactory splitFactory = new InternalHiveSplitFactory(
                "partition",
                AVRO,
                schema,
                List.of(),
                TupleDomain.all(),
                () -> true,
                ImmutableMap.of(),
                Optional.empty(),
                Optional.empty(),
                DataSize.of(512, MEGABYTE),
                false,
                Optional.empty());
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                locations,
                directoryLister);
        Iterator<InternalHiveSplit> splitIterator = backgroundHiveSplitLoader.buildManifestFileIterator(
                splitFactory,
                Location.of(TABLE_PATH),
                locations,
                true);
        List<InternalHiveSplit> splits = ImmutableList.copyOf(splitIterator);
        assertThat(splits.size()).isEqualTo(2);
        assertThat(splits.get(0).getPath()).isEqualTo(firstFilePath.toString());
        assertThat(splits.get(1).getPath()).isEqualTo(secondFilePath.toString());
    }

    @Test
    public void testBuildManifestFileIteratorNestedDirectory()
            throws IOException
    {
        CachingDirectoryLister directoryLister = new CachingDirectoryLister(new Duration(5, TimeUnit.MINUTES), DataSize.of(100, KILOBYTE), List.of());
        Map<String, String> schema = ImmutableMap.<String, String>builder()
                .put(FILE_INPUT_FORMAT, SYMLINK_TEXT_INPUT_FORMAT_CLASS)
                .put(SERIALIZATION_LIB, AVRO.getSerde())
                .buildOrThrow();

        Location filePath = Location.of("memory:///db_name/table_name/file1");
        Location directoryPath = Location.of("memory:///db_name/table_name/dir/file2");
        List<Location> locations = List.of(filePath, directoryPath);

        InternalHiveSplitFactory splitFactory = new InternalHiveSplitFactory(
                "partition",
                AVRO,
                schema,
                List.of(),
                TupleDomain.all(),
                () -> true,
                ImmutableMap.of(),
                Optional.empty(),
                Optional.empty(),
                DataSize.of(512, MEGABYTE),
                false,
                Optional.empty());

        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                locations,
                directoryLister);
        Iterator<InternalHiveSplit> splitIterator = backgroundHiveSplitLoader.buildManifestFileIterator(
                splitFactory,
                Location.of(TABLE_PATH),
                locations,
                false);
        List<InternalHiveSplit> splits = ImmutableList.copyOf(splitIterator);
        assertThat(splits.size()).isEqualTo(2);
        assertThat(splits.get(0).getPath()).isEqualTo(filePath.toString());
        assertThat(splits.get(1).getPath()).isEqualTo(directoryPath.toString());
    }

    @Test
    public void testBuildManifestFileIteratorWithCacheInvalidation()
            throws IOException
    {
        CachingDirectoryLister directoryLister = new CachingDirectoryLister(new Duration(5, TimeUnit.MINUTES), DataSize.of(1, MEGABYTE), List.of("*"));
        Map<String, String> schema = ImmutableMap.<String, String>builder()
                .put(FILE_INPUT_FORMAT, SYMLINK_TEXT_INPUT_FORMAT_CLASS)
                .put(SERIALIZATION_LIB, AVRO.getSerde())
                .buildOrThrow();

        InternalHiveSplitFactory splitFactory = new InternalHiveSplitFactory(
                "partition",
                AVRO,
                schema,
                List.of(),
                TupleDomain.all(),
                () -> true,
                ImmutableMap.of(),
                Optional.empty(),
                Optional.empty(),
                DataSize.of(512, MEGABYTE),
                false,
                Optional.empty());

        Location firstFilePath = Location.of("memory:///db_name/table_name/file1");
        List<Location> locations1 = List.of(firstFilePath);
        BackgroundHiveSplitLoader backgroundHiveSplitLoader1 = backgroundHiveSplitLoader(
                locations1,
                directoryLister);
        Iterator<InternalHiveSplit> splitIterator1 = backgroundHiveSplitLoader1.buildManifestFileIterator(
                splitFactory,
                Location.of(TABLE_PATH),
                locations1,
                true);
        List<InternalHiveSplit> splits1 = ImmutableList.copyOf(splitIterator1);
        assertThat(splits1.size()).isEqualTo(1);
        assertThat(splits1.get(0).getPath()).isEqualTo(firstFilePath.toString());

        Location secondFilePath = Location.of("memory:///db_name/table_name/file2");
        List<Location> locations2 = List.of(firstFilePath, secondFilePath);
        BackgroundHiveSplitLoader backgroundHiveSplitLoader2 = backgroundHiveSplitLoader(
                locations2,
                directoryLister);
        Iterator<InternalHiveSplit> splitIterator2 = backgroundHiveSplitLoader2.buildManifestFileIterator(
                splitFactory,
                Location.of(TABLE_PATH),
                locations2,
                true);
        List<InternalHiveSplit> splits2 = ImmutableList.copyOf(splitIterator2);
        assertThat(splits2.size()).isEqualTo(2);
        assertThat(splits2.get(0).getPath()).isEqualTo(firstFilePath.toString());
        assertThat(splits2.get(1).getPath()).isEqualTo(secondFilePath.toString());
    }

    @Test
    public void testMaxPartitions()
            throws Exception
    {
        CachingDirectoryLister directoryLister = new CachingDirectoryLister(new Duration(0, TimeUnit.MINUTES), DataSize.ofBytes(0), List.of());
        // zero partitions
        {
            BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                    List.of(),
                    List.of(),
                    directoryLister,
                    0);
            HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
            backgroundHiveSplitLoader.start(hiveSplitSource);
            assertThat(drainSplits(hiveSplitSource)).isEmpty();
        }

        // single partition, not crossing the limit
        {
            BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                    List.of(createPartitionMetadata()),
                    TEST_LOCATIONS,
                    directoryLister,
                    1);
            HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
            backgroundHiveSplitLoader.start(hiveSplitSource);
            assertThat(drainSplits(hiveSplitSource)).hasSize(TEST_LOCATIONS.size());
        }

        // single partition, crossing the limit
        {
            int partitionLimit = 0;
            BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                    List.of(createPartitionMetadata()),
                    TEST_LOCATIONS,
                    directoryLister,
                    partitionLimit);
            HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
            backgroundHiveSplitLoader.start(hiveSplitSource);
            assertThatThrownBy(() -> drainSplits(hiveSplitSource))
                    .isInstanceOf(TrinoException.class)
                    .hasMessage(format(
                            "Query over table '%s' can potentially read more than %s partitions",
                            SIMPLE_TABLE.getSchemaTableName(),
                            partitionLimit));
        }

        // multiple partitions, not crossing the limit
        {
            int partitionLimit = 3;
            List<HivePartitionMetadata> partitions = List.of(createPartitionMetadata(), createPartitionMetadata(), createPartitionMetadata());
            BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                    partitions,
                    TEST_LOCATIONS,
                    directoryLister,
                    partitionLimit);
            HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
            backgroundHiveSplitLoader.start(hiveSplitSource);
            assertThat(drainSplits(hiveSplitSource)).hasSize(TEST_LOCATIONS.size() * partitions.size());
        }

        // multiple partitions, crossing the limit
        {
            int partitionLimit = 3;
            BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                    List.of(createPartitionMetadata(), createPartitionMetadata(), createPartitionMetadata(), createPartitionMetadata()),
                    TEST_LOCATIONS,
                    directoryLister,
                    partitionLimit);
            HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
            backgroundHiveSplitLoader.start(hiveSplitSource);
            assertThatThrownBy(() -> drainSplits(hiveSplitSource))
                    .isInstanceOf(TrinoException.class)
                    .hasMessage(format(
                            "Query over table '%s' can potentially read more than %s partitions",
                            SIMPLE_TABLE.getSchemaTableName(),
                            partitionLimit));
        }
    }

    private static HivePartitionMetadata createPartitionMetadata()
    {
        return new HivePartitionMetadata(
                new HivePartition(SIMPLE_TABLE.getSchemaTableName()),
                Optional.empty(),
                ImmutableMap.of());
    }

    private static void createOrcAcidFile(TrinoFileSystem fileSystem, Location location)
            throws IOException
    {
        createOrcAcidFile(fileSystem, location, 2);
    }

    private static void createOrcAcidFile(TrinoFileSystem fileSystem, Location location, int orcAcidVersion)
            throws IOException
    {
        try (OutputStream outputStream = fileSystem.newOutputFile(location).create()) {
            if (location.fileName().equals("_orc_acid_version")) {
                outputStream.write(String.valueOf(orcAcidVersion).getBytes(UTF_8));
                return;
            }
            Resources.copy(getResource("fullacidNationTableWithOriginalFiles/000000_0"), outputStream);
        }
    }

    private static List<String> drain(HiveSplitSource source)
            throws Exception
    {
        return drainSplits(source).stream()
                .map(HiveSplit::getPath)
                .toList();
    }

    private static List<HiveSplit> drainSplits(HiveSplitSource source)
            throws Exception
    {
        ImmutableList.Builder<HiveSplit> splits = ImmutableList.builder();
        while (!source.isFinished()) {
            ConnectorSplitBatch batch;
            try {
                batch = source.getNextBatch(100).get();
            }
            catch (ExecutionException e) {
                throwIfUnchecked(e.getCause());
                throw e;
            }
            batch.getSplits().stream()
                    .map(HiveSplit.class::cast)
                    .forEach(splits::add);
        }
        return splits.build();
    }

    private BackgroundHiveSplitLoader backgroundHiveSplitLoader(
            DynamicFilter dynamicFilter,
            Duration dynamicFilteringProbeBlockingTimeoutMillis)
            throws IOException
    {
        TrinoFileSystemFactory fileSystemFactory = createTestingFileSystem(TEST_LOCATIONS);
        return backgroundHiveSplitLoader(
                fileSystemFactory,
                TupleDomain.all(),
                dynamicFilter,
                dynamicFilteringProbeBlockingTimeoutMillis,
                Optional.empty(),
                SIMPLE_TABLE,
                Optional.empty(),
                Optional.empty());
    }

    private static TrinoFileSystemFactory createTestingFileSystem(Collection<Location> locations)
            throws IOException
    {
        TrinoFileSystemFactory fileSystemFactory = new MemoryFileSystemFactory();
        TrinoFileSystem fileSystem = fileSystemFactory.create(ConnectorIdentity.ofUser("test"));
        for (Location location : locations) {
            try (OutputStream outputStream = fileSystem.newOutputFile(location).create()) {
                outputStream.write(new byte[10]);
            }
        }
        return fileSystemFactory;
    }

    private BackgroundHiveSplitLoader backgroundHiveSplitLoader(
            List<Location> locations,
            TupleDomain<HiveColumnHandle> tupleDomain)
            throws IOException
    {
        return backgroundHiveSplitLoader(
                locations,
                tupleDomain,
                Optional.empty(),
                SIMPLE_TABLE,
                Optional.empty());
    }

    private BackgroundHiveSplitLoader backgroundHiveSplitLoader(
            List<Location> locations,
            TupleDomain<HiveColumnHandle> compactEffectivePredicate,
            Optional<HiveBucketFilter> hiveBucketFilter,
            Table table,
            Optional<HiveBucketHandle> bucketHandle)
            throws IOException
    {
        return backgroundHiveSplitLoader(
                locations,
                compactEffectivePredicate,
                hiveBucketFilter,
                table,
                bucketHandle,
                Optional.empty());
    }

    private BackgroundHiveSplitLoader backgroundHiveSplitLoader(
            List<Location> locations,
            TupleDomain<HiveColumnHandle> compactEffectivePredicate,
            Optional<HiveBucketFilter> hiveBucketFilter,
            Table table,
            Optional<HiveBucketHandle> bucketHandle,
            Optional<ValidWriteIdList> validWriteIds)
            throws IOException
    {
        TrinoFileSystemFactory fileSystemFactory = createTestingFileSystem(locations);
        return backgroundHiveSplitLoader(
                fileSystemFactory,
                compactEffectivePredicate,
                hiveBucketFilter,
                table,
                bucketHandle,
                validWriteIds);
    }

    private BackgroundHiveSplitLoader backgroundHiveSplitLoader(
            TrinoFileSystemFactory fileSystemFactory,
            TupleDomain<HiveColumnHandle> compactEffectivePredicate,
            Optional<HiveBucketFilter> hiveBucketFilter,
            Table table,
            Optional<HiveBucketHandle> bucketHandle,
            Optional<ValidWriteIdList> validWriteIds)
    {
        return backgroundHiveSplitLoader(
                fileSystemFactory,
                compactEffectivePredicate,
                DynamicFilter.EMPTY,
                new Duration(0, SECONDS),
                hiveBucketFilter,
                table,
                bucketHandle,
                validWriteIds);
    }

    private BackgroundHiveSplitLoader backgroundHiveSplitLoader(
            TrinoFileSystemFactory fileSystemFactory,
            TupleDomain<HiveColumnHandle> compactEffectivePredicate,
            DynamicFilter dynamicFilter,
            Duration dynamicFilteringProbeBlockingTimeout,
            Optional<HiveBucketFilter> hiveBucketFilter,
            Table table,
            Optional<HiveBucketHandle> bucketHandle,
            Optional<ValidWriteIdList> validWriteIds)
    {
        List<HivePartitionMetadata> hivePartitionMetadatas =
                List.of(
                        new HivePartitionMetadata(
                                new HivePartition(new SchemaTableName("testSchema", "table_name")),
                                Optional.empty(),
                                ImmutableMap.of()));

        return new BackgroundHiveSplitLoader(
                table,
                hivePartitionMetadatas.iterator(),
                compactEffectivePredicate,
                dynamicFilter,
                dynamicFilteringProbeBlockingTimeout,
                TESTING_TYPE_MANAGER,
                createBucketSplitInfo(bucketHandle, hiveBucketFilter),
                SESSION,
                fileSystemFactory,
                new CachingDirectoryLister(new HiveConfig()),
                executor,
                2,
                false,
                false,
                validWriteIds,
                Optional.empty(),
                100);
    }

    private BackgroundHiveSplitLoader backgroundHiveSplitLoader(
            List<Location> locations,
            DirectoryLister directoryLister)
            throws IOException
    {
        List<HivePartitionMetadata> partitions = List.of(
                new HivePartitionMetadata(
                        new HivePartition(new SchemaTableName("testSchema", "table_name")),
                        Optional.empty(),
                        ImmutableMap.of()));
        return backgroundHiveSplitLoader(partitions, locations, directoryLister, 100);
    }

    private BackgroundHiveSplitLoader backgroundHiveSplitLoader(
            List<HivePartitionMetadata> partitions,
            List<Location> locations,
            DirectoryLister directoryLister,
            int maxPartitions)
            throws IOException
    {
        ConnectorSession connectorSession = getHiveSession(new HiveConfig()
                .setMaxSplitSize(DataSize.of(1, GIGABYTE)));

        TrinoFileSystemFactory fileSystemFactory = createTestingFileSystem(locations);
        return new BackgroundHiveSplitLoader(
                SIMPLE_TABLE,
                partitions.iterator(),
                TupleDomain.none(),
                DynamicFilter.EMPTY,
                new Duration(0, SECONDS),
                TESTING_TYPE_MANAGER,
                Optional.empty(),
                connectorSession,
                fileSystemFactory,
                directoryLister,
                executor,
                2,
                false,
                false,
                Optional.empty(),
                Optional.empty(),
                maxPartitions);
    }

    private BackgroundHiveSplitLoader backgroundHiveSplitLoaderOfflinePartitions()
            throws IOException
    {
        ConnectorSession connectorSession = getHiveSession(new HiveConfig()
                .setMaxSplitSize(DataSize.of(1, GIGABYTE)));

        TrinoFileSystemFactory fileSystemFactory = createTestingFileSystem(TEST_LOCATIONS);
        return new BackgroundHiveSplitLoader(
                SIMPLE_TABLE,
                createPartitionMetadataWithOfflinePartitions(),
                TupleDomain.all(),
                DynamicFilter.EMPTY,
                new Duration(0, SECONDS),
                TESTING_TYPE_MANAGER,
                createBucketSplitInfo(Optional.empty(), Optional.empty()),
                connectorSession,
                fileSystemFactory,
                new CachingDirectoryLister(new HiveConfig()),
                executor,
                2,
                false,
                false,
                Optional.empty(),
                Optional.empty(),
                100);
    }

    private static Iterator<HivePartitionMetadata> createPartitionMetadataWithOfflinePartitions()
            throws RuntimeException
    {
        return new AbstractIterator<>()
        {
            // This iterator is crafted to return a valid partition for the first calls to
            // hasNext() and next(), and then it should throw for the second call to hasNext()
            private int position = -1;

            @Override
            protected HivePartitionMetadata computeNext()
            {
                position++;
                return switch (position) {
                    case 0 -> new HivePartitionMetadata(new HivePartition(new SchemaTableName("testSchema", "table_name")), Optional.empty(), ImmutableMap.of());
                    case 1 -> throw new RuntimeException("OFFLINE");
                    default -> endOfData();
                };
            }
        };
    }

    private HiveSplitSource hiveSplitSource(HiveSplitLoader hiveSplitLoader)
    {
        return HiveSplitSource.allAtOnce(
                SESSION,
                SIMPLE_TABLE.getDatabaseName(),
                SIMPLE_TABLE.getTableName(),
                1,
                1,
                DataSize.of(32, MEGABYTE),
                Integer.MAX_VALUE,
                hiveSplitLoader,
                executor,
                new CounterStat(),
                new DefaultCachingHostAddressProvider(),
                false);
    }

    private static Table table(
            String location,
            List<Column> partitionColumns,
            Optional<HiveBucketProperty> bucketProperty,
            Map<String, String> tableParameters)
    {
        return table(location,
                partitionColumns,
                bucketProperty,
                tableParameters,
                StorageFormat.fromHiveStorageFormat(ORC));
    }

    private static Table table(
            String location,
            List<Column> partitionColumns,
            Optional<HiveBucketProperty> bucketProperty,
            Map<String, String> tableParameters,
            StorageFormat storageFormat)
    {
        Table.Builder tableBuilder = Table.builder();
        tableBuilder.getStorageBuilder()
                .setStorageFormat(storageFormat)
                .setLocation(location)
                .setSkewed(false)
                .setBucketProperty(bucketProperty);

        return tableBuilder
                .setDatabaseName("test_dbname")
                .setOwner(Optional.of("testOwner"))
                .setTableName("test_table")
                .setTableType(MANAGED_TABLE.name())
                .setDataColumns(List.of(new Column("col1", HIVE_STRING, Optional.empty(), Map.of())))
                .setParameters(tableParameters)
                .setPartitionColumns(partitionColumns)
                .build();
    }

    private record ListSingleFileFileSystemFactory(FileEntry fileEntry)
            implements TrinoFileSystemFactory
    {
        @Override
        public TrinoFileSystem create(ConnectorIdentity identity)
        {
            return new TrinoFileSystem()
            {
                @Override
                public Optional<Boolean> directoryExists(Location location)
                {
                    return Optional.empty();
                }

                @Override
                public FileIterator listFiles(Location location)
                {
                    Iterator<FileEntry> iterator = List.of(fileEntry).iterator();
                    return new FileIterator()
                    {
                        @Override
                        public boolean hasNext()
                        {
                            return iterator.hasNext();
                        }

                        @Override
                        public FileEntry next()
                        {
                            return iterator.next();
                        }
                    };
                }

                @Override
                public TrinoInputFile newInputFile(Location location)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public TrinoInputFile newInputFile(Location location, long length)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public TrinoOutputFile newOutputFile(Location location)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void deleteFile(Location location)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void deleteDirectory(Location location)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void renameFile(Location source, Location target)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void createDirectory(Location location)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void renameDirectory(Location source, Location target)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public Set<Location> listDirectories(Location location)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public Optional<Location> createTemporaryDirectory(Location targetPath, String temporaryPrefix, String relativePrefix)
                {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }
}
