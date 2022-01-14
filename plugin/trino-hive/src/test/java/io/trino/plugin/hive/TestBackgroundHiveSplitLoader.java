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

import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import io.airlift.stats.CounterStat;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.plugin.hive.HiveColumnHandle.ColumnType;
import io.trino.plugin.hive.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.util.HiveBucketing.HiveBucketFilter;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitSource.ConnectorSplitBatch;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.common.io.Resources.getResource;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.unmodifiableFuture;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.plugin.hive.BackgroundHiveSplitLoader.BucketSplitInfo.createBucketSplitInfo;
import static io.trino.plugin.hive.BackgroundHiveSplitLoader.getBucketNumber;
import static io.trino.plugin.hive.BackgroundHiveSplitLoader.hasAttemptId;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveColumnHandle.pathColumnHandle;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_BUCKET_FILES;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_UNKNOWN_ERROR;
import static io.trino.plugin.hive.HiveSessionProperties.getMaxInitialSplitSize;
import static io.trino.plugin.hive.HiveStorageFormat.AVRO;
import static io.trino.plugin.hive.HiveStorageFormat.CSV;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.SESSION;
import static io.trino.plugin.hive.HiveTestUtils.getHiveSession;
import static io.trino.plugin.hive.HiveTimestampPrecision.DEFAULT_PRECISION;
import static io.trino.plugin.hive.HiveType.HIVE_INT;
import static io.trino.plugin.hive.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static io.trino.plugin.hive.util.HiveBucketing.BucketingVersion.BUCKETING_V1;
import static io.trino.plugin.hive.util.HiveUtil.getRegularColumnHandles;
import static io.trino.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static io.trino.spi.predicate.TupleDomain.withColumnDomains;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestBackgroundHiveSplitLoader
{
    private static final int BUCKET_COUNT = 2;

    private static final String SAMPLE_PATH = "hdfs://VOL1:9000/db_name/table_name/000000_0";
    private static final String SAMPLE_PATH_FILTERED = "hdfs://VOL1:9000/db_name/table_name/000000_1";

    private static final Path RETURNED_PATH = new Path(SAMPLE_PATH);
    private static final Path FILTERED_PATH = new Path(SAMPLE_PATH_FILTERED);

    private static final TupleDomain<HiveColumnHandle> RETURNED_PATH_DOMAIN = withColumnDomains(
            ImmutableMap.of(
                    pathColumnHandle(),
                    Domain.singleValue(VARCHAR, utf8Slice(RETURNED_PATH.toString()))));

    private static final List<LocatedFileStatus> TEST_FILES = ImmutableList.of(
            locatedFileStatus(RETURNED_PATH),
            locatedFileStatus(FILTERED_PATH));

    private static final List<Column> PARTITION_COLUMNS = ImmutableList.of(
            new Column("partitionColumn", HIVE_INT, Optional.empty()));
    private static final List<HiveColumnHandle> BUCKET_COLUMN_HANDLES = ImmutableList.of(
            createBaseColumn("col1", 0, HIVE_INT, INTEGER, ColumnType.REGULAR, Optional.empty()));

    private static final Optional<HiveBucketProperty> BUCKET_PROPERTY = Optional.of(
            new HiveBucketProperty(ImmutableList.of("col1"), BUCKETING_V1, BUCKET_COUNT, ImmutableList.of()));

    private static final Table SIMPLE_TABLE = table(ImmutableList.of(), Optional.empty(), ImmutableMap.of());
    private static final Table PARTITIONED_TABLE = table(PARTITION_COLUMNS, BUCKET_PROPERTY, ImmutableMap.of());

    private ExecutorService executor;

    @BeforeClass
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
        executor = null;
    }

    @Test
    public void testNoPathFilter()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                TEST_FILES,
                TupleDomain.none());

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);

        assertEquals(drain(hiveSplitSource).size(), 2);
    }

    @Test
    public void testCsv()
            throws Exception
    {
        DataSize fileSize = DataSize.of(2, GIGABYTE);
        assertSplitCount(CSV, ImmutableMap.of(), fileSize, 33);
        assertSplitCount(CSV, ImmutableMap.of("skip.header.line.count", "1"), fileSize, 33);
        assertSplitCount(CSV, ImmutableMap.of("skip.header.line.count", "2"), fileSize, 1);
        assertSplitCount(CSV, ImmutableMap.of("skip.footer.line.count", "1"), fileSize, 1);
        assertSplitCount(CSV, ImmutableMap.of("skip.header.line.count", "1", "skip.footer.line.count", "1"), fileSize, 1);
    }

    @Test
    public void testSplittableNotCheckedOnSmallFiles()
            throws Exception
    {
        DataSize initialSplitSize = getMaxInitialSplitSize(SESSION);

        Table table = table(
                ImmutableList.of(),
                Optional.empty(),
                ImmutableMap.of(),
                StorageFormat.create(LazySimpleSerDe.class.getName(), TestSplittableFailureInputFormat.class.getName(), TestSplittableFailureInputFormat.class.getName()));

        //  Exactly minimum split size, no isSplittable check
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                ImmutableList.of(locatedFileStatus(new Path(SAMPLE_PATH), initialSplitSize.toBytes())),
                TupleDomain.all(),
                Optional.empty(),
                table,
                Optional.empty());

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);

        assertEquals(drainSplits(hiveSplitSource).size(), 1);

        //  Large enough for isSplittable to be called
        backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                ImmutableList.of(locatedFileStatus(new Path(SAMPLE_PATH), initialSplitSize.toBytes() + 1)),
                TupleDomain.all(),
                Optional.empty(),
                table,
                Optional.empty());

        HiveSplitSource finalHiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(finalHiveSplitSource);
        assertTrinoExceptionThrownBy(() -> drainSplits(finalHiveSplitSource))
                .hasErrorCode(HIVE_UNKNOWN_ERROR)
                .isInstanceOfSatisfying(TrinoException.class, e -> {
                    Throwable cause = Throwables.getRootCause(e);
                    assertTrue(cause instanceof IllegalStateException);
                    assertEquals(cause.getMessage(), "isSplittable called");
                });
    }

    public static final class TestSplittableFailureInputFormat
            extends FileInputFormat<Void, Void>
    {
        @Override
        protected boolean isSplitable(FileSystem fs, Path filename)
        {
            throw new IllegalStateException("isSplittable called");
        }

        @Override
        public RecordReader<Void, Void> getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter)
                throws IOException
        {
            throw new UnsupportedOperationException();
        }
    }

    private void assertSplitCount(HiveStorageFormat storageFormat, Map<String, String> tableProperties, DataSize fileSize, int expectedSplitCount)
            throws Exception
    {
        Table table = table(
                ImmutableList.of(),
                Optional.empty(),
                ImmutableMap.copyOf(tableProperties),
                StorageFormat.fromHiveStorageFormat(storageFormat));

        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                ImmutableList.of(locatedFileStatus(new Path(SAMPLE_PATH), fileSize.toBytes())),
                TupleDomain.all(),
                Optional.empty(),
                table,
                Optional.empty());

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);

        assertEquals(drainSplits(hiveSplitSource).size(), expectedSplitCount);
    }

    @Test
    public void testPathFilter()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                TEST_FILES,
                RETURNED_PATH_DOMAIN);

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);
        List<String> paths = drain(hiveSplitSource);
        assertEquals(paths.size(), 1);
        assertEquals(paths.get(0), RETURNED_PATH.toString());
    }

    @Test
    public void testPathFilterOneBucketMatchPartitionedTable()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                TEST_FILES,
                RETURNED_PATH_DOMAIN,
                Optional.of(new HiveBucketFilter(ImmutableSet.of(0, 1))),
                PARTITIONED_TABLE,
                Optional.of(new HiveBucketHandle(BUCKET_COLUMN_HANDLES, BUCKETING_V1, BUCKET_COUNT, BUCKET_COUNT, ImmutableList.of())));

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);
        List<String> paths = drain(hiveSplitSource);
        assertEquals(paths.size(), 1);
        assertEquals(paths.get(0), RETURNED_PATH.toString());
    }

    @Test
    public void testPathFilterBucketedPartitionedTable()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                TEST_FILES,
                RETURNED_PATH_DOMAIN,
                Optional.empty(),
                PARTITIONED_TABLE,
                Optional.of(
                        new HiveBucketHandle(
                                getRegularColumnHandles(PARTITIONED_TABLE, TESTING_TYPE_MANAGER, DEFAULT_PRECISION),
                                BUCKETING_V1,
                                BUCKET_COUNT,
                                BUCKET_COUNT,
                                ImmutableList.of())));

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);
        List<String> paths = drain(hiveSplitSource);
        assertEquals(paths.size(), 1);
        assertEquals(paths.get(0), RETURNED_PATH.toString());
    }

    @Test
    public void testEmptyFileWithNoBlocks()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                ImmutableList.of(locatedFileStatusWithNoBlocks(RETURNED_PATH)),
                TupleDomain.none());

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);

        List<HiveSplit> splits = drainSplits(hiveSplitSource);
        assertEquals(splits.size(), 0);
    }

    @Test
    public void testNoHangIfPartitionIsOffline()
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

    @Test(timeOut = 30_000)
    public void testIncompleteDynamicFilterTimeout()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                new DynamicFilter()
                {
                    @Override
                    public Set<ColumnHandle> getColumnsCovered()
                    {
                        return ImmutableSet.of();
                    }

                    @Override
                    public CompletableFuture<?> isBlocked()
                    {
                        return unmodifiableFuture(CompletableFuture.runAsync(() -> {
                            try {
                                TimeUnit.HOURS.sleep(1);
                            }
                            catch (InterruptedException e) {
                                throw new IllegalStateException(e);
                            }
                        }));
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

        assertEquals(drain(hiveSplitSource).size(), 2);
        assertTrue(hiveSplitSource.isFinished());
    }

    @Test
    public void testCachedDirectoryLister()
            throws Exception
    {
        CachingDirectoryLister cachingDirectoryLister = new CachingDirectoryLister(new Duration(5, TimeUnit.MINUTES), 1000, ImmutableList.of("test_dbname.test_table"));
        assertEquals(cachingDirectoryLister.getRequestCount(), 0);

        int totalCount = 100;
        CountDownLatch firstVisit = new CountDownLatch(1);
        List<Future<List<HiveSplit>>> futures = new ArrayList<>();

        futures.add(executor.submit(() -> {
            BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(TEST_FILES, cachingDirectoryLister);
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
                BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(TEST_FILES, cachingDirectoryLister);
                HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
                backgroundHiveSplitLoader.start(hiveSplitSource);
                return drainSplits(hiveSplitSource);
            }));
        }

        for (Future<List<HiveSplit>> future : futures) {
            assertEquals(future.get().size(), TEST_FILES.size());
        }
        assertEquals(cachingDirectoryLister.getRequestCount(), totalCount);
        assertEquals(cachingDirectoryLister.getHitCount(), totalCount - 1);
        assertEquals(cachingDirectoryLister.getMissCount(), 1);
    }

    @Test
    public void testGetBucketNumber()
    {
        // legacy Presto naming pattern
        assertEquals(getBucketNumber("20190526_072952_00009_fn7s5_bucket-00234"), OptionalInt.of(234));
        assertEquals(getBucketNumber("20190526_072952_00009_fn7s5_bucket-00234.txt"), OptionalInt.of(234));
        assertEquals(getBucketNumber("20190526_235847_87654_fn7s5_bucket-56789"), OptionalInt.of(56789));

        // Hive
        assertEquals(getBucketNumber("0234_0"), OptionalInt.of(234));
        assertEquals(getBucketNumber("000234_0"), OptionalInt.of(234));
        assertEquals(getBucketNumber("0234_99"), OptionalInt.of(234));
        assertEquals(getBucketNumber("0234_0.txt"), OptionalInt.of(234));
        assertEquals(getBucketNumber("0234_0_copy_1"), OptionalInt.of(234));
        // starts with non-zero
        assertEquals(getBucketNumber("234_99"), OptionalInt.of(234));
        assertEquals(getBucketNumber("1234_0_copy_1"), OptionalInt.of(1234));

        // Hive ACID
        assertEquals(getBucketNumber("bucket_1234"), OptionalInt.of(1234));
        assertEquals(getBucketNumber("bucket_01234"), OptionalInt.of(1234));

        // not matching
        assertEquals(getBucketNumber("0234.txt"), OptionalInt.empty());
        assertEquals(getBucketNumber("0234.txt"), OptionalInt.empty());
    }

    @Test
    public void testGetAttemptId()
    {
        assertFalse(hasAttemptId("bucket_00000"));
        assertTrue(hasAttemptId("bucket_00000_0"));
        assertTrue(hasAttemptId("bucket_00000_10"));
        assertTrue(hasAttemptId("bucket_00000_1000"));
        assertFalse(hasAttemptId("bucket_00000__1000"));
        assertFalse(hasAttemptId("bucket_00000_a"));
        assertFalse(hasAttemptId("bucket_00000_ad"));
        assertFalse(hasAttemptId("base_00000_00"));
    }

    @Test(dataProvider = "testPropagateExceptionDataProvider", timeOut = 60_000)
    public void testPropagateException(boolean error, int threads)
    {
        AtomicBoolean iteratorUsedAfterException = new AtomicBoolean();

        BackgroundHiveSplitLoader backgroundHiveSplitLoader = new BackgroundHiveSplitLoader(
                SIMPLE_TABLE,
                NO_ACID_TRANSACTION,
                () -> new Iterator<>()
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
                new TestingHdfsEnvironment(TEST_FILES),
                new NamenodeStats(),
                new CachingDirectoryLister(new HiveConfig()),
                executor,
                threads,
                false,
                false,
                true,
                Optional.empty(),
                Optional.empty());

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);

        assertThatThrownBy(() -> drain(hiveSplitSource))
                .hasMessageEndingWith("loading error occurred");

        assertThatThrownBy(hiveSplitSource::isFinished)
                .hasMessageEndingWith("loading error occurred");

        if (threads == 1) {
            assertFalse(iteratorUsedAfterException.get());
        }
    }

    @DataProvider
    public Object[][] testPropagateExceptionDataProvider()
    {
        return new Object[][] {
                {false, 1},
                {true, 1},
                {false, 2},
                {true, 2},
                {false, 4},
                {true, 4},
        };
    }

    @Test
    public void testMultipleSplitsPerBucket()
            throws Exception
    {
        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                ImmutableList.of(locatedFileStatus(new Path(SAMPLE_PATH), DataSize.of(1, GIGABYTE).toBytes())),
                TupleDomain.all(),
                Optional.empty(),
                SIMPLE_TABLE,
                Optional.of(new HiveBucketHandle(BUCKET_COLUMN_HANDLES, BUCKETING_V1, BUCKET_COUNT, BUCKET_COUNT, ImmutableList.of())));

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);

        assertEquals(drainSplits(hiveSplitSource).size(), 17);
    }

    @Test
    public void testSplitsGenerationWithAbortedTransactions()
            throws Exception
    {
        java.nio.file.Path tablePath = Files.createTempDirectory("TestBackgroundHiveSplitLoader");
        Table table = table(
                tablePath.toString(),
                ImmutableList.of(),
                Optional.empty(),
                ImmutableMap.of(
                        "transactional", "true",
                        "transactional_properties", "insert_only"));

        List<String> filePaths = ImmutableList.of(
                tablePath + "/delta_0000001_0000001_0000/_orc_acid_version",
                tablePath + "/delta_0000001_0000001_0000/bucket_00000",
                tablePath + "/delta_0000002_0000002_0000/_orc_acid_version",
                tablePath + "/delta_0000002_0000002_0000/bucket_00000",
                tablePath + "/delta_0000003_0000003_0000/_orc_acid_version",
                tablePath + "/delta_0000003_0000003_0000/bucket_00000");

        for (String path : filePaths) {
            File file = new File(path);
            assertTrue(file.getParentFile().exists() || file.getParentFile().mkdirs(), "Failed creating directory " + file.getParentFile());
            createOrcAcidFile(file);
        }

        // ValidWriteIdList is of format <currentTxn>$<schema>.<table>:<highWatermark>:<minOpenWriteId>::<AbortedTxns>
        // This writeId list has high watermark transaction=3 and aborted transaction=2
        String validWriteIdsList = format("4$%s.%s:3:9223372036854775807::2", table.getDatabaseName(), table.getTableName());

        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                HDFS_ENVIRONMENT,
                TupleDomain.none(),
                Optional.empty(),
                table,
                Optional.empty(),
                Optional.of(new ValidReaderWriteIdList(validWriteIdsList)));

        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);
        List<String> splits = drain(hiveSplitSource);
        assertTrue(splits.stream().anyMatch(p -> p.contains(filePaths.get(1))), format("%s not found in splits %s", filePaths.get(1), splits));
        assertTrue(splits.stream().anyMatch(p -> p.contains(filePaths.get(5))), format("%s not found in splits %s", filePaths.get(5), splits));

        deleteRecursively(tablePath, ALLOW_INSECURE);
    }

    @Test
    public void testFullAcidTableWithOriginalFiles()
            throws Exception
    {
        java.nio.file.Path tablePath = Files.createTempDirectory("TestBackgroundHiveSplitLoader");
        Table table = table(
                tablePath.toString(),
                ImmutableList.of(),
                Optional.empty(),
                ImmutableMap.of("transactional", "true"));

        String originalFile = tablePath + "/000000_1";
        List<String> filePaths = ImmutableList.of(
                tablePath + "/delta_0000002_0000002_0000/_orc_acid_version",
                tablePath + "/delta_0000002_0000002_0000/bucket_00000");

        for (String path : filePaths) {
            File file = new File(path);
            assertTrue(file.getParentFile().exists() || file.getParentFile().mkdirs(), "Failed creating directory " + file.getParentFile());
            createOrcAcidFile(file);
        }
        Files.write(Paths.get(originalFile), "test".getBytes(UTF_8));

        // ValidWriteIdsList is of format <currentTxn>$<schema>.<table>:<highWatermark>:<minOpenWriteId>::<AbortedTxns>
        // This writeId list has high watermark transaction=3
        ValidReaderWriteIdList validWriteIdsList = new ValidReaderWriteIdList(format("4$%s.%s:3:9223372036854775807::", table.getDatabaseName(), table.getTableName()));

        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                HDFS_ENVIRONMENT,
                TupleDomain.all(),
                Optional.empty(),
                table,
                Optional.empty(),
                Optional.of(validWriteIdsList));
        HiveSplitSource hiveSplitSource = hiveSplitSource(backgroundHiveSplitLoader);
        backgroundHiveSplitLoader.start(hiveSplitSource);
        List<String> splits = drain(hiveSplitSource);
        assertTrue(splits.stream().anyMatch(p -> p.contains(originalFile)), format("%s not found in splits %s", filePaths.get(0), splits));
        assertTrue(splits.stream().anyMatch(p -> p.contains(filePaths.get(1))), format("%s not found in splits %s", filePaths.get(1), splits));
    }

    @Test
    public void testVersionValidationNoOrcAcidVersionFile()
            throws Exception
    {
        java.nio.file.Path tablePath = Files.createTempDirectory("TestBackgroundHiveSplitLoader");
        Table table = table(
                tablePath.toString(),
                ImmutableList.of(),
                Optional.empty(),
                ImmutableMap.of("transactional", "true"));

        List<String> filePaths = ImmutableList.of(
                tablePath + "/000000_1",
                // no /delta_0000002_0000002_0000/_orc_acid_version file
                tablePath + "/delta_0000002_0000002_0000/bucket_00000");

        for (String path : filePaths) {
            File file = new File(path);
            assertTrue(file.getParentFile().exists() || file.getParentFile().mkdirs(), "Failed creating directory " + file.getParentFile());
            createOrcAcidFile(file);
        }

        // ValidWriteIdsList is of format <currentTxn>$<schema>.<table>:<highWatermark>:<minOpenWriteId>::<AbortedTxns>
        // This writeId list has high watermark transaction=3
        ValidReaderWriteIdList validWriteIdsList = new ValidReaderWriteIdList(format("4$%s.%s:3:9223372036854775807::", table.getDatabaseName(), table.getTableName()));

        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                HDFS_ENVIRONMENT,
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

        deleteRecursively(tablePath, ALLOW_INSECURE);
    }

    @Test
    public void testVersionValidationOrcAcidVersionFileHasVersion2()
            throws Exception
    {
        java.nio.file.Path tablePath = Files.createTempDirectory("TestBackgroundHiveSplitLoader");
        Table table = table(
                tablePath.toString(),
                ImmutableList.of(),
                Optional.empty(),
                ImmutableMap.of("transactional", "true"));

        List<String> filePaths = ImmutableList.of(
                tablePath + "/000000_1", // _orc_acid_version does not exist so it's assumed to be "ORC ACID version 0"
                tablePath + "/delta_0000002_0000002_0000/_orc_acid_version",
                tablePath + "/delta_0000002_0000002_0000/bucket_00000");

        for (String path : filePaths) {
            File file = new File(path);
            assertTrue(file.getParentFile().exists() || file.getParentFile().mkdirs(), "Failed creating directory " + file.getParentFile());
            createOrcAcidFile(file, 2);
        }

        // ValidWriteIdsList is of format <currentTxn>$<schema>.<table>:<highWatermark>:<minOpenWriteId>::<AbortedTxns>
        // This writeId list has high watermark transaction=3
        ValidReaderWriteIdList validWriteIdsList = new ValidReaderWriteIdList(format("4$%s.%s:3:9223372036854775807::", table.getDatabaseName(), table.getTableName()));

        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                HDFS_ENVIRONMENT,
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

        deleteRecursively(tablePath, ALLOW_INSECURE);
    }

    @Test
    public void testVersionValidationOrcAcidVersionFileHasVersion1()
            throws Exception
    {
        java.nio.file.Path tablePath = Files.createTempDirectory("TestBackgroundHiveSplitLoader");
        Table table = table(
                tablePath.toString(),
                ImmutableList.of(),
                Optional.empty(),
                ImmutableMap.of("transactional", "true"));

        List<String> filePaths = ImmutableList.of(
                tablePath + "/000000_1",
                tablePath + "/delta_0000002_0000002_0000/_orc_acid_version",
                tablePath + "/delta_0000002_0000002_0000/bucket_00000");

        for (String path : filePaths) {
            File file = new File(path);
            assertTrue(file.getParentFile().exists() || file.getParentFile().mkdirs(), "Failed creating directory " + file.getParentFile());
            // _orc_acid_version_exists but has version 1
            createOrcAcidFile(file, 1);
        }

        // ValidWriteIdsList is of format <currentTxn>$<schema>.<table>:<highWatermark>:<minOpenWriteId>::<AbortedTxns>
        // This writeId list has high watermark transaction=3
        ValidReaderWriteIdList validWriteIdsList = new ValidReaderWriteIdList(format("4$%s.%s:3:9223372036854775807::", table.getDatabaseName(), table.getTableName()));

        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                HDFS_ENVIRONMENT,
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

        deleteRecursively(tablePath, ALLOW_INSECURE);
    }

    @Test
    public void testValidateFileBuckets()
    {
        ListMultimap<Integer, LocatedFileStatus> bucketFiles = ArrayListMultimap.create();
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
            throws Exception
    {
        CachingDirectoryLister directoryLister = new CachingDirectoryLister(new Duration(5, TimeUnit.MINUTES), 1000, ImmutableList.of());
        Properties schema = new Properties();
        schema.setProperty(FILE_INPUT_FORMAT, SymlinkTextInputFormat.class.getName());
        schema.setProperty(SERIALIZATION_LIB, AVRO.getSerde());

        Path firstFilePath = new Path("hdfs://VOL1:9000/db_name/table_name/file1");
        Path secondFilePath = new Path("hdfs://VOL1:9000/db_name/table_name/file2");
        List<Path> paths = ImmutableList.of(firstFilePath, secondFilePath);
        List<LocatedFileStatus> files = paths.stream()
                .map(TestBackgroundHiveSplitLoader::locatedFileStatus)
                .collect(toImmutableList());

        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                files,
                directoryLister);
        Optional<Iterator<InternalHiveSplit>> splitIterator = backgroundHiveSplitLoader.buildManifestFileIterator(
                new AvroContainerInputFormat(),
                "partition",
                schema,
                ImmutableList.of(),
                TupleDomain.all(),
                () -> true,
                false,
                TableToPartitionMapping.empty(),
                new Path("hdfs://VOL1:9000/db_name/table_name"),
                paths,
                true);
        assertTrue(splitIterator.isPresent());
        List<InternalHiveSplit> splits = ImmutableList.copyOf(splitIterator.get());
        assertEquals(splits.size(), 2);
        assertEquals(splits.get(0).getPath(), firstFilePath.toString());
        assertEquals(splits.get(1).getPath(), secondFilePath.toString());
    }

    @Test
    public void testBuildManifestFileIteratorNestedDirectory()
            throws Exception
    {
        CachingDirectoryLister directoryLister = new CachingDirectoryLister(new Duration(5, TimeUnit.MINUTES), 1000, ImmutableList.of());
        Properties schema = new Properties();
        schema.setProperty(FILE_INPUT_FORMAT, SymlinkTextInputFormat.class.getName());
        schema.setProperty(SERIALIZATION_LIB, AVRO.getSerde());

        Path filePath = new Path("hdfs://VOL1:9000/db_name/table_name/file1");
        Path directoryPath = new Path("hdfs://VOL1:9000/db_name/table_name/dir");
        List<Path> paths = ImmutableList.of(filePath, directoryPath);
        List<LocatedFileStatus> files = ImmutableList.of(
                locatedFileStatus(filePath),
                locatedDirectoryStatus(directoryPath));

        BackgroundHiveSplitLoader backgroundHiveSplitLoader = backgroundHiveSplitLoader(
                files,
                directoryLister);
        Optional<Iterator<InternalHiveSplit>> splitIterator = backgroundHiveSplitLoader.buildManifestFileIterator(
                new AvroContainerInputFormat(),
                "partition",
                schema,
                ImmutableList.of(),
                TupleDomain.all(),
                () -> true,
                false,
                TableToPartitionMapping.empty(),
                new Path("hdfs://VOL1:9000/db_name/table_name"),
                paths,
                false);
        assertTrue(splitIterator.isEmpty());
    }

    private static void createOrcAcidFile(File file)
            throws IOException
    {
        createOrcAcidFile(file, 2);
    }

    private static void createOrcAcidFile(File file, int orcAcidVersion)
            throws IOException
    {
        if (file.getName().equals("_orc_acid_version")) {
            Files.write(file.toPath(), String.valueOf(orcAcidVersion).getBytes(UTF_8));
            return;
        }
        Files.copy(getResource("fullacidNationTableWithOriginalFiles/000000_0").openStream(), file.toPath());
    }

    private static List<String> drain(HiveSplitSource source)
            throws Exception
    {
        return drainSplits(source).stream()
                .map(HiveSplit::getPath)
                .collect(toImmutableList());
    }

    private static List<HiveSplit> drainSplits(HiveSplitSource source)
            throws Exception
    {
        ImmutableList.Builder<HiveSplit> splits = ImmutableList.builder();
        while (!source.isFinished()) {
            ConnectorSplitBatch batch;
            try {
                batch = source.getNextBatch(NOT_PARTITIONED, 100).get();
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
    {
        return backgroundHiveSplitLoader(
                new TestingHdfsEnvironment(TEST_FILES),
                TupleDomain.all(),
                dynamicFilter,
                dynamicFilteringProbeBlockingTimeoutMillis,
                Optional.empty(),
                SIMPLE_TABLE,
                Optional.empty(),
                Optional.empty());
    }

    private BackgroundHiveSplitLoader backgroundHiveSplitLoader(
            List<LocatedFileStatus> files,
            TupleDomain<HiveColumnHandle> tupleDomain)
    {
        return backgroundHiveSplitLoader(
                files,
                tupleDomain,
                Optional.empty(),
                SIMPLE_TABLE,
                Optional.empty());
    }

    private BackgroundHiveSplitLoader backgroundHiveSplitLoader(
            List<LocatedFileStatus> files,
            TupleDomain<HiveColumnHandle> compactEffectivePredicate,
            Optional<HiveBucketFilter> hiveBucketFilter,
            Table table,
            Optional<HiveBucketHandle> bucketHandle)
    {
        return backgroundHiveSplitLoader(
                files,
                compactEffectivePredicate,
                hiveBucketFilter,
                table,
                bucketHandle,
                Optional.empty());
    }

    private BackgroundHiveSplitLoader backgroundHiveSplitLoader(
            List<LocatedFileStatus> files,
            TupleDomain<HiveColumnHandle> compactEffectivePredicate,
            Optional<HiveBucketFilter> hiveBucketFilter,
            Table table,
            Optional<HiveBucketHandle> bucketHandle,
            Optional<ValidWriteIdList> validWriteIds)
    {
        return backgroundHiveSplitLoader(
                new TestingHdfsEnvironment(files),
                compactEffectivePredicate,
                hiveBucketFilter,
                table,
                bucketHandle,
                validWriteIds);
    }

    private BackgroundHiveSplitLoader backgroundHiveSplitLoader(
            HdfsEnvironment hdfsEnvironment,
            TupleDomain<HiveColumnHandle> compactEffectivePredicate,
            Optional<HiveBucketFilter> hiveBucketFilter,
            Table table,
            Optional<HiveBucketHandle> bucketHandle,
            Optional<ValidWriteIdList> validWriteIds)
    {
        return backgroundHiveSplitLoader(
                hdfsEnvironment,
                compactEffectivePredicate,
                DynamicFilter.EMPTY,
                new Duration(0, SECONDS),
                hiveBucketFilter,
                table,
                bucketHandle,
                validWriteIds);
    }

    private BackgroundHiveSplitLoader backgroundHiveSplitLoader(
            HdfsEnvironment hdfsEnvironment,
            TupleDomain<HiveColumnHandle> compactEffectivePredicate,
            DynamicFilter dynamicFilter,
            Duration dynamicFilteringProbeBlockingTimeout,
            Optional<HiveBucketFilter> hiveBucketFilter,
            Table table,
            Optional<HiveBucketHandle> bucketHandle,
            Optional<ValidWriteIdList> validWriteIds)
    {
        List<HivePartitionMetadata> hivePartitionMetadatas =
                ImmutableList.of(
                        new HivePartitionMetadata(
                                new HivePartition(new SchemaTableName("testSchema", "table_name")),
                                Optional.empty(),
                                TableToPartitionMapping.empty()));

        return new BackgroundHiveSplitLoader(
                table,
                NO_ACID_TRANSACTION,
                hivePartitionMetadatas,
                compactEffectivePredicate,
                dynamicFilter,
                dynamicFilteringProbeBlockingTimeout,
                TESTING_TYPE_MANAGER,
                createBucketSplitInfo(bucketHandle, hiveBucketFilter),
                SESSION,
                hdfsEnvironment,
                new NamenodeStats(),
                new CachingDirectoryLister(new HiveConfig()),
                executor,
                2,
                false,
                false,
                true,
                validWriteIds,
                Optional.empty());
    }

    private BackgroundHiveSplitLoader backgroundHiveSplitLoader(List<LocatedFileStatus> files, DirectoryLister directoryLister)
    {
        List<HivePartitionMetadata> hivePartitionMetadatas = ImmutableList.of(
                new HivePartitionMetadata(
                        new HivePartition(new SchemaTableName("testSchema", "table_name")),
                        Optional.empty(),
                        TableToPartitionMapping.empty()));

        ConnectorSession connectorSession = getHiveSession(new HiveConfig()
                .setMaxSplitSize(DataSize.of(1, GIGABYTE)));

        return new BackgroundHiveSplitLoader(
                SIMPLE_TABLE,
                NO_ACID_TRANSACTION,
                hivePartitionMetadatas,
                TupleDomain.none(),
                DynamicFilter.EMPTY,
                new Duration(0, SECONDS),
                TESTING_TYPE_MANAGER,
                Optional.empty(),
                connectorSession,
                new TestingHdfsEnvironment(files),
                new NamenodeStats(),
                directoryLister,
                executor,
                2,
                false,
                false,
                true,
                Optional.empty(),
                Optional.empty());
    }

    private static BackgroundHiveSplitLoader backgroundHiveSplitLoaderOfflinePartitions()
    {
        ConnectorSession connectorSession = getHiveSession(new HiveConfig()
                .setMaxSplitSize(DataSize.of(1, GIGABYTE)));

        return new BackgroundHiveSplitLoader(
                SIMPLE_TABLE,
                NO_ACID_TRANSACTION,
                createPartitionMetadataWithOfflinePartitions(),
                TupleDomain.all(),
                DynamicFilter.EMPTY,
                new Duration(0, SECONDS),
                TESTING_TYPE_MANAGER,
                createBucketSplitInfo(Optional.empty(), Optional.empty()),
                connectorSession,
                new TestingHdfsEnvironment(TEST_FILES),
                new NamenodeStats(),
                new CachingDirectoryLister(new HiveConfig()),
                directExecutor(),
                2,
                false,
                false,
                true,
                Optional.empty(),
                Optional.empty());
    }

    private static Iterable<HivePartitionMetadata> createPartitionMetadataWithOfflinePartitions()
            throws RuntimeException
    {
        return () -> new AbstractIterator<>()
        {
            // This iterator is crafted to return a valid partition for the first calls to
            // hasNext() and next(), and then it should throw for the second call to hasNext()
            private int position = -1;

            @Override
            protected HivePartitionMetadata computeNext()
            {
                position++;
                switch (position) {
                    case 0:
                        return new HivePartitionMetadata(
                                new HivePartition(new SchemaTableName("testSchema", "table_name")),
                                Optional.empty(),
                                TableToPartitionMapping.empty());
                    case 1:
                        throw new RuntimeException("OFFLINE");
                    default:
                        return endOfData();
                }
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
                false);
    }

    private static Table table(
            List<Column> partitionColumns,
            Optional<HiveBucketProperty> bucketProperty,
            ImmutableMap<String, String> tableParameters)
    {
        return table(partitionColumns,
                bucketProperty,
                tableParameters,
                StorageFormat.create(
                        "com.facebook.hive.orc.OrcSerde",
                        "org.apache.hadoop.hive.ql.io.RCFileInputFormat",
                        "org.apache.hadoop.hive.ql.io.RCFileInputFormat"));
    }

    private static Table table(
            String location,
            List<Column> partitionColumns,
            Optional<HiveBucketProperty> bucketProperty,
            ImmutableMap<String, String> tableParameters)
    {
        return table(location,
                partitionColumns,
                bucketProperty,
                tableParameters,
                StorageFormat.create(
                        "com.facebook.hive.orc.OrcSerde",
                        "org.apache.hadoop.hive.ql.io.RCFileInputFormat",
                        "org.apache.hadoop.hive.ql.io.RCFileInputFormat"));
    }

    private static Table table(
            List<Column> partitionColumns,
            Optional<HiveBucketProperty> bucketProperty,
            Map<String, String> tableParameters,
            StorageFormat storageFormat)
    {
        return table("hdfs://VOL1:9000/db_name/table_name",
                partitionColumns,
                bucketProperty,
                tableParameters,
                storageFormat);
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
                .setTableType(TableType.MANAGED_TABLE.toString())
                .setDataColumns(ImmutableList.of(new Column("col1", HIVE_STRING, Optional.empty())))
                .setParameters(tableParameters)
                .setPartitionColumns(partitionColumns)
                .build();
    }

    private static LocatedFileStatus locatedFileStatus(Path path)
    {
        return locatedFileStatus(path, 10);
    }

    private static LocatedFileStatus locatedFileStatus(Path path, long fileLength)
    {
        return new LocatedFileStatus(
                fileLength,
                false,
                0,
                0L,
                0L,
                0L,
                null,
                null,
                null,
                null,
                path,
                new BlockLocation[] {new BlockLocation(new String[1], new String[] {"localhost"}, 0, fileLength)});
    }

    private static LocatedFileStatus locatedFileStatusWithNoBlocks(Path path)
    {
        return new LocatedFileStatus(
                0L,
                false,
                0,
                0L,
                0L,
                0L,
                null,
                null,
                null,
                null,
                path,
                new BlockLocation[] {});
    }

    private static LocatedFileStatus locatedDirectoryStatus(Path path)
    {
        return new LocatedFileStatus(
                0L,
                true,
                0,
                0L,
                0L,
                0L,
                null,
                null,
                null,
                null,
                path,
                new BlockLocation[] {});
    }

    public static class TestingHdfsEnvironment
            extends HdfsEnvironment
    {
        private final List<LocatedFileStatus> files;

        public TestingHdfsEnvironment(List<LocatedFileStatus> files)
        {
            super(
                    new HiveHdfsConfiguration(
                            new HdfsConfigurationInitializer(new HdfsConfig()),
                            ImmutableSet.of()),
                    new HdfsConfig(),
                    new NoHdfsAuthentication());
            this.files = ImmutableList.copyOf(files);
        }

        @Override
        public FileSystem getFileSystem(ConnectorIdentity identity, Path path, Configuration configuration)
        {
            return new TestingHdfsFileSystem(files);
        }
    }

    private static class TestingHdfsFileSystem
            extends FileSystem
    {
        private final List<LocatedFileStatus> files;

        public TestingHdfsFileSystem(List<LocatedFileStatus> files)
        {
            this.files = ImmutableList.copyOf(files);
        }

        @Override
        public boolean delete(Path f, boolean recursive)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean rename(Path src, Path dst)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setWorkingDirectory(Path dir)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileStatus[] listStatus(Path f)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f)
        {
            return new RemoteIterator<>()
            {
                private final Iterator<LocatedFileStatus> iterator = files.iterator();

                @Override
                public boolean hasNext()
                {
                    return iterator.hasNext();
                }

                @Override
                public LocatedFileStatus next()
                {
                    return iterator.next();
                }
            };
        }

        @Override
        public FSDataOutputStream create(
                Path f,
                FsPermission permission,
                boolean overwrite,
                int bufferSize,
                short replication,
                long blockSize,
                Progressable progress)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean mkdirs(Path f, FsPermission permission)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FSDataInputStream open(Path f, int bufferSize)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileStatus getFileStatus(Path f)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Path getWorkingDirectory()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public URI getUri()
        {
            throw new UnsupportedOperationException();
        }
    }
}
