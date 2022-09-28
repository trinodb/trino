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
package io.trino.plugin.hive.metastore;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.HiveBucketProperty;
import io.trino.plugin.hive.HiveMetastoreClosure;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.PartitionStatistics;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.fs.FileSystemDirectoryLister;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.plugin.hive.HiveBasicStatistics.createEmptyStatistics;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.acid.AcidOperation.INSERT;
import static io.trino.plugin.hive.util.HiveBucketing.BucketingVersion.BUCKETING_V1;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertTrue;

// countDownLatch field is shared between tests
@Test(singleThreaded = true)
public class TestSemiTransactionalHiveMetastore
{
    private static final Column TABLE_COLUMN = new Column(
            "column",
            HiveType.HIVE_INT,
            Optional.of("comment"));
    private static final Storage TABLE_STORAGE = new Storage(
            StorageFormat.create("serde", "input", "output"),
            Optional.of("location"),
            Optional.of(new HiveBucketProperty(ImmutableList.of("column"), BUCKETING_V1, 10, ImmutableList.of(new SortingColumn("column", SortingColumn.Order.ASCENDING)))),
            true,
            ImmutableMap.of("param", "value2"));

    private CountDownLatch countDownLatch;

    @Test
    public void testParallelPartitionDrops()
    {
        int partitionsToDrop = 5;
        IntStream dropThreadsConfig = IntStream.of(1, 2);
        dropThreadsConfig.forEach(dropThreads -> {
            countDownLatch = new CountDownLatch(dropThreads);
            SemiTransactionalHiveMetastore semiTransactionalHiveMetastore = getSemiTransactionalHiveMetastoreWithDropExecutor(newFixedThreadPool(dropThreads));
            IntStream.range(0, partitionsToDrop).forEach(i -> semiTransactionalHiveMetastore.dropPartition(SESSION,
                    "test",
                    "test",
                    ImmutableList.of(String.valueOf(i)),
                    true));
            semiTransactionalHiveMetastore.commit();
        });
    }

    private SemiTransactionalHiveMetastore getSemiTransactionalHiveMetastoreWithDropExecutor(Executor dropExecutor)
    {
        return new SemiTransactionalHiveMetastore(HDFS_ENVIRONMENT,
                new HiveMetastoreClosure(new TestingHiveMetastore()),
                directExecutor(),
                dropExecutor,
                directExecutor(),
                false,
                false,
                true,
                Optional.empty(),
                newScheduledThreadPool(1),
                new FileSystemDirectoryLister());
    }

    @Test
    public void testParallelUpdateStatisticsOperations()
    {
        int tablesToUpdate = 5;
        IntStream updateThreadsConfig = IntStream.of(1, 2);
        updateThreadsConfig.forEach(updateThreads -> {
            countDownLatch = new CountDownLatch(updateThreads);
            SemiTransactionalHiveMetastore semiTransactionalHiveMetastore;
            if (updateThreads == 1) {
                semiTransactionalHiveMetastore = getSemiTransactionalHiveMetastoreWithUpdateExecutor(directExecutor());
            }
            else {
                semiTransactionalHiveMetastore = getSemiTransactionalHiveMetastoreWithUpdateExecutor(newFixedThreadPool(updateThreads));
            }
            IntStream.range(0, tablesToUpdate).forEach(i -> semiTransactionalHiveMetastore.finishChangingExistingTable(INSERT, SESSION,
                    "database",
                    "table_" + i,
                    new Path("location"),
                    ImmutableList.of(),
                    PartitionStatistics.empty(),
                    false));
            semiTransactionalHiveMetastore.commit();
        });
    }

    private SemiTransactionalHiveMetastore getSemiTransactionalHiveMetastoreWithUpdateExecutor(Executor updateExecutor)
    {
        return new SemiTransactionalHiveMetastore(HDFS_ENVIRONMENT,
                new HiveMetastoreClosure(new TestingHiveMetastore()),
                directExecutor(),
                directExecutor(),
                updateExecutor,
                false,
                false,
                true,
                Optional.empty(),
                newScheduledThreadPool(1),
                new FileSystemDirectoryLister());
    }

    private class TestingHiveMetastore
            extends UnimplementedHiveMetastore
    {
        @Override
        public Optional<Table> getTable(String databaseName, String tableName)
        {
            if (databaseName.equals("database")) {
                return Optional.of(new Table(
                        "database",
                        tableName,
                        Optional.of("owner"),
                        "table_type",
                        TABLE_STORAGE,
                        ImmutableList.of(TABLE_COLUMN),
                        ImmutableList.of(TABLE_COLUMN),
                        ImmutableMap.of("param", "value3"),
                        Optional.of("original_text"),
                        Optional.of("expanded_text"),
                        OptionalLong.empty()));
            }
            return Optional.empty();
        }

        @Override
        public PartitionStatistics getTableStatistics(Table table)
        {
            return new PartitionStatistics(createEmptyStatistics(), ImmutableMap.of());
        }

        @Override
        public void dropPartition(String databaseName, String tableName, List<String> parts, boolean deleteData)
        {
            assertCountDownLatch();
        }

        @Override
        public void updateTableStatistics(String databaseName,
                String tableName,
                AcidTransaction transaction,
                Function<PartitionStatistics, PartitionStatistics> update)
        {
            assertCountDownLatch();
        }

        private void assertCountDownLatch()
        {
            try {
                countDownLatch.countDown();
                assertTrue(countDownLatch.await(10, TimeUnit.SECONDS)); //all other threads launched should count down within 10 seconds
            }
            catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
