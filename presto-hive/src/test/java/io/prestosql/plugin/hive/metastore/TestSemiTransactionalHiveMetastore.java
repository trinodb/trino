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
package io.prestosql.plugin.hive.metastore;

import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.hive.HiveMetastoreClosure;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.prestosql.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.prestosql.testing.TestingConnectorSession.SESSION;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertTrue;

public class TestSemiTransactionalHiveMetastore
{
    private static CountDownLatch countDownLatch;

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
                false,
                false,
                Optional.empty(),
                newScheduledThreadPool(1));
    }

    private static class TestingHiveMetastore
            extends UnimplementedHiveMetastore
    {
        @Override
        public void dropPartition(HiveIdentity identity, String databaseName, String tableName, List<String> parts, boolean deleteData)
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
