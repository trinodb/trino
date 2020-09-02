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
package io.prestosql.tests.hive;

<<<<<<< HEAD
=======
import com.google.inject.Inject;
import io.prestosql.plugin.hive.metastore.thrift.ThriftHiveMetastoreClient;
import io.prestosql.tempto.context.ThreadLocalTestContextHolder;
import io.prestosql.tempto.query.QueryExecutor;
import io.prestosql.tempto.query.QueryResult;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.thrift.TException;
>>>>>>> Acquire lock before dropping transactional table.
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

<<<<<<< HEAD
=======
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;
>>>>>>> Acquire lock before dropping transactional table.
import static io.prestosql.tempto.assertions.QueryAssert.Row.row;
import static io.prestosql.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tempto.query.QueryExecutor.defaultQueryExecutor;
import static io.prestosql.tempto.query.QueryExecutor.query;
import static io.prestosql.tests.TestGroups.HIVE_TRANSACTIONAL;
import static io.prestosql.tests.utils.QueryExecutors.onHive;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.joining;

public class TestHiveTransactionalTableInsert
        extends HiveProductTest
{
    @Inject
    private TransactionalTestHelper transactionalTestHelper;

    @Test(dataProvider = "transactionalTableType", groups = HIVE_TRANSACTIONAL)
    public void testInsertIntoTransactionalTable(TransactionalTableType type)
    {
        if (getHiveVersionMajor() < 3) {
            throw new SkipException("Hive transactional tables are supported with Hive version 3 or above");
        }

        String tableName = "test_insert_into_transactional_table_" + type.name().toLowerCase(ENGLISH);
        onHive().executeQuery("" +
                "CREATE TABLE " + tableName + "(a bigint)" +
                "CLUSTERED BY(a) INTO 4 BUCKETS STORED AS ORC " +
                hiveTableProperties(type));

        try {
            query("INSERT INTO " + tableName + " (a) VALUES (42)");
            assertThat(query("SELECT * FROM " + tableName))
                    .containsOnly(row(42));
        }
        finally {
            onHive().executeQuery("DROP TABLE " + tableName);
        }
    }

    @Test(dataProvider = "isTablePartitioned", groups = HIVE_TRANSACTIONAL)
    public void testDropTableTakeExclusiveLock(boolean isPartitioned)
            throws TException, InterruptedException
    {
        if (getHiveVersionMajor() < 3) {
            throw new SkipException("Hive transactional tables are supported with Hive version 3 or above");
        }

        String tableName = "drop_table_take_exclusive_lock_insert_only" + (isPartitioned ? "_partitioned" : "");

        String createTable = "CREATE TABLE IF NOT EXISTS " +
                tableName +
                " (col INT) " +
                (isPartitioned ? "PARTITIONED BY (part_col INT) " : "") +
                "STORED AS ORC " +
                "TBLPROPERTIES ('transactional_properties'='insert_only', 'transactional'='true') ";

        onHive().executeQuery(createTable);

        ThriftHiveMetastoreClient client = transactionalTestHelper.createMetastoreClient();
        ExecutorService executor = Executors.newFixedThreadPool(1);

        try {
            String hivePartitionStringPartition = isPartitioned ? " PARTITION (part_col=2) " : "";
            String predicate = isPartitioned ? " WHERE part_col = 2 " : "";

            onHive().executeQuery(
                    "INSERT OVERWRITE TABLE " + tableName + hivePartitionStringPartition + " select 1");

            String selectFromOnePartitionsSql = "SELECT col FROM " + tableName + predicate + " ORDER BY COL";
            QueryResult onePartitionQueryResult = query(selectFromOnePartitionsSql);
            assertThat(onePartitionQueryResult).containsOnly(row(1));

            long transactionId = client.openTransaction("test");

            // Acquire exclusive lock manually on table
            LockResponse lockResponse = transactionalTestHelper.acquireDropTableLock(client, "default", tableName, transactionId);
            checkState(lockResponse.getState() == LockState.ACQUIRED, "Drop table didn't acquire exclusive lock");

            String dropTable = "DROP TABLE " + tableName;
            // Run DROP TABLE command
            executor.execute(() -> defaultQueryExecutor().executeQuery(dropTable));

            Thread.sleep(5000);

            QueryExecutor hiveQueryExecutor = ThreadLocalTestContextHolder.testContext().getDependency(QueryExecutor.class, "hive");

            QueryResult queryResult = hiveQueryExecutor.executeQuery("SHOW TABLES");
            checkState(queryResult.rows().stream().filter(objects -> objects.get(0).equals(tableName)).count() == 1,
                    "Drop table shouldn't have finished, query result : " + queryResult.rows());

            // Rollback the transaction created in Step 1, to release the lock
            client.abortTransaction(transactionId);

            executor.shutdown();
            executor.awaitTermination(300, TimeUnit.SECONDS);

            // Check if table has dropped successfully
            queryResult = hiveQueryExecutor.executeQuery("SHOW TABLES");
            checkState(!queryResult.rows().stream().filter(objects -> objects.get(0).equals(tableName)).findAny().isPresent(),
                    "Drop table query didn't work: " + queryResult.rows());
        }
        finally {
            client.close();
            onHive().executeQuery("DROP TABLE " + tableName);
        }
    }

    @DataProvider
    public Object[][] isTablePartitioned()
    {
        return new Object[][] {
                {true},
                {false}
        };
    }

    @DataProvider
    public Object[][] transactionalTableType()
    {
        return new Object[][] {
                {TransactionalTableType.ACID},
                {TransactionalTableType.INSERT_ONLY},
        };
    }

    private static String hiveTableProperties(TransactionalTableType transactionalTableType)
    {
        return transactionalTableType.getHiveTableProperties().stream()
                .collect(joining(",", "TBLPROPERTIES (", ")"));
    }
}
