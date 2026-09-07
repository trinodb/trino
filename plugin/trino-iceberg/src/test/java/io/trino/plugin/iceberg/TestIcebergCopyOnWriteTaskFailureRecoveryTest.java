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
import com.google.inject.Module;
import io.trino.operator.RetryPolicy;
import io.trino.plugin.exchange.filesystem.containers.MinioStorage;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.exchange.filesystem.containers.MinioStorage.getExchangeManagerProperties;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.apache.iceberg.FileContent.EQUALITY_DELETES;
import static org.apache.iceberg.FileContent.POSITION_DELETES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

/**
 * Exercises copy-on-write DELETE/UPDATE/MERGE under fault-tolerant execution
 * ({@code retry-policy=TASK}) with injected task failures. Copy-on-write rewrites whole
 * data files on workers and commits an {@code OverwriteFiles} operation on the coordinator,
 * so it must survive task-level retries and spooled exchanges. The created tables set
 * {@code write.merge.mode=copy-on-write}, otherwise the inherited harness would only cover
 * merge-on-read (see {@link TestIcebergTaskFailureRecoveryTest}).
 */
@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestIcebergCopyOnWriteTaskFailureRecoveryTest
        extends BaseIcebergFailureRecoveryTest
{
    private static final String COPY_ON_WRITE = "extra_properties = MAP(ARRAY['write.merge.mode'], ARRAY['copy-on-write'])";

    private MinioStorage minioStorage;

    protected TestIcebergCopyOnWriteTaskFailureRecoveryTest()
    {
        super(RetryPolicy.TASK);
    }

    @Override
    protected QueryRunner createQueryRunner(
            List<TpchTable<?>> requiredTpchTables,
            Map<String, String> configProperties,
            Map<String, String> coordinatorProperties,
            Module failureInjectionModule)
            throws Exception
    {
        this.minioStorage = closeAfterClass(new MinioStorage("test-exchange-spooling-" + randomNameSuffix()));
        minioStorage.start();

        return IcebergQueryRunner.builder()
                .setCoordinatorProperties(coordinatorProperties)
                .setExtraProperties(configProperties)
                .setIcebergProperties(ImmutableMap.of("iceberg.allowed-extra-properties", "*"))
                .withExchange("filesystem", getExchangeManagerProperties(minioStorage))
                .setAdditionalModule(failureInjectionModule)
                .setInitialTables(requiredTpchTables)
                .build();
    }

    @Test
    @Override
    protected void testDelete()
    {
        testTableModification(
                Optional.of("CREATE TABLE <table> WITH (" + COPY_ON_WRITE + ") AS SELECT * FROM orders"),
                "DELETE FROM <table> WHERE orderkey = 1",
                Optional.of("DROP TABLE <table>"));
        assertCopyOnWriteLeavesNoDeleteFiles(
                "CREATE TABLE <table> WITH (" + COPY_ON_WRITE + ") AS SELECT * FROM orders",
                "DELETE FROM <table> WHERE orderkey = 1");
    }

    @Test
    @Override
    protected void testUpdate()
    {
        testTableModification(
                Optional.of("CREATE TABLE <table> WITH (" + COPY_ON_WRITE + ") AS SELECT * FROM orders"),
                "UPDATE <table> SET shippriority = 101 WHERE custkey = 1",
                Optional.of("DROP TABLE <table>"));
        assertCopyOnWriteLeavesNoDeleteFiles(
                "CREATE TABLE <table> WITH (" + COPY_ON_WRITE + ") AS SELECT * FROM orders",
                "UPDATE <table> SET shippriority = 101 WHERE custkey = 1");
    }

    @Test
    @Override
    protected void testCreatePartitionedTable()
    {
        testTableModification(
                Optional.empty(),
                "CREATE TABLE <table> WITH (partitioning = ARRAY['p'], " + COPY_ON_WRITE + ") AS SELECT *, 'partition1' p FROM orders",
                Optional.of("DROP TABLE <table>"));
    }

    @Test
    @Override
    protected void testInsertIntoNewPartition()
    {
        testTableModification(
                Optional.of("CREATE TABLE <table> WITH (partitioning = ARRAY['p'], " + COPY_ON_WRITE + ") AS SELECT *, 'partition1' p FROM orders"),
                "INSERT INTO <table> SELECT *, 'partition2' p FROM orders",
                Optional.of("DROP TABLE <table>"));
    }

    @Test
    @Override
    protected void testInsertIntoExistingPartition()
    {
        testTableModification(
                Optional.of("CREATE TABLE <table> WITH (partitioning = ARRAY['p'], " + COPY_ON_WRITE + ") AS SELECT *, 'partition1' p FROM orders"),
                "INSERT INTO <table> SELECT *, 'partition1' p FROM orders",
                Optional.of("DROP TABLE <table>"));
    }

    @Test
    @Override
    protected void testMergePartitionedTable()
    {
        testTableModification(
                Optional.of("CREATE TABLE <table> WITH (partitioning = ARRAY['bucket(orderkey, 10)'], " + COPY_ON_WRITE + ") AS SELECT * FROM orders"),
                """
                MERGE INTO <table> t
                USING (SELECT orderkey, 'X' clerk FROM <table>) s
                ON t.orderkey = s.orderkey
                WHEN MATCHED AND s.orderkey > 1000
                    THEN UPDATE SET clerk = t.clerk || s.clerk
                WHEN MATCHED AND s.orderkey <= 1000
                    THEN DELETE
                """,
                Optional.of("DROP TABLE <table>"));
        assertCopyOnWriteLeavesNoDeleteFiles(
                "CREATE TABLE <table> WITH (partitioning = ARRAY['bucket(orderkey, 10)'], " + COPY_ON_WRITE + ") AS SELECT * FROM orders",
                """
                MERGE INTO <table> t
                USING (SELECT orderkey, 'X' clerk FROM <table>) s
                ON t.orderkey = s.orderkey
                WHEN MATCHED AND s.orderkey > 1000
                    THEN UPDATE SET clerk = t.clerk || s.clerk
                WHEN MATCHED AND s.orderkey <= 1000
                    THEN DELETE
                """);
    }

    /**
     * Proves the copy-on-write path -- not merge-on-read -- is what runs under this
     * fault-tolerant runner ({@code retry-policy=TASK} is set globally on the query
     * runner). A copy-on-write {@code DELETE}/{@code UPDATE}/{@code MERGE} rewrites
     * whole data files and must leave zero position/equality delete files behind. The
     * harness-driven {@code testTableModification} above covers task-failure recovery
     * but never inspects the resulting table, so this guards against a silent
     * regression to merge-on-read.
     */
    private void assertCopyOnWriteLeavesNoDeleteFiles(String createTable, String dml)
    {
        String table = "test_cow_fte_no_delete_files_" + randomNameSuffix();
        try {
            // CTAS over the orders table returns an update count, so assert it rather than expecting no row count.
            assertUpdate(createTable.replace("<table>", table), 15000);
            getQueryRunner().execute(dml.replace("<table>", table));
            assertThat(computeScalar(
                    "SELECT count(*) FROM \"" + table + "$files\" WHERE content IN (" +
                            POSITION_DELETES.id() + ", " + EQUALITY_DELETES.id() + ")"))
                    .isEqualTo(0L);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + table);
        }
    }

    @AfterAll
    public void destroy()
            throws Exception
    {
        minioStorage = null; // closed by closeAfterClass
    }
}
