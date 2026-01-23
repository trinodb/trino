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
package io.trino.plugin.postgresql;

import io.trino.plugin.blackhole.BlackHolePlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryAssertions;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.plugin.base.TemporaryTables.TEMPORARY_TABLE_NAME_PREFIX;
import static io.trino.plugin.jdbc.BaseJdbcConnectorTest.getQueryId;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD) // Asserts between each test that no temp table exists
public class TestPostgreSqlRollbacks
        extends AbstractTestQueryFramework
{
    private TestingPostgreSqlServer postgreSqlServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        postgreSqlServer = closeAfterClass(new TestingPostgreSqlServer());
        DistributedQueryRunner queryRunner = PostgreSqlQueryRunner.builder(postgreSqlServer)
                .build();
        queryRunner.installPlugin(new BlackHolePlugin());
        queryRunner.createCatalog("blackhole", "blackhole", Map.of());
        return queryRunner;
    }

    @AfterEach
    public void ensureNoTemporaryTablesRemain()
    {
        // assertEventually as cleanup might be asynchronous from query completion
        assertEventually(() -> {
            assertThat(getQueryRunner().execute("SHOW TABLES").getOnlyColumn()).isEmpty();
        });
    }

    /**
     * @see BasePostgresFailureRecoveryTest#testRollbackCreateTableAsSelect()
     */
    @Test
    @Timeout(60)
    public void testRollbackCreateTableAsSelect()
            throws Exception
    {
        QueryRunner queryRunner = getQueryRunner();
        doTestRollbackCreateTableAsSelect(queryRunner);

        // Verify no tables remain, not even temporary ones
        assertThat(queryRunner.execute("SHOW TABLES").getOnlyColumn()).isEmpty();
    }

    static void doTestRollbackCreateTableAsSelect(QueryRunner queryRunner)
            throws Exception
    {
        String destinationTable = "test_rollback_ctas_" + randomNameSuffix();
        try (TestTable slowSource = new TestTable(
                queryRunner::execute,
                "blackhole.default.slow_source",
                "(i bigint) WITH (split_count = 1, pages_per_split = 10, rows_per_page = 1, page_processing_delay = '10s')");
                ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("TestPostgreSqlRollbacks-doTestRollbackCreateTableAsSelect-%s"))) {
            // Use completion service so that first failure is propagated
            CompletionService<?> completionService = new ExecutorCompletionService<>(executor);

            String query = "CREATE TABLE " + destinationTable + " AS SELECT * FROM " + slowSource.getName();
            completionService.submit(() -> {
                QueryAssertions.assertQueryFails(queryRunner, queryRunner.getDefaultSession(), query, "Query killed. Message: Killed by test");
                return null;
            });

            completionService.submit(() -> {
                // Wait for the first query to start
                String queryId = getQueryId(queryRunner, query).id();

                // Wait for the first query to create some tables
                assertEventually(() -> {
                    if (!queryRunner.tableExists(queryRunner.getDefaultSession(), destinationTable)) {
                        fail("CTAS destination table %s does not exist yet".formatted(destinationTable));
                    }
                    if (!anyTemporaryTableExists(queryRunner)) {
                        fail("CTAS temporary table does not exist yet");
                    }
                });

                QueryAssertions.assertUpdate(queryRunner, queryRunner.getDefaultSession(), format("CALL system.runtime.kill_query(query_id => '%s', message => '%s')", queryId, "Killed by test"), OptionalLong.empty());
                return null;
            });

            completionService.take().get();
            completionService.take().get();
        }

        // The canceled query returns immediately but cleanup can be asynchronous
        assertEventually(() -> {
            if (queryRunner.tableExists(queryRunner.getDefaultSession(), destinationTable)) {
                fail("CTAS destination table should not exists after CTAS query is cancelled");
            }
        });
    }

    @Test
    @Timeout(60)
    public void testRollbackMerge()
            throws Exception
    {
        QueryRunner queryRunner = getQueryRunner();

        try (TestTable destinationTable = new TestTable(queryRunner::execute, "test_rollback_merge", "(customer VARCHAR, purchases INT, address VARCHAR)");
                TestTable slowSource = new TestTable(
                        queryRunner::execute,
                        "blackhole.default.slow_source",
                        "(i bigint) WITH (split_count = 1, pages_per_split = 10, rows_per_page = 1, page_processing_delay = '10s')");
                ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("TestPostgreSqlRollbacks-testRollbackMerge-%s"))) {
            // setup
            postgreSqlServer.execute("ALTER TABLE " + destinationTable.getName() + " ADD CONSTRAINT pk PRIMARY KEY (customer)");
            assertUpdate("INSERT INTO " + destinationTable.getName() + " (customer, purchases, address) VALUES ('Aaron', 11, 'Antioch'), ('Bill', 7, 'Buena')", 2);

            // Use completion service so that first failure is propagated
            CompletionService<?> completionService = new ExecutorCompletionService<>(executor);

            String query = "MERGE INTO " + destinationTable.getName() + " t " +
                    " USING (SELECT 'Carol' AS customer, 9 AS purchases, 'Centreville' AS address FROM " + slowSource.getName() + ") s ON (t.customer = s.customer)" +
                    " WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)";
            completionService.submit(() -> {
                QueryAssertions.assertQueryFails(queryRunner, queryRunner.getDefaultSession(), query, "Query killed. Message: Killed by test");
                return null;
            });

            completionService.submit(() -> {
                // Wait for the first query to start
                String queryId = getQueryId(queryRunner, query).id();

                // Wait for the first query to create some tables
                assertEventually(() -> {
                    if (!anyTemporaryTableExists(queryRunner)) {
                        fail("CTAS temporary table does not exist yet");
                    }
                });

                QueryAssertions.assertUpdate(queryRunner, queryRunner.getDefaultSession(), format("CALL system.runtime.kill_query(query_id => '%s', message => '%s')", queryId, "Killed by test"), OptionalLong.empty());
                return null;
            });

            completionService.take().get();
            completionService.take().get();
        }

        // Verify no tables remain, not even temporary ones
        assertThat(queryRunner.execute("SHOW TABLES").getOnlyColumn()).isEmpty();
    }

    private static boolean anyTemporaryTableExists(QueryRunner queryRunner)
    {
        return queryRunner.execute("SHOW TABLES")
                .getOnlyColumn()
                .map(String.class::cast)
                .anyMatch(tableName -> tableName.startsWith(TEMPORARY_TABLE_NAME_PREFIX));
    }
}
