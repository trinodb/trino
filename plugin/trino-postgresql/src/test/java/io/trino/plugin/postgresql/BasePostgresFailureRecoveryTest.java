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

import com.google.inject.Module;
import io.trino.Session;
import io.trino.operator.RetryPolicy;
import io.trino.plugin.blackhole.BlackHolePlugin;
import io.trino.plugin.jdbc.BaseJdbcFailureRecoveryTest;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.postgresql.TestPostgreSqlRollbacks.doTestRollbackCreateTableAsSelect;

public abstract class BasePostgresFailureRecoveryTest
        extends BaseJdbcFailureRecoveryTest
{
    private TestingPostgreSqlServer postgreSqlServer;

    public BasePostgresFailureRecoveryTest(RetryPolicy retryPolicy)
    {
        super(retryPolicy);
    }

    @Override
    protected QueryRunner createQueryRunner(
            List<TpchTable<?>> requiredTpchTables,
            Map<String, String> configProperties,
            Map<String, String> coordinatorProperties,
            Module failureInjectionModule)
            throws Exception
    {
        this.postgreSqlServer = new TestingPostgreSqlServer();
        DistributedQueryRunner queryRunner = PostgreSqlQueryRunner.builder(closeAfterClass(this.postgreSqlServer))
                .setExtraProperties(configProperties)
                .setCoordinatorProperties(configProperties)
                .withExchange("filesystem")
                .setAdditionalModule(failureInjectionModule)
                .setInitialTables(requiredTpchTables)
                .build();
        queryRunner.installPlugin(new BlackHolePlugin());
        queryRunner.createCatalog("blackhole", "blackhole", Map.of());
        return queryRunner;
    }

    @Test
    @Override
    protected void testUpdate()
    {
        // This simple update on JDBC ends up as a very simple, single-fragment, coordinator-only plan,
        // which has no ability to recover from errors. This test simply verifies that's still the case.
        Optional<String> setupQuery = Optional.of("CREATE TABLE <table> AS SELECT * FROM orders");
        String testQuery = "UPDATE <table> SET shippriority = 101 WHERE custkey = 1";
        Optional<String> cleanupQuery = Optional.of("DROP TABLE <table>");

        assertThatQuery(testQuery)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .isCoordinatorOnly();
    }

    /**
     * Similar to {@link TestPostgreSqlRollbacks#testRollbackCreateTableAsSelect()},
     * except this one runs in fault-tolerant mode, should this affect connector behavior.
     *
     * @see TestPostgreSqlRollbacks#testRollbackCreateTableAsSelect()
     */
    // TODO move this test to BaseJdbcFailureRecoveryTest
    @Test
    @Timeout(60)
    public void testRollbackCreateTableAsSelect()
            throws Exception
    {
        doTestRollbackCreateTableAsSelect(getQueryRunner());
    }

    @Override
    protected void addPrimaryKeyForMergeTarget(Session session, String tableName, String primaryKey)
    {
        postgreSqlServer.execute("ALTER TABLE %s ADD CONSTRAINT pk_%s PRIMARY KEY (%s)".formatted(tableName, tableName, primaryKey));
    }

    @Override
    protected boolean supportsMerge()
    {
        return true;
    }
}
