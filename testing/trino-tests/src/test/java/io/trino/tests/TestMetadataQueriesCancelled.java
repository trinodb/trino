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
package io.trino.tests;

import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.TestingH2JdbcClient;
import io.trino.plugin.jdbc.TestingH2JdbcModule;
import io.trino.spi.ErrorCode;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.plugin.jdbc.H2QueryRunner.createH2QueryRunner;
import static io.trino.spi.ErrorType.INTERNAL_ERROR;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.lang.String.format;
import static java.lang.Thread.sleep;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestMetadataQueriesCancelled
        extends AbstractTestQueryFramework
{
    private final Map<String, String> properties = TestingH2JdbcModule.createProperties();
    private final ExecutorService threadPool = newCachedThreadPool(daemonThreadsNamed("cancel-query-test"));
    private boolean test;

    @AfterClass(alwaysRun = true)
    public void afterClass()
    {
        assertThat(threadPool.shutdownNow()).isEmpty();
    }

    @BeforeMethod
    public void reset()
    {
        test = true;
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingH2JdbcModule module = new TestingH2JdbcModule((config, connectionFactory) -> new TestingH2JdbcClient(config, connectionFactory)
        {
            @Override
            public Map<String, Object> getTableProperties(ConnectorSession session, JdbcTableHandle tableHandle)
            {
                if (!test) {
                    return super.getTableProperties(session, tableHandle);
                }
                try {
                    sleep(10_000);
                }
                catch (InterruptedException e) {
                    // this is expected situation for this test, so test should not fail
                    return super.getTableProperties(session, tableHandle);
                }
                throw new TrinoException(() -> new ErrorCode(1, "Test exception", INTERNAL_ERROR),
                        "Thread with metadata query was not interrupted when query cancelled.");
            }
        });
        DistributedQueryRunner queryRunner = createH2QueryRunner(ImmutableList.copyOf(TpchTable.getTables()), properties, module);
        queryRunner.execute("CREATE TABLE copy_of_nation AS SELECT * FROM nation");
        return queryRunner;
    }

    @Test
    public void testOverrideTestMethodIsImplementedCorrectly()
    {
        assertQueryFails("SELECT nationkey FROM copy_of_nation", "Thread with metadata query was not interrupted when query cancelled.");
    }

    @Test
    public void testMetadataQueryWasCancelled()
            throws InterruptedException, ExecutionException
    {
        Future<?> future = threadPool.submit(() -> assertQueryFails("SELECT nationkey FROM copy_of_nation", "Query killed. Message: Killed by test"));
//        sleep(5_000); // to make sure datasource is queried
        QueryId queryId = getQueryId("SELECT nationkey");
        assertUpdate(format("CALL system.runtime.kill_query(query_id => '%s', message => '%s')", queryId, "Killed by test"));
        future.get();
    }

    private QueryId getQueryId(String sqlPattern)
    {
        AtomicReference<QueryId> queryId = new AtomicReference<>();
        assertEventually(Duration.valueOf("10s"), () -> {
            MaterializedResult queriesResult = getQueryRunner().execute(format(
                    ""
                            + "SELECT query_id "
                            + "FROM system.runtime.queries "
                            + "WHERE state = 'WAITING_FOR_RESOURCES' "
                            + "AND query LIKE '%%%s%%' "
                            + "AND query NOT LIKE '%%system.runtime.queries%%'",
                    sqlPattern));
            assertThat(queriesResult.getRowCount()).isEqualTo(1);

            queryId.set(new QueryId((String) queriesResult.getOnlyValue()));
        });
        return queryId.get();
    }
}