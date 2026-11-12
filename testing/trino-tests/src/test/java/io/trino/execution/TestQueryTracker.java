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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;

import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;

import static io.trino.SystemSessionProperties.QUERY_MAX_INITIAL_PLAN_NODES;
import static io.trino.SystemSessionProperties.QUERY_MAX_PLANNING_TIME;
import static io.trino.spi.StandardErrorCode.QUERY_PLAN_TOO_LARGE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

// Tests need to finish before strict timeouts. Any background work
// may make them flaky
@TestInstance(PER_CLASS)
@Execution(SAME_THREAD) // CountDownLatches are shared mutable state
public class TestQueryTracker
        extends AbstractTestQueryFramework
{
    private final CountDownLatch freeze = new CountDownLatch(1);
    private final CountDownLatch interrupted = new CountDownLatch(1);

    @AfterAll
    public void unfreeze()
    {
        freeze.countDown();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("mock")
                .setSchema("default")
                .setSystemProperty(QUERY_MAX_PLANNING_TIME, "2s")
                .build();

        QueryRunner queryRunner = DistributedQueryRunner
                .builder(defaultSession)
                .build();
        queryRunner.installPlugin(new Plugin()
        {
            @Override
            public Iterable<ConnectorFactory> getConnectorFactories()
            {
                return ImmutableList.of(MockConnectorFactory.builder()
                        .withGetColumns(_ -> ImmutableList.of(new ColumnMetadata("col", VARCHAR)))
                        // Apply filter happens inside optimizer so this should model most blocking tasks in planning phase
                        .withApplyFilter((_, _, _) -> freeze())
                        .build());
            }
        });
        queryRunner.createCatalog("mock", "mock");

        return queryRunner;
    }

    @Test
    @Timeout(10)
    public void testInterruptApplyFilter()
            throws InterruptedException
    {
        assertThatThrownBy(() -> getQueryRunner().execute("SELECT * FROM t1 WHERE col = 'abc'"))
                .hasMessageContaining("Query exceeded the maximum planning time limit of 2.00s");

        interrupted.await();
    }

    @Test
    @Timeout(30)
    public void testInitialPlanSizeLimit()
    {
        Session defaultSession = Session.builder(getSession())
                .setSystemProperty(QUERY_MAX_PLANNING_TIME, "30s")
                .build();
        // 2^14 copies of the innermost CTE exceed the default limit; failing on size happens long before the 2s planning time limit
        assertTrinoExceptionThrownBy(() -> getQueryRunner().execute(defaultSession, doublingCteQuery(14)))
                .hasErrorCode(QUERY_PLAN_TOO_LARGE)
                .hasMessage(
                        """
                        Number of nodes in the initial query plan exceeds the allowed maximum (100000).
                        The query likely references the same views or WITH queries many times.
                        Simplify the query or increase the query_max_initial_plan_nodes session property
                        """);

        Session session = Session.builder(defaultSession)
                .setSystemProperty(QUERY_MAX_INITIAL_PLAN_NODES, "100")
                .build();
        assertTrinoExceptionThrownBy(() -> getQueryRunner().execute(session, doublingCteQuery(6)))
                .hasErrorCode(QUERY_PLAN_TOO_LARGE);
        assertThat(getQueryRunner().execute(doublingCteQuery(6)).getOnlyValue()).isEqualTo(64L);

        // Nodes added after the last nested relation is planned count as well
        Session singleNodeSession = Session.builder(defaultSession)
                .setSystemProperty(QUERY_MAX_INITIAL_PLAN_NODES, "1")
                .build();
        assertTrinoExceptionThrownBy(() -> getQueryRunner().execute(singleNodeSession, "SELECT 1"))
                .hasErrorCode(QUERY_PLAN_TOO_LARGE);
    }

    // Each WITH query references the previous one twice, so the plan doubles per level
    private static String doublingCteQuery(int levels)
    {
        return "WITH t0 AS (SELECT 1 a)" +
                IntStream.rangeClosed(1, levels)
                        .mapToObj(i -> ", t%s AS (SELECT * FROM t%s UNION ALL SELECT * FROM t%s)".formatted(i, i - 1, i - 1))
                        .collect(joining()) +
                " SELECT count(*) FROM t" + levels;
    }

    private <T> T freeze()
    {
        try {
            freeze.await();
        }
        catch (InterruptedException e) {
            interrupted.countDown();
            throw new RuntimeException(e);
        }

        return null;
    }
}
