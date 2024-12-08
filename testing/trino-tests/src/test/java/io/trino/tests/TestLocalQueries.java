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
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.Plugin;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.EventListenerFactory;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.testing.AbstractTestQueries;
import io.trino.testing.QueryFailedException;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import io.trino.testing.TestingDirectTrinoClient;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static io.trino.SystemSessionProperties.PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN;
import static io.trino.plugin.tpch.TpchConnectorFactory.TPCH_SPLITS_PER_NODE;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestLocalQueries
        extends AbstractTestQueries
{
    private final Set<String> queryCompletedQueryIds = Collections.newSetFromMap(new ConcurrentHashMap<>());

    @Override
    protected QueryRunner createQueryRunner()
    {
        QueryRunner queryRunner = createTestQueryRunner();

        EventListener listener = new EventListener()
        {
            @Override
            public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
            {
                queryCompletedQueryIds.add(queryCompletedEvent.getMetadata().getQueryId());
            }
        };
        queryRunner.installPlugin(new Plugin()
        {
            @Override
            public Iterable<EventListenerFactory> getEventListenerFactories()
            {
                return ImmutableList.of(new EventListenerFactory()
                {
                    @Override
                    public String getName()
                    {
                        return "TestLocalQueries";
                    }

                    @Override
                    public EventListener create(Map<String, String> config)
                    {
                        return listener;
                    }
                });
            }
        });

        return queryRunner;
    }

    public static QueryRunner createTestQueryRunner()
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("local")
                .setSchema(TINY_SCHEMA_NAME)
                .setSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN, "true")
                .build();

        QueryRunner queryRunner = new StandaloneQueryRunner(defaultSession);
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog(defaultSession.getCatalog().get(), "tpch", ImmutableMap.of(TPCH_SPLITS_PER_NODE, "1"));
        queryRunner.installPlugin(new MemoryPlugin());
        queryRunner.createCatalog("memory", "memory");

        return queryRunner;
    }

    @Test
    public void testQueryCompletedEvent()
    {
        ImmutableList.Builder<String> queryIds = ImmutableList.builder();

        // plain DDL:
        queryIds.add(assertUnmaterializedQuerySucceeds("CREATE SCHEMA memory.test_schema").queryId().getId());
        // DDL with a DML component:
        queryIds.add(assertUnmaterializedQuerySucceeds("CREATE TABLE memory.test_schema.test_table (c) AS VALUES 1").queryId().getId());
        // plain DML:
        queryIds.add(assertUnmaterializedQuerySucceeds("SELECT count(*) FROM memory.test_schema.test_table").queryId().getId());
        // more DDL as part of cleanup:
        queryIds.add(assertUnmaterializedQuerySucceeds("DROP TABLE memory.test_schema.test_table").queryId().getId());
        queryIds.add(assertUnmaterializedQuerySucceeds("DROP SCHEMA memory.test_schema").queryId().getId());

        assertEventually(new Duration(5, SECONDS), () -> assertThat(queryCompletedQueryIds).containsAll(queryIds.build()));
    }

    @Test
    public void testShowColumnStats()
    {
        // FIXME Add tests for more complex scenario with more stats
        assertThat(query("SHOW STATS FOR nation"))
                .result().matches(resultBuilder(getSession(), VARCHAR, DOUBLE, DOUBLE, DOUBLE, DOUBLE, VARCHAR, VARCHAR)
                        .row("nationkey", null, 25.0, 0.0, null, "0", "24")
                        .row("name", 177.0, 25.0, 0.0, null, null, null)
                        .row("regionkey", null, 5.0, 0.0, null, "0", "4")
                        .row("comment", 1857.0, 25.0, 0.0, null, null, null)
                        .row(null, null, null, null, 25.0, null, null)
                        .build());
    }

    @Test
    public void testRejectStarQueryWithoutFromRelation()
    {
        assertQueryFails("SELECT *", "line \\S+ SELECT \\* not allowed in queries without FROM clause");
        assertQueryFails("SELECT 1, '2', *", "line \\S+ SELECT \\* not allowed in queries without FROM clause");
    }

    @Test
    public void testDecimal()
    {
        assertQuery("SELECT DECIMAL '1.0'", "SELECT CAST('1.0' AS DECIMAL(10, 1))");
        assertQuery("SELECT DECIMAL '1.'", "SELECT CAST('1.0' AS DECIMAL(10, 1))");
        assertQuery("SELECT DECIMAL '0.1'", "SELECT CAST('0.1' AS DECIMAL(10, 1))");
        assertQuery("SELECT 1.0");
        assertQuery("SELECT 1.");
        assertQuery("SELECT 0.1");
    }

    @Test
    public void testHueQueries()
    {
        // https://github.com/cloudera/hue/blob/b49e98c1250c502be596667ce1f0fe118983b432/desktop/libs/notebook/src/notebook/connectors/jdbc.py#L205
        assertQuerySucceeds(getSession(), "SELECT table_name, table_comment FROM information_schema.tables WHERE table_schema='nation'");

        // https://github.com/cloudera/hue/blob/b49e98c1250c502be596667ce1f0fe118983b432/desktop/libs/notebook/src/notebook/connectors/jdbc.py#L213
        assertQuerySucceeds(getSession(), "SELECT column_name, data_type, column_comment FROM information_schema.columns WHERE table_schema='local' AND TABLE_NAME='nation'");
    }

    @Test
    public void testTransformValuesInTry()
    {
        // Test resetting of transform_values internal state after recovery from try()
        assertQuery(
                "SELECT json_format(CAST(try(transform_values(m, (k, v) -> k / v)) AS json)) " +
                        "FROM (VALUES map(ARRAY[1, 2], ARRAY[0, 0]),  map(ARRAY[28], ARRAY[2]), map(ARRAY[18], ARRAY[2]), map(ARRAY[4, 5], ARRAY[1, 0]),  map(ARRAY[12], ARRAY[3])) AS t(m)",
                "VALUES NULL, '{\"28\":14}', '{\"18\":9}', NULL, '{\"12\":4}'");
    }

    private TestingDirectTrinoClient.Result assertUnmaterializedQuerySucceeds(@Language("SQL") String sql)
    {
        try {
            return ((StandaloneQueryRunner) getQueryRunner()).executeUnmaterialized(getSession(), sql);
        }
        catch (QueryFailedException e) {
            fail(format("Expected query %s to succeed: %s", e.getQueryId(), sql), e);
        }
        catch (RuntimeException e) {
            fail(format("Expected query to succeed: %s", sql), e);
        }
        fail(format("Expected query to succeed: %s", sql));
        throw new IllegalStateException();
    }
}
