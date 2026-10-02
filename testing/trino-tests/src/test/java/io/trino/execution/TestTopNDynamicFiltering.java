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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.operator.OperatorStats;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.QueryId;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.DYNAMIC_ROW_FILTERING_SELECTIVITY_THRESHOLD;
import static io.trino.SystemSessionProperties.ENABLE_TOP_N_DYNAMIC_FILTERING;
import static io.trino.SystemSessionProperties.MAX_DRIVERS_PER_TASK;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTopNDynamicFiltering
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .setSystemProperty(DYNAMIC_ROW_FILTERING_SELECTIVITY_THRESHOLD, "1")
                .build();
        QueryRunner queryRunner = DistributedQueryRunner.builder(session).build();
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of("tpch.splits-per-node", "16"));
        queryRunner.installPlugin(new MemoryPlugin());
        queryRunner.createCatalog("memory", "memory", ImmutableMap.of("memory.splits-per-node", "16"));
        queryRunner.execute("CREATE TABLE memory.default.nullable_keys AS SELECT nullif(orderkey % 7, 0) key, orderkey FROM orders");
        queryRunner.execute("CREATE TABLE memory.default.typed_keys (char_key char(15), double_key double, array_key array(double), row_key row(value double), orderkey bigint)");
        // insert in parts, so that the table has many pages and splits which can be filtered by the bound of earlier splits
        for (int part = 0; part < 16; part++) {
            queryRunner.execute(
                    """
                    INSERT INTO memory.default.typed_keys
                    SELECT
                        CAST(orderpriority AS char(15)),
                        if(orderkey %% 5 = 0, nan(), CAST(orderkey AS double)),
                        ARRAY[if(orderkey %% 5 = 0, nan(), CAST(orderkey AS double))],
                        ROW(if(orderkey %% 5 = 0, nan(), CAST(orderkey AS double))),
                        orderkey
                    FROM orders
                    WHERE orderkey %% 16 = %s
                    """.formatted(part));
        }
        return queryRunner;
    }

    @Test
    public void testRowsAreFiltered()
    {
        Session session = sequentialSplits();
        @Language("SQL") String sql = "SELECT orderkey, linenumber, partkey FROM lineitem ORDER BY orderkey, linenumber LIMIT 10";

        QueryRunner.MaterializedResultWithPlan filtered = getDistributedQueryRunner().executeWithPlan(session, sql);
        QueryRunner.MaterializedResultWithPlan unfiltered = getDistributedQueryRunner().executeWithPlan(withoutTopNDynamicFiltering(session), sql);
        assertThat(filtered.result().getMaterializedRows()).isEqualTo(unfiltered.result().getMaterializedRows());

        ScanPositions filteredScan = scanPositions(filtered.queryId());
        ScanPositions unfilteredScan = scanPositions(unfiltered.queryId());
        assertThat(filteredScan.input()).isEqualTo(unfilteredScan.input());
        assertThat(unfilteredScan.output()).isEqualTo(unfilteredScan.input());
        assertThat(filteredScan.output()).isLessThan(unfilteredScan.output() / 2);
    }

    @Test
    public void testResults()
    {
        // multiple sort keys, rows tied on the first key must be kept
        assertSameResults("SELECT orderkey, linenumber FROM lineitem ORDER BY orderkey DESC, linenumber DESC LIMIT 7");
        assertSameResults("SELECT orderdate, orderkey FROM orders ORDER BY orderdate DESC, orderkey LIMIT 20");
        // table scan without projection
        assertSameResults("SELECT * FROM orders ORDER BY orderkey LIMIT 5");
        // filter between the table scan and the TopN
        assertSameResults("SELECT custkey, orderkey FROM orders WHERE orderstatus = 'F' ORDER BY orderkey DESC LIMIT 10");
        // projection between the table scan and the TopN
        assertSameResults("SELECT orderkey, totalprice * 2 FROM orders ORDER BY orderkey DESC LIMIT 10");
        // varchar sort key
        assertSameResults("SELECT name FROM customer ORDER BY name DESC LIMIT 3");
        // single sort key with many ties, rows tied with the bound are not needed
        assertSameResults("SELECT count(*), count(DISTINCT shipmode) FROM (SELECT shipmode FROM lineitem ORDER BY shipmode LIMIT 10000)");
        assertSameResults("SELECT count(*), max(shipdate) FROM (SELECT shipdate FROM lineitem ORDER BY shipdate DESC LIMIT 5000)");
        // sort key computed by a projection
        assertSameResults("SELECT orderkey % 1000 k, orderkey FROM orders ORDER BY k DESC, orderkey LIMIT 10");
        // unsupported sort key type
        assertSameResults("SELECT totalprice FROM orders ORDER BY totalprice DESC LIMIT 10");
    }

    @Test
    public void testNulls()
    {
        for (String ordering : new String[] {"ASC NULLS FIRST", "ASC NULLS LAST", "DESC NULLS FIRST", "DESC NULLS LAST"}) {
            assertSameResults("SELECT key, orderkey FROM memory.default.nullable_keys ORDER BY key %s, orderkey LIMIT 10".formatted(ordering));
            assertSameResults("SELECT count(*), count(key) FROM (SELECT key FROM memory.default.nullable_keys ORDER BY key %s LIMIT 3000)".formatted(ordering));
        }
    }

    @Test
    public void testOrderableType()
    {
        Session session = sequentialSplits();
        for (String ordering : new String[] {"ASC", "DESC"}) {
            assertSameResults(session, "SELECT char_key, orderkey FROM memory.default.typed_keys ORDER BY char_key %s, orderkey LIMIT 10".formatted(ordering));
            assertSameResults(session, "SELECT count(*), count(DISTINCT char_key) FROM (SELECT char_key FROM memory.default.typed_keys ORDER BY char_key %s LIMIT 3000)".formatted(ordering));
        }
    }

    @Test
    public void testOrderableTypeContainingNaN()
    {
        Session session = sequentialSplits();
        for (String key : new String[] {"double_key", "array_key", "row_key"}) {
            for (String ordering : new String[] {"ASC NULLS FIRST", "ASC NULLS LAST", "DESC NULLS FIRST", "DESC NULLS LAST"}) {
                assertSameResults(session, "SELECT %1$s, orderkey FROM memory.default.typed_keys ORDER BY %1$s %2$s, orderkey LIMIT 10".formatted(key, ordering));
                assertSameResults(session, "SELECT count(*), count_if(is_nan(double_key)) FROM (SELECT double_key FROM memory.default.typed_keys ORDER BY %1$s %2$s LIMIT 1000)".formatted(key, ordering));
            }
        }
    }

    private void assertSameResults(@Language("SQL") String sql)
    {
        assertSameResults(getSession(), sql);
    }

    private void assertSameResults(Session session, @Language("SQL") String sql)
    {
        MaterializedResult expected = computeActual(withoutTopNDynamicFiltering(session), sql);
        MaterializedResult actual = computeActual(session, sql);
        assertThat(actual.getMaterializedRows())
                .describedAs(sql)
                .isEqualTo(expected.getMaterializedRows());
    }

    private ScanPositions scanPositions(QueryId queryId)
    {
        List<OperatorStats> scans = getDistributedQueryRunner().getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(queryId)
                .getQueryStats()
                .getOperatorSummaries()
                .stream()
                .filter(summary -> summary.getOperatorType().equals("ScanFilterAndProjectOperator") || summary.getOperatorType().equals("TableScanOperator"))
                .collect(toImmutableList());
        assertThat(scans).isNotEmpty();
        return new ScanPositions(
                scans.stream().mapToLong(OperatorStats::getInputPositions).sum(),
                scans.stream().mapToLong(OperatorStats::getOutputPositions).sum());
    }

    private record ScanPositions(long input, long output) {}

    // run the splits of a task one at a time, so that later splits see the bound of earlier ones
    private Session sequentialSplits()
    {
        return Session.builder(getSession())
                .setSystemProperty(MAX_DRIVERS_PER_TASK, "1")
                .build();
    }

    private static Session withoutTopNDynamicFiltering(Session session)
    {
        return Session.builder(session)
                .setSystemProperty(ENABLE_TOP_N_DYNAMIC_FILTERING, "false")
                .build();
    }
}
