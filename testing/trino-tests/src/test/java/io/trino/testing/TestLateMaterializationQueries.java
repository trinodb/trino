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
package io.trino.testing;

import io.trino.Session;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.spi.QueryId;
import io.trino.spi.metrics.Count;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.Objects;

import static io.trino.operator.TopNProcessor.SKIPPED_DECODE_POSITIONS;
import static org.assertj.core.api.Assertions.assertThat;

final class TestLateMaterializationQueries
        extends AbstractTestQueryFramework
{
    private Session maskedPagesDisabled;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner runner = HiveQueryRunner.builder().build();
        // masked page hand-off narrows a real Parquet source page, so read from a Parquet-backed table
        runner.execute("CREATE TABLE orders WITH (format = 'PARQUET') AS SELECT * FROM tpch.tiny.orders");
        // masked page hand-off is on by default; compare against it explicitly disabled
        maskedPagesDisabled = Session.builder(runner.getDefaultSession())
                .setSystemProperty("masked_page_enabled", "false")
                .build();
        return runner;
    }

    @Test
    void testIdentityProjection()
    {
        assertDecodeSkipped("SELECT * FROM orders WHERE orderkey % 7 = 0 ORDER BY orderkey LIMIT 10");
    }

    @Test
    void testProjectionWithoutFilter()
    {
        // computed projection without a filter still hands masked pages to the partial TopN
        assertDecodeSkipped("SELECT orderkey + 1, comment FROM orders ORDER BY totalprice, orderkey LIMIT 10");
    }

    @Test
    void testPrunedAndReorderedChannels()
    {
        assertDecodeSkipped("SELECT comment, orderkey FROM orders WHERE orderkey % 7 = 0 ORDER BY totalprice, orderkey LIMIT 10");
    }

    @Test
    void testComputedSortChannel()
    {
        assertDecodeSkipped("SELECT orderkey, comment FROM orders WHERE orderkey % 7 = 0 ORDER BY totalprice / 2, orderkey LIMIT 10");
    }

    @Test
    void testComputedPayloadChannels()
    {
        assertDecodeSkipped("SELECT orderkey + 1, upper(comment) FROM orders WHERE orderkey % 7 = 0 ORDER BY totalprice, orderkey LIMIT 10");
    }

    @Test
    void testAllSortChannels()
    {
        // every output column is a sort channel: nothing to skip, so the masked path is not taken
        assertNoDecodeSkipped("SELECT orderkey, totalprice FROM orders WHERE orderkey % 7 = 0 ORDER BY orderkey, totalprice LIMIT 10");
    }

    @Test
    void testEmptyResult()
    {
        // no row survives the filter, so no position is ever skipped
        assertNoDecodeSkipped("SELECT * FROM orders WHERE orderkey < 0 ORDER BY orderkey LIMIT 10");
    }

    private void assertDecodeSkipped(@Language("SQL") String sql)
    {
        assertThat(skippedDecodePositions(sql)).isPositive();
    }

    private void assertNoDecodeSkipped(@Language("SQL") String sql)
    {
        assertThat(skippedDecodePositions(sql)).isZero();
    }

    private long skippedDecodePositions(@Language("SQL") String sql)
    {
        MaterializedResultWithPlan masked = getDistributedQueryRunner().executeWithPlan(getSession(), sql);
        MaterializedResult unmasked = getDistributedQueryRunner().execute(maskedPagesDisabled, sql);
        // every query is ordered, so compare row order too: masked TopN must not reorder survivors
        assertThat(masked.result().getMaterializedRows())
                .as("For query: %s", sql)
                .containsExactlyElementsOf(unmasked.getMaterializedRows());

        // only the partial TopN fed by the masked source emits the metric, so summing over all TopN operators is exact
        return topNSkippedDecodePositions(masked.queryId());
    }

    private long topNSkippedDecodePositions(QueryId queryId)
    {
        return getDistributedQueryRunner().getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(queryId)
                .getQueryStats()
                .getOperatorSummaries()
                .stream()
                .filter(summary -> summary.getOperatorType().equals("TopNOperator"))
                .map(summary -> summary.getMetrics().getMetrics().get(SKIPPED_DECODE_POSITIONS))
                .filter(Objects::nonNull)
                .mapToLong(metric -> ((Count<?>) metric).getTotal())
                .sum();
    }
}
