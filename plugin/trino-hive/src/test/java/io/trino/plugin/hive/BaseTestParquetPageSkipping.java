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
package io.trino.plugin.hive;

import io.trino.Session;
import io.trino.execution.QueryStats;
import io.trino.operator.OperatorStats;
import io.trino.spi.QueryId;
import io.trino.spi.metrics.Count;
import io.trino.spi.metrics.Metric;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.parquet.reader.ParquetReader.COLUMN_INDEX_ROWS_FILTERED;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Page skipping tests over Parquet files with column indexes, shared by connectors.
 * Subclasses create a table backed by a data file from the {@code parquet_page_skipping} test resources.
 */
public abstract class BaseTestParquetPageSkipping
        extends AbstractTestQueryFramework
{
    /**
     * Creates a table over the given resource file and returns its name.
     */
    protected abstract String createTableWithDataFile(String tableNamePrefix, String columnsDefinition, String resourceFileName)
            throws Exception;

    /**
     * Connector type for a Parquet INT96 timestamp column.
     */
    protected abstract String timestampMillisType();

    @Test
    public void testRowGroupPruningFromPageIndexes()
            throws Exception
    {
        String tableName = createTableWithDataFile(
                "test_row_group_pruning",
                """
                (
                   orderkey bigint,
                   custkey bigint,
                   orderstatus varchar,
                   totalprice double,
                   orderdate date,
                   orderpriority varchar,
                   clerk varchar,
                   shippriority integer,
                   comment varchar,
                   rvalues array(double))
                """,
                "parquet_page_skipping/orders_sorted_by_totalprice/data.parquet");

        int rowCount = assertColumnIndexResults("SELECT * FROM " + tableName + " WHERE totalprice BETWEEN 100000 AND 131280 AND clerk = 'Clerk#000000624'");
        assertThat(rowCount).isGreaterThan(0);

        // `totalprice BETWEEN 51890 AND 51900` is chosen to lie between min/max values of row group
        // but outside page level min/max boundaries to trigger pruning of row group using column index
        assertRowGroupPruning("SELECT * FROM " + tableName + " WHERE totalprice BETWEEN 51890 AND 51900 AND orderkey > 0");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testPageSkippingWithNonSequentialOffsets()
            throws Exception
    {
        String tableName = createTableWithDataFile("test_random", "(col double)", "parquet_page_skipping/random/data.parquet");
        // These queries select a subset of pages which are stored at non-sequential offsets
        // This reproduces the issue identified in https://github.com/trinodb/trino/issues/9097
        for (double i = 0; i < 1; i += 0.1) {
            assertColumnIndexResults(format("SELECT * FROM %s WHERE col BETWEEN %f AND %f", tableName, i - 0.00001, i + 0.00001));
        }
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testFilteringOnColumnNameWithDot()
            throws Exception
    {
        String nameInSql = "\"a.dot\"";
        String tableName = createTableWithDataFile(
                "test_column_name_with_dot",
                format("(key varchar, %s varchar)", nameInSql),
                "parquet_page_skipping/column_name_with_dot/data.parquet");

        assertQuery("SELECT key FROM " + tableName + " WHERE " + nameInSql + " IS NULL", "VALUES ('null value')");
        assertQuery("SELECT key FROM " + tableName + " WHERE " + nameInSql + " = 'abc'", "VALUES ('sample value')");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testUnsupportedColumnIndex()
            throws Exception
    {
        // Test for https://github.com/trinodb/trino/issues/16801
        String tableName = createTableWithDataFile(
                "test_unsupported_column_index",
                format("(stime %1$s, btime %1$s, detail varchar)", timestampMillisType()),
                "parquet_page_skipping/unsupported_column_index/data.parquet");

        assertQuery(
                "SELECT detail, stime IS NULL, btime IS NULL FROM " + tableName + " WHERE btime >= timestamp '2023-03-27 13:30:00'",
                "VALUES ('record_1', false, false)");

        assertQuery(
                "SELECT detail, stime IS NULL, btime IS NULL FROM " + tableName + " WHERE detail = 'record_2'",
                "VALUES ('record_2', false, true)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testFilteringWithColumnIndex()
            throws Exception
    {
        String tableName = createTableWithDataFile(
                "test_page_filtering",
                "(suppkey bigint, extendedprice decimal(12, 2), shipmode varchar, comment varchar)",
                "parquet_page_skipping/lineitem_sorted_by_suppkey/data.parquet");

        verifyFilteringWithColumnIndex("SELECT * FROM " + tableName + " WHERE suppkey = 10");
        verifyFilteringWithColumnIndex("SELECT * FROM " + tableName + " WHERE suppkey BETWEEN 25 AND 35");
        verifyFilteringWithColumnIndex("SELECT * FROM " + tableName + " WHERE suppkey >= 60");
        verifyFilteringWithColumnIndex("SELECT * FROM " + tableName + " WHERE suppkey <= 40");
        verifyFilteringWithColumnIndex("SELECT * FROM " + tableName + " WHERE suppkey IN (25, 35, 50, 80)");

        assertUpdate("DROP TABLE " + tableName);
    }

    protected void verifyFilteringWithColumnIndex(@Language("SQL") String query)
    {
        QueryRunner queryRunner = getDistributedQueryRunner();
        MaterializedResultWithPlan resultWithoutColumnIndex = queryRunner.executeWithPlan(
                noParquetColumnIndexFiltering(getSession()),
                query);
        QueryStats queryStatsWithoutColumnIndex = getQueryStats(resultWithoutColumnIndex.queryId());
        assertThat(queryStatsWithoutColumnIndex.getPhysicalInputPositions()).isGreaterThan(0);
        Map<String, Metric<?>> metricsWithoutColumnIndex = getScanOperatorStats(resultWithoutColumnIndex.queryId())
                .getConnectorMetrics()
                .getMetrics();
        assertThat(metricsWithoutColumnIndex).doesNotContainKey(COLUMN_INDEX_ROWS_FILTERED);

        MaterializedResultWithPlan resultWithColumnIndex = queryRunner.executeWithPlan(getSession(), query);
        QueryStats queryStatsWithColumnIndex = getQueryStats(resultWithColumnIndex.queryId());
        assertThat(queryStatsWithColumnIndex.getPhysicalInputPositions()).isGreaterThan(0);
        assertThat(queryStatsWithColumnIndex.getPhysicalInputPositions())
                .isLessThan(queryStatsWithoutColumnIndex.getPhysicalInputPositions());
        Map<String, Metric<?>> metricsWithColumnIndex = getScanOperatorStats(resultWithColumnIndex.queryId())
                .getConnectorMetrics()
                .getMetrics();
        assertThat(metricsWithColumnIndex).containsKey(COLUMN_INDEX_ROWS_FILTERED);
        assertThat(((Count<?>) metricsWithColumnIndex.get(COLUMN_INDEX_ROWS_FILTERED)).getTotal())
                .isGreaterThan(0);

        assertEqualsIgnoreOrder(resultWithColumnIndex.result(), resultWithoutColumnIndex.result());
    }

    protected int assertColumnIndexResults(String query)
    {
        MaterializedResult withColumnIndexing = computeActual(query);
        MaterializedResult withoutColumnIndexing = computeActual(noParquetColumnIndexFiltering(getSession()), query);
        assertEqualsIgnoreOrder(withColumnIndexing, withoutColumnIndexing);
        return withoutColumnIndexing.getRowCount();
    }

    protected void assertRowGroupPruning(@Language("SQL") String sql)
    {
        assertQueryStats(
                noParquetColumnIndexFiltering(getSession()),
                sql,
                queryStats -> {
                    assertThat(queryStats.getPhysicalInputPositions()).isGreaterThan(0);
                    assertThat(queryStats.getProcessedInputPositions()).isEqualTo(queryStats.getPhysicalInputPositions());
                },
                results -> assertThat(results.getRowCount()).isEqualTo(0));

        assertQueryStats(
                getSession(),
                sql,
                queryStats -> {
                    assertThat(queryStats.getPhysicalInputPositions()).isEqualTo(0);
                    assertThat(queryStats.getProcessedInputPositions()).isEqualTo(0);
                },
                results -> assertThat(results.getRowCount()).isEqualTo(0));
    }

    protected Session noParquetColumnIndexFiltering(Session session)
    {
        return Session.builder(session)
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "parquet_use_column_index", "false")
                .build();
    }

    protected static String tableName(String tableNamePrefix)
    {
        return tableNamePrefix + "_" + randomNameSuffix();
    }

    private QueryStats getQueryStats(QueryId queryId)
    {
        return getDistributedQueryRunner().getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(queryId)
                .getQueryStats();
    }

    private OperatorStats getScanOperatorStats(QueryId queryId)
    {
        return getQueryStats(queryId)
                .getOperatorSummaries()
                .stream()
                .filter(summary -> summary.getOperatorType().startsWith("TableScan") || summary.getOperatorType().startsWith("Scan"))
                .collect(onlyElement());
    }
}
