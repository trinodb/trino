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

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.trino.Session;
import io.trino.execution.QueryStats;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.operator.OperatorStats;
import io.trino.spi.QueryId;
import io.trino.spi.metrics.Count;
import io.trino.spi.metrics.Metric;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.util.Map;
import java.util.UUID;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.parquet.reader.ParquetReader.COLUMN_INDEX_ROWS_FILTERED;
import static io.trino.plugin.hive.TestingHiveUtils.getConnectorService;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestParquetPageSkipping
        extends AbstractTestQueryFramework
{
    private TrinoFileSystem fileSystem;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = HiveQueryRunner.builder()
                .setHiveProperties(
                        ImmutableMap.of(
                                "parquet.use-column-index", "true",
                                "parquet.max-buffer-size", "1MB"))
                .build();

        fileSystem = getConnectorService(queryRunner, TrinoFileSystemFactory.class)
                .create(ConnectorIdentity.ofUser("test"));

        return queryRunner;
    }

    @Test
    public void testRowGroupPruningFromPageIndexes()
            throws Exception
    {
        Location dataFile = copyInDataFile("parquet_page_skipping/orders_sorted_by_totalprice/data.parquet");

        String tableName = "test_row_group_pruning_" + randomNameSuffix();
        assertUpdate(
                """
                        CREATE TABLE %s (
                           orderkey bigint,
                           custkey bigint,
                           orderstatus varchar(1),
                           totalprice double,
                           orderdate date,
                           orderpriority varchar(15),
                           clerk varchar(15),
                           shippriority integer,
                           comment varchar(79),
                           rvalues double array)
                        WITH (
                           format = 'PARQUET',
                           external_location = '%s')
                        """.formatted(tableName, dataFile.parentDirectory()));

        int rowCount = assertColumnIndexResults("SELECT * FROM " + tableName + " WHERE totalprice BETWEEN 100000 AND 131280 AND clerk = 'Clerk#000000624'");
        assertThat(rowCount).isGreaterThan(0);

        // `totalprice BETWEEN 51890 AND 51900` is chosen to lie between min/max values of row group
        // but outside page level min/max boundaries to trigger pruning of row group using column index
        assertRowGroupPruning("SELECT * FROM " + tableName + " WHERE totalprice BETWEEN 51890 AND 51900 AND orderkey > 0");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testPageSkippingWithNonSequentialOffsets()
            throws IOException
    {
        Location dataFile = copyInDataFile("parquet_page_skipping/random/data.parquet");
        String tableName = "test_random_" + randomNameSuffix();
        assertUpdate(format(
                "CREATE TABLE %s (col double) WITH (format = 'PARQUET', external_location = '%s')",
                tableName,
                dataFile.parentDirectory()));
        // These queries select a subset of pages which are stored at non-sequential offsets
        // This reproduces the issue identified in https://github.com/trinodb/trino/issues/9097
        for (double i = 0; i < 1; i += 0.1) {
            assertColumnIndexResults(format("SELECT * FROM %s WHERE col BETWEEN %f AND %f", tableName, i - 0.00001, i + 0.00001));
        }
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testFilteringOnColumnNameWithDot()
            throws IOException
    {
        Location dataFile = copyInDataFile("parquet_page_skipping/column_name_with_dot/data.parquet");

        String nameInSql = "\"a.dot\"";
        String tableName = "test_column_name_with_dot_" + randomNameSuffix();

        assertUpdate(format(
                "CREATE TABLE %s (key varchar(50), %s varchar(50)) WITH (format = 'PARQUET', external_location = '%s')",
                tableName,
                nameInSql,
                dataFile.parentDirectory()));

        assertQuery("SELECT key FROM " + tableName + " WHERE " + nameInSql + " IS NULL", "VALUES ('null value')");
        assertQuery("SELECT key FROM " + tableName + " WHERE " + nameInSql + " = 'abc'", "VALUES ('sample value')");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testUnsupportedColumnIndex()
            throws IOException
    {
        String tableName = "test_unsupported_column_index_" + randomNameSuffix();

        // Test for https://github.com/trinodb/trino/issues/16801
        Location dataFile = copyInDataFile("parquet_page_skipping/unsupported_column_index/data.parquet");
        assertUpdate(format(
                "CREATE TABLE %s (stime timestamp(3), btime timestamp(3), detail varchar) WITH (format = 'PARQUET', external_location = '%s')",
                tableName,
                dataFile.parentDirectory()));

        assertQuery(
                "SELECT * FROM " + tableName + " WHERE btime >= timestamp '2023-03-27 13:30:00'",
                "VALUES ('2023-03-31 18:00:00.000', '2023-03-31 18:00:00.000', 'record_1')");

        assertQuery(
                "SELECT * FROM " + tableName + " WHERE detail = 'record_2'",
                "VALUES ('2023-03-31 18:00:00.000', null, 'record_2')");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testPageSkipping()
    {
        testPageSkipping("orderkey", "bigint", new Object[][] {{2, 7520, 7523, 14950}});
        testPageSkipping("totalprice", "double", new Object[][] {{974.04, 131094.34, 131279.97, 406938.36}});
        testPageSkipping("totalprice", "real", new Object[][] {{974.04, 131094.34, 131279.97, 406938.36}});
        testPageSkipping("totalprice", "decimal(12,2)", new Object[][] {
                {974.04, 131094.34, 131279.97, 406938.36},
                {973, 131095, 131280, 406950},
                {974.04123, 131094.34123, 131279.97012, 406938.36555}});
        testPageSkipping("totalprice", "decimal(12,0)", new Object[][] {
                {973, 131095, 131280, 406950}});
        testPageSkipping("totalprice", "decimal(35,2)", new Object[][] {
                {974.04, 131094.34, 131279.97, 406938.36},
                {973, 131095, 131280, 406950},
                {974.04123, 131094.34123, 131279.97012, 406938.36555}});
        testPageSkipping("orderdate", "date", new Object[][] {{"DATE '1992-01-05'", "DATE '1995-10-13'", "DATE '1995-10-13'", "DATE '1998-07-29'"}});
        testPageSkipping("orderdate", "timestamp", new Object[][] {{"TIMESTAMP '1992-01-05'", "TIMESTAMP '1995-10-13'", "TIMESTAMP '1995-10-14'", "TIMESTAMP '1998-07-29'"}});
        testPageSkipping("clerk", "varchar(15)", new Object[][] {{"'Clerk#000000006'", "'Clerk#000000508'", "'Clerk#000000513'", "'Clerk#000000996'"}});
        testPageSkipping("custkey", "integer", new Object[][] {{4, 634, 640, 1493}});
        testPageSkipping("custkey", "smallint", new Object[][] {{4, 634, 640, 1493}});
    }

    private void testPageSkipping(String sortByColumn, String sortByColumnType, Object[][] valuesArray)
    {
        String tableName = "test_page_skipping_" + randomNameSuffix();
        buildSortedTables(tableName, sortByColumn, sortByColumnType);
        for (Object[] values : valuesArray) {
            Object lowValue = values[0];
            Object middleLowValue = values[1];
            Object middleHighValue = values[2];
            Object highValue = values[3];
            assertColumnIndexResults(format("SELECT %s FROM %s WHERE %s = %s", sortByColumn, tableName, sortByColumn, middleLowValue));
            assertThat(assertColumnIndexResults(format("SELECT %s FROM %s WHERE %s < %s", sortByColumn, tableName, sortByColumn, lowValue))).isGreaterThan(0);
            assertThat(assertColumnIndexResults(format("SELECT %s FROM %s WHERE %s > %s", sortByColumn, tableName, sortByColumn, highValue))).isGreaterThan(0);
            assertThat(assertColumnIndexResults(format("SELECT %s FROM %s WHERE %s BETWEEN %s AND %s", sortByColumn, tableName, sortByColumn, middleLowValue, middleHighValue))).isGreaterThan(0);
            // Tests synchronization of reading values across columns
            assertColumnIndexResults(format("SELECT * FROM %s WHERE %s = %s", tableName, sortByColumn, middleLowValue));
            assertThat(assertColumnIndexResults(format("SELECT * FROM %s WHERE %s < %s", tableName, sortByColumn, lowValue))).isGreaterThan(0);
            assertThat(assertColumnIndexResults(format("SELECT * FROM %s WHERE %s > %s", tableName, sortByColumn, highValue))).isGreaterThan(0);
            assertThat(assertColumnIndexResults(format("SELECT * FROM %s WHERE %s BETWEEN %s AND %s", tableName, sortByColumn, middleLowValue, middleHighValue))).isGreaterThan(0);
            // Nested data
            assertColumnIndexResults(format("SELECT rvalues FROM %s WHERE %s IN (%s, %s, %s, %s)", tableName, sortByColumn, lowValue, middleLowValue, middleHighValue, highValue));
            // Without nested data
            assertColumnIndexResults(format("SELECT orderkey, orderdate FROM %s WHERE %s IN (%s, %s, %s, %s)", tableName, sortByColumn, lowValue, middleLowValue, middleHighValue, highValue));
        }
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testFilteringWithColumnIndex()
            throws IOException
    {
        Location dataFile = copyInDataFile("parquet_page_skipping/lineitem_sorted_by_suppkey/data.parquet");
        String tableName = "test_page_filtering_" + randomNameSuffix();
        assertUpdate(format(
                "CREATE TABLE %s (suppkey bigint, extendedprice decimal(12, 2), shipmode varchar(10), comment varchar(44)) " +
                        "WITH (format = 'PARQUET', external_location = '%s')",
                tableName,
                dataFile.parentDirectory()));

        verifyFilteringWithColumnIndex("SELECT * FROM " + tableName + " WHERE suppkey = 10");
        verifyFilteringWithColumnIndex("SELECT * FROM " + tableName + " WHERE suppkey BETWEEN 25 AND 35");
        verifyFilteringWithColumnIndex("SELECT * FROM " + tableName + " WHERE suppkey >= 60");
        verifyFilteringWithColumnIndex("SELECT * FROM " + tableName + " WHERE suppkey <= 40");
        verifyFilteringWithColumnIndex("SELECT * FROM " + tableName + " WHERE suppkey IN (25, 35, 50, 80)");

        assertUpdate("DROP TABLE " + tableName);
    }

    private void verifyFilteringWithColumnIndex(@Language("SQL") String query)
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

    private int assertColumnIndexResults(String query)
    {
        MaterializedResult withColumnIndexing = computeActual(query);
        MaterializedResult withoutColumnIndexing = computeActual(noParquetColumnIndexFiltering(getSession()), query);
        assertEqualsIgnoreOrder(withColumnIndexing, withoutColumnIndexing);
        return withoutColumnIndexing.getRowCount();
    }

    private void assertRowGroupPruning(@Language("SQL") String sql)
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

    private Session noParquetColumnIndexFiltering(Session session)
    {
        return Session.builder(session)
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "parquet_use_column_index", "false")
                .build();
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

    private void buildSortedTables(String tableName, String sortByColumnName, String sortByColumnType)
    {
        String createTableTemplate =
                "CREATE TABLE %s ( " +
                        "   orderkey bigint, " +
                        "   custkey bigint, " +
                        "   orderstatus varchar(1), " +
                        "   totalprice double, " +
                        "   orderdate date, " +
                        "   orderpriority varchar(15), " +
                        "   clerk varchar(15), " +
                        "   shippriority integer, " +
                        "   comment varchar(79), " +
                        "   rvalues double array " +
                        ") " +
                        "WITH ( " +
                        "   format = 'PARQUET', " +
                        "   bucketed_by = array['orderstatus'], " +
                        "   bucket_count = 1, " +
                        "   sorted_by = array['%s'] " +
                        ")";
        createTableTemplate = createTableTemplate.replaceFirst(sortByColumnName + "[ ]+([^,]*)", sortByColumnName + " " + sortByColumnType);

        assertUpdate(format(
                createTableTemplate,
                tableName,
                sortByColumnName));
        String catalog = getSession().getCatalog().orElseThrow();
        assertUpdate(
                Session.builder(getSession())
                        .setCatalogSessionProperty(catalog, "parquet_writer_page_size", "10000B")
                        .setCatalogSessionProperty(catalog, "parquet_writer_block_size", "2GB")
                        .build(),
                format("INSERT INTO %s SELECT *, ARRAY[rand(), rand(), rand()] FROM tpch.tiny.orders", tableName),
                15000);
    }

    private Location copyInDataFile(String resourceFileName)
            throws IOException
    {
        URL resourceLocation = Resources.getResource(resourceFileName);

        Location tempDir = Location.of("local:///temp_" + UUID.randomUUID());
        fileSystem.createDirectory(tempDir);
        Location dataFile = tempDir.appendPath("data.parquet");
        try (OutputStream out = fileSystem.newOutputFile(dataFile).create()) {
            Resources.copy(resourceLocation, out);
        }
        return dataFile;
    }
}
