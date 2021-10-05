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
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestParquetPageSkipping
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                .setHiveProperties(ImmutableMap.of(
                        // Reduce writer sort buffer size to ensure SortingFileWriter gets used
                        "hive.writer-sort-buffer-size", "1MB",
                        "parquet.use-column-index", "true"))
                .build();
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
                        .setCatalogSessionProperty(catalog, "parquet_writer_block_size", "100GB")
                        .build(),
                format("INSERT INTO %s SELECT *, ARRAY[rand(), rand(), rand()] FROM tpch.tiny.orders", tableName),
                15000);
    }

    @Test
    public void testAndPredicates()
    {
        String tableName = "test_and_predicate_" + randomTableSuffix();
        buildSortedTables(tableName, "totalprice", "double");
        int rowCount = assertColumnIndexResults("SELECT * FROM " + tableName + " WHERE totalprice BETWEEN 100000 AND 131280 AND clerk = 'Clerk#000000624'");
        assertThat(rowCount).isGreaterThan(0);

        // `totalprice BETWEEN 51890 AND 51900` is chosen to lie between min/max values of row group
        // but outside page level min/max boundaries to trigger pruning of row group using column index
        assertRowGroupPruning("SELECT * FROM " + tableName + " WHERE totalprice BETWEEN 51890 AND 51900 AND orderkey > 0");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testPageSkippingWithNonSequentialOffsets()
    {
        String tableName = "test_random_" + randomTableSuffix();
        int updateCount = 8192;
        assertUpdate(
                "CREATE TABLE " + tableName + " (col) WITH (format = 'PARQUET') AS " +
                        "SELECT * FROM unnest(transform(repeat(1, 8192), x -> rand()))",
                updateCount);
        for (int i = 0; i < 8; i++) {
            assertUpdate(
                    "INSERT INTO " + tableName + " SELECT rand() FROM " + tableName,
                    updateCount);
            updateCount += updateCount;
        }
        // These queries select a subset of pages which are stored at non-sequential offsets
        // This reproduces the issue identified in https://github.com/trinodb/trino/issues/9097
        for (double i = 0; i < 1; i += 0.1) {
            assertColumnIndexResults(format("SELECT * FROM %s WHERE col BETWEEN %f AND %f", tableName, i - 0.00001, i + 0.00001));
        }
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testFilteringOnColumnNameWithDot()
    {
        String nameInSql = "\"a.dot\"";
        String tableName = "test_column_name_with_dot_" + randomTableSuffix();

        assertUpdate("CREATE TABLE " + tableName + "(key varchar(50), " + nameInSql + " varchar(50)) WITH (format = 'PARQUET')");
        assertUpdate("INSERT INTO " + tableName + " VALUES ('null value', NULL), ('sample value', 'abc'), ('other value', 'xyz')", 3);

        assertQuery("SELECT key FROM " + tableName + " WHERE " + nameInSql + " IS NULL", "VALUES ('null value')");
        assertQuery("SELECT key FROM " + tableName + " WHERE " + nameInSql + " = 'abc'", "VALUES ('sample value')");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test(dataProvider = "dataType")
    public void testPageSkipping(String sortByColumn, String sortByColumnType, Object[][] valuesArray)
    {
        String tableName = "test_page_skipping_" + randomTableSuffix();
        buildSortedTables(tableName, sortByColumn, sortByColumnType);
        for (Object[] values : valuesArray) {
            Object lowValue = values[0];
            Object middleLowValue = values[1];
            Object middleHighValue = values[2];
            Object highValue = values[3];
            assertColumnIndexResults(format("SELECT %s FROM %s WHERE %s = %s", sortByColumn, tableName, sortByColumn, middleLowValue));
            assertThat(assertColumnIndexResults(format("SELECT %s FROM %s WHERE %s < %s ORDER BY %s", sortByColumn, tableName, sortByColumn, lowValue, sortByColumn))).isGreaterThan(0);
            assertThat(assertColumnIndexResults(format("SELECT %s FROM %s WHERE %s > %s ORDER BY %s", sortByColumn, tableName, sortByColumn, highValue, sortByColumn))).isGreaterThan(0);
            assertThat(assertColumnIndexResults(format("SELECT %s FROM %s WHERE %s BETWEEN %s AND %s ORDER BY %s", sortByColumn, tableName, sortByColumn, middleLowValue, middleHighValue, sortByColumn))).isGreaterThan(0);
            // Tests synchronization of reading values across columns
            assertColumnIndexResults(format("SELECT * FROM %s WHERE %s = %s ORDER BY orderkey", tableName, sortByColumn, middleLowValue));
            assertThat(assertColumnIndexResults(format("SELECT * FROM %s WHERE %s < %s ORDER BY orderkey", tableName, sortByColumn, lowValue))).isGreaterThan(0);
            assertThat(assertColumnIndexResults(format("SELECT * FROM %s WHERE %s > %s ORDER BY orderkey", tableName, sortByColumn, highValue))).isGreaterThan(0);
            assertThat(assertColumnIndexResults(format("SELECT * FROM %s WHERE %s BETWEEN %s AND %s ORDER BY orderkey", tableName, sortByColumn, middleLowValue, middleHighValue))).isGreaterThan(0);
        }
        assertUpdate("DROP TABLE " + tableName);
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
                results -> assertThat(results.getRowCount()).isEqualTo(0),
                new Duration(10, SECONDS));

        assertQueryStats(
                getSession(),
                sql,
                queryStats -> {
                    assertThat(queryStats.getPhysicalInputPositions()).isEqualTo(0);
                    assertThat(queryStats.getProcessedInputPositions()).isEqualTo(0);
                },
                results -> assertThat(results.getRowCount()).isEqualTo(0),
                new Duration(10, SECONDS));
    }

    @DataProvider
    public Object[][] dataType()
    {
        return new Object[][] {
                {"orderkey", "bigint", new Object[][] {{2, 7520, 7523, 14950}}},
                {"totalprice", "double", new Object[][] {{974.04, 131094.34, 131279.97, 406938.36}}},
                {"totalprice", "real", new Object[][] {{974.04, 131094.34, 131279.97, 406938.36}}},
                {"totalprice", "decimal(12,2)", new Object[][] {
                        {974.04, 131094.34, 131279.97, 406938.36},
                        {973, 131095, 131280, 406950},
                        {974.04123, 131094.34123, 131279.97012, 406938.36555}}},
                {"totalprice", "decimal(12,0)", new Object[][] {
                        {973, 131095, 131280, 406950}}},
                {"totalprice", "decimal(35,2)", new Object[][] {
                        {974.04, 131094.34, 131279.97, 406938.36},
                        {973, 131095, 131280, 406950},
                        {974.04123, 131094.34123, 131279.97012, 406938.36555}}},
                {"orderdate", "date", new Object[][] {{"DATE '1992-01-05'", "DATE '1995-10-13'", "DATE '1995-10-13'", "DATE '1998-07-29'"}}},
                {"orderdate", "timestamp", new Object[][] {{"TIMESTAMP '1992-01-05'", "TIMESTAMP '1995-10-13'", "TIMESTAMP '1995-10-14'", "TIMESTAMP '1998-07-29'"}}},
                {"clerk", "varchar(15)", new Object[][] {{"'Clerk#000000006'", "'Clerk#000000508'", "'Clerk#000000513'", "'Clerk#000000996'"}}},
                {"custkey", "integer", new Object[][] {{4, 634, 640, 1493}}},
                {"custkey", "smallint", new Object[][] {{4, 634, 640, 1493}}}
        };
    }

    private Session noParquetColumnIndexFiltering(Session session)
    {
        return Session.builder(session)
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "parquet_use_column_index", "false")
                .build();
    }
}
