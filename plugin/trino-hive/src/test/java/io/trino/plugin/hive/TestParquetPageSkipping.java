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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static io.trino.testing.assertions.Assert.assertEquals;
import static io.trino.tpch.TpchTable.ORDERS;
import static java.lang.String.format;
import static org.testng.Assert.assertTrue;

public class TestParquetPageSkipping
        extends AbstractTestQueryFramework
{
    private static final String PAGE_SKIPPING_TABLE = "orders_bucketed_and_sorted";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                .setHiveProperties(ImmutableMap.of(
                        "hive.allow-register-partition-procedure", "true",
                        // Reduce writer sort buffer size to ensure SortingFileWriter gets used
                        "hive.writer-sort-buffer-size", "1MB",
                        "parquet.use-column-index", "true"))
                .setInitialTables(ImmutableList.of(ORDERS))
                .build();
    }

    private void buildSortedTables(String sortByColumnName, String sortByColumnType)
    {
        String createTableTemplate =
                "CREATE TABLE %s.%s.%s (\n" +
                "   orderkey bigint,\n" +
                "   custkey bigint,\n" +
                "   orderstatus varchar(1),\n" +
                "   totalprice double,\n" +
                "   orderdate date,\n" +
                "   orderpriority varchar(15),\n" +
                "   clerk varchar(15),\n" +
                "   shippriority integer,\n" +
                "   comment varchar(79),\n" +
                "   rvalues double array\n" +
                ")\n" +
                "WITH (\n" +
                "   format = 'PARQUET',\n" +
                "   bucketed_by = array['orderstatus'],\n" +
                "   bucket_count = 1,\n" +
                "   sorted_by = array['%s']\n" +
                ")";
        createTableTemplate = createTableTemplate.replaceFirst(sortByColumnName + "[ ]+([^,]*)", sortByColumnName + " " + sortByColumnType);
        String createTableSql = format(
                createTableTemplate,
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                PAGE_SKIPPING_TABLE,
                sortByColumnName);

        assertUpdate(createTableSql);
        assertUpdate(
                Session.builder(getSession())
                        .setCatalogSessionProperty(getSession().getCatalog().get(), "parquet_writer_page_size", "10000B")
                        .setCatalogSessionProperty(getSession().getCatalog().get(), "parquet_writer_block_size", "100GB")
                        .build(),
                format("INSERT INTO %s SELECT *,ARRAY[rand(),rand(),rand()] FROM orders", PAGE_SKIPPING_TABLE),
                15000);
    }

    @Test
    public void testAndPredicates()
    {
        try {
            buildSortedTables("totalprice", "double");
            String query = "SELECT * FROM " + PAGE_SKIPPING_TABLE + " WHERE totalprice BETWEEN 100000 AND 131280 AND clerk = 'Clerk#000000624'";
            int rowCount = assertSameResults(query);
            assertTrue(rowCount > 0);
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", PAGE_SKIPPING_TABLE));
        }
    }

    @Test(dataProvider = "dataType")
    public void testPageSkipping(String sortByColumn, String sortByColumnType, Object[][] valuesArray)
    {
        try {
            buildSortedTables(sortByColumn, sortByColumnType);
            for (Object[] values : valuesArray) {
                Object lowValue = values[0];
                Object middleLowValue = values[1];
                Object middleHighValue = values[2];
                Object highValue = values[3];
                assertSameResults(format("SELECT %s FROM %s WHERE %s = %s", sortByColumn, PAGE_SKIPPING_TABLE, sortByColumn, middleLowValue));
                assertTrue(assertSameResults(format("SELECT %s FROM %s WHERE %s < %s ORDER BY %s", sortByColumn, PAGE_SKIPPING_TABLE, sortByColumn, lowValue, sortByColumn)) > 0);
                assertTrue(assertSameResults(format("SELECT %s FROM %s WHERE %s > %s ORDER BY %s", sortByColumn, PAGE_SKIPPING_TABLE, sortByColumn, highValue, sortByColumn)) > 0);
                assertTrue(assertSameResults(format("SELECT %s FROM %s WHERE %s BETWEEN %s AND %s ORDER BY %s", sortByColumn, PAGE_SKIPPING_TABLE, sortByColumn, middleLowValue, middleHighValue, sortByColumn)) > 0);
                // Tests synchronization of reading values across columns
                assertSameResults(format("SELECT * FROM %s WHERE %s = %s ORDER BY orderkey", PAGE_SKIPPING_TABLE, sortByColumn, middleLowValue));
                assertTrue(assertSameResults(format("SELECT * FROM %s WHERE %s < %s ORDER BY orderkey", PAGE_SKIPPING_TABLE, sortByColumn, lowValue)) > 0);
                assertTrue(assertSameResults(format("SELECT * FROM %s WHERE %s > %s ORDER BY orderkey", PAGE_SKIPPING_TABLE, sortByColumn, highValue)) > 0);
                assertTrue(assertSameResults(format("SELECT * FROM %s WHERE %s BETWEEN %s AND %s ORDER BY orderkey", PAGE_SKIPPING_TABLE, sortByColumn, middleLowValue, middleHighValue)) > 0);
            }
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s", PAGE_SKIPPING_TABLE));
        }
    }

    private int assertSameResults(String query)
    {
        MaterializedResult withColumnIndexing = computeActual(query);
        MaterializedResult withoutColumnIndexing = computeActual(Session.builder(getSession())
                        .setCatalogSessionProperty(getSession().getCatalog().get(), "parquet_use_column_index", "false")
                        .build(),
                query);
        assertEquals(withColumnIndexing, withoutColumnIndexing);
        return withoutColumnIndexing.getRowCount();
    }

    @DataProvider
    public Object[][] dataType()
    {
        return new Object[][] {
                {"orderkey", "bigint", new Object[][]{{2, 7520, 7523, 14950}}},
                {"totalprice", "double", new Object[][]{{974.04, 131094.34, 131279.97, 406938.36}}},
                {"totalprice", "real", new Object[][]{{974.04, 131094.34, 131279.97, 406938.36}}},
                {"totalprice", "decimal(12,2)", new Object[][]{
                        {974.04, 131094.34, 131279.97, 406938.36},
                        {973, 131095, 131280, 406950},
                        {974.04123, 131094.34123, 131279.97012, 406938.36555}}},
                {"totalprice", "decimal(12,0)", new Object[][]{
                        {973, 131095, 131280, 406950}}},
                {"totalprice", "decimal(35,2)", new Object[][]{
                        {974.04, 131094.34, 131279.97, 406938.36},
                        {973, 131095, 131280, 406950},
                        {974.04123, 131094.34123, 131279.97012, 406938.36555}}},
                {"orderdate", "date", new Object[][]{{"DATE '1992-01-05'", "DATE '1995-10-13'", "DATE '1995-10-13'", "DATE '1998-07-29'"}}},
                {"orderdate", "timestamp", new Object[][]{{"TIMESTAMP '1992-01-05'", "TIMESTAMP '1995-10-13'", "TIMESTAMP '1995-10-14'", "TIMESTAMP '1998-07-29'"}}},
                {"clerk", "varchar(15)", new Object[][]{{"'Clerk#000000006'", "'Clerk#000000508'", "'Clerk#000000513'", "'Clerk#000000996'"}}},
                {"custkey", "integer", new Object[][]{{4, 634, 640, 1493}}},
                {"custkey", "smallint", new Object[][]{{4, 634, 640, 1493}}}
        };
    }
}
