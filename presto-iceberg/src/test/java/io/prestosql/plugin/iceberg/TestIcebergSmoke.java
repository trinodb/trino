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
package io.prestosql.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.testing.AbstractTestIntegrationSmokeTest;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.assertions.Assert;
import org.apache.iceberg.FileFormat;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.function.BiConsumer;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.plugin.iceberg.IcebergQueryRunner.createIcebergQueryRunner;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.MaterializedResult.resultBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestIcebergSmoke
        extends AbstractTestIntegrationSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createIcebergQueryRunner(ImmutableMap.of());
    }

    @Test
    @Override
    public void testDescribeTable()
    {
        MaterializedResult expectedColumns = resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("clerk", "varchar", "", "")
                .row("shippriority", "integer", "", "")
                .row("comment", "varchar", "", "")
                .build();
        MaterializedResult actualColumns = computeActual("DESCRIBE orders");
        Assert.assertEquals(actualColumns, expectedColumns);
    }

    @Override
    public void testShowCreateTable()
    {
        assertThat(computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .isEqualTo("CREATE TABLE iceberg.tpch.orders (\n" +
                        "   orderkey bigint,\n" +
                        "   custkey bigint,\n" +
                        "   orderstatus varchar,\n" +
                        "   totalprice double,\n" +
                        "   orderdate date,\n" +
                        "   orderpriority varchar,\n" +
                        "   clerk varchar,\n" +
                        "   shippriority integer,\n" +
                        "   comment varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = 'ORC'\n" +
                        ")");
    }

    @Test
    public void testDecimal()
    {
        assertUpdate("CREATE TABLE test_decimal_short (x decimal(3,2))");
        assertQueryFails("INSERT INTO test_decimal_short VALUES (decimal '3.14')", "Writing to columns of type decimal not yet supported");
//        assertQuery("SELECT * FROM test_decimal_short", "SELECT CAST('3.14' AS DECIMAL(3,2))");
        dropTable(getSession(), "test_decimal_short");

        assertUpdate("CREATE TABLE test_decimal_long (x decimal(25,2))");
        assertQueryFails("INSERT INTO test_decimal_long VALUES (decimal '3.14')", "Writing to columns of type decimal not yet supported");
//        assertQuery("SELECT * FROM test_decimal_long", "SELECT CAST('3.14' AS DECIMAL(25,2))");
        dropTable(getSession(), "test_decimal_long");
    }

    @Test
    public void testTimestamp()
    {
        assertUpdate("CREATE TABLE test_timestamp (x timestamp)");
        assertQueryFails("INSERT INTO test_timestamp VALUES (timestamp '2017-05-01 10:12:34')", "Writing to columns of type timestamp not yet supported");
//        assertQuery("SELECT * FROM test_timestamp", "SELECT CAST('2017-05-01 10:12:34' AS TIMESTAMP)");
        dropTable(getSession(), "test_timestamp");
    }

    @Test
    public void testCreatePartitionedTable()
    {
        testWithAllFileFormats(this::testCreatePartitionedTable);
        testWithAllFileFormats(this::testCreatePartitionedTableWithNestedTypes);
    }

    private void testCreatePartitionedTable(Session session, FileFormat fileFormat)
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_partitioned_table (" +
                "  _string VARCHAR" +
                ", _bigint BIGINT" +
                ", _integer INTEGER" +
                ", _real REAL" +
                ", _double DOUBLE" +
                ", _boolean BOOLEAN" +
//                ", _decimal_short DECIMAL(3,2)" +
//                ", _decimal_long DECIMAL(30,10)" +
//                ", _timestamp TIMESTAMP" +
                ", _date DATE" +
                ") " +
                "WITH (" +
                "format = '" + fileFormat + "', " +
                "partitioning = ARRAY[" +
                "  '_string'," +
                "  '_integer'," +
                "  '_bigint'," +
                "  '_boolean'," +
                "  '_real'," +
                "  '_double'," +
//                "  '_decimal_short', " +
//                "  '_decimal_long'," +
//                "  '_timestamp'," +
                "  '_date']" +
                ")";

        assertUpdate(session, createTable);

        MaterializedResult result = computeActual("SELECT * from test_partitioned_table");
        assertEquals(result.getRowCount(), 0);

        @Language("SQL") String select = "" +
                "SELECT" +
                " 'foo' _string" +
                ", CAST(123 AS BIGINT) _bigint" +
                ", 456 _integer" +
                ", CAST('123.45' AS REAL) _real" +
                ", CAST('3.14' AS DOUBLE) _double" +
                ", true _boolean" +
//                ", CAST('3.14' AS DECIMAL(3,2)) _decimal_short" +
//                ", CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) _decimal_long" +
//                ", CAST('2017-05-01 10:12:34' AS TIMESTAMP) _timestamp" +
                ", CAST('2017-05-01' AS DATE) _date";

        assertUpdate(session, "INSERT INTO test_partitioned_table " + select, 1);
        assertQuery(session, "SELECT * from test_partitioned_table", select);
        assertQuery(session, "" +
                        "SELECT * FROM test_partitioned_table WHERE" +
                        " 'foo' = _string" +
                        " AND 456 = _integer" +
                        " AND CAST(123 AS BIGINT) = _bigint" +
                        " AND true = _boolean" +
//                        " AND CAST('3.14' AS DECIMAL(3,2)) = _decimal_short" +
//                        " AND CAST('12345678901234567890.0123456789' AS DECIMAL(30,10)) = _decimal_long" +
//                        " AND CAST('2017-05-01 10:12:34' AS TIMESTAMP) = _timestamp" +
                        " AND CAST('2017-05-01' AS DATE) = _date",
                select);

        dropTable(session, "test_partitioned_table");
    }

    private void testCreatePartitionedTableWithNestedTypes(Session session, FileFormat fileFormat)
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_partitioned_table_nested_type (" +
                "  _string VARCHAR" +
                ", _struct ROW(_field1 INT, _field2 VARCHAR)" +
                ", _date DATE" +
                ") " +
                "WITH (" +
                "format = '" + fileFormat + "', " +
                "partitioning = ARRAY['_date']" +
                ")";

        assertUpdate(session, createTable);

        dropTable(session, "test_partitioned_table_nested_type");
    }

    @Test
    public void testCreatePartitionedTableAs()
    {
        testWithAllFileFormats(this::testCreatePartitionedTableAs);
    }

    private void testCreatePartitionedTableAs(Session session, FileFormat fileFormat)
    {
        @Language("SQL") String createTable = "" +
                "CREATE TABLE test_create_partitioned_table_as " +
                "WITH (" +
                "format = '" + fileFormat + "', " +
                "partitioning = ARRAY['ORDER_STATUS', 'Ship_Priority', 'Bucket(order_key,9)']" +
                ") " +
                "AS " +
                "SELECT orderkey AS order_key, shippriority AS ship_priority, orderstatus AS order_status " +
                "FROM tpch.tiny.orders";

        assertUpdate(session, createTable, "SELECT count(*) from orders");

        String createTableSql = format("" +
                        "CREATE TABLE %s.%s.%s (\n" +
                        "   order_key bigint,\n" +
                        "   ship_priority integer,\n" +
                        "   order_status varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = '" + fileFormat + "',\n" +
                        "   partitioning = ARRAY['order_status','ship_priority','bucket(order_key, 9)']\n" +
                        ")",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                "test_create_partitioned_table_as");

        MaterializedResult actualResult = computeActual("SHOW CREATE TABLE test_create_partitioned_table_as");
        assertEquals(getOnlyElement(actualResult.getOnlyColumnAsSet()), createTableSql);

//        assertEquals(partitions.size(), 3);

        assertQuery(session, "SELECT * from test_create_partitioned_table_as", "SELECT orderkey, shippriority, orderstatus FROM orders");

        dropTable(session, "test_create_partitioned_table_as");
    }

    @Test
    public void testColumnComments()
    {
        Session session = getSession();
        assertUpdate(session, "CREATE TABLE test_column_comments (_bigint BIGINT COMMENT 'test column comment')");

        assertQuery(session, "SHOW COLUMNS FROM test_column_comments",
                "VALUES ('_bigint', 'bigint', '', 'test column comment')");

        dropTable(session, "test_column_comments");
    }

    @Test
    public void testSchemaEvolution()
    {
        // Schema evolution should be id based
        testWithAllFileFormats(this::testSchemaEvolution);
    }

    private void testSchemaEvolution(Session session, FileFormat fileFormat)
    {
        assertUpdate(session, "CREATE TABLE test_schema_evolution_drop_end (col0 INTEGER, col1 INTEGER, col2 INTEGER) WITH (format = '" + fileFormat + "')");
        assertUpdate(session, "INSERT INTO test_schema_evolution_drop_end VALUES (0, 1, 2)", 1);
        assertQuery(session, "SELECT * FROM test_schema_evolution_drop_end", "VALUES(0, 1, 2)");
        assertUpdate(session, "ALTER TABLE test_schema_evolution_drop_end DROP COLUMN col2");
        assertQuery(session, "SELECT * FROM test_schema_evolution_drop_end", "VALUES(0, 1)");
        assertUpdate(session, "ALTER TABLE test_schema_evolution_drop_end ADD COLUMN col2 INTEGER");
        assertUpdate(session, "INSERT INTO test_schema_evolution_drop_end VALUES (3, 4, 5)", 1);
        assertQuery(session, "SELECT * FROM test_schema_evolution_drop_end", "VALUES(0, 1, NULL), (3, 4, 5)");
        dropTable(session, "test_schema_evolution_drop_end");

        assertUpdate(session, "CREATE TABLE test_schema_evolution_drop_middle (col0 INTEGER, col1 INTEGER, col2 INTEGER) WITH (format = '" + fileFormat + "')");
        assertUpdate(session, "INSERT INTO test_schema_evolution_drop_middle VALUES (0, 1, 2)", 1);
        assertQuery(session, "SELECT * FROM test_schema_evolution_drop_middle", "VALUES(0, 1, 2)");
        assertUpdate(session, "ALTER TABLE test_schema_evolution_drop_middle DROP COLUMN col1");
        assertQuery(session, "SELECT * FROM test_schema_evolution_drop_middle", "VALUES(0, 2)");
        assertUpdate(session, "ALTER TABLE test_schema_evolution_drop_middle ADD COLUMN col1 INTEGER");
        assertUpdate(session, "INSERT INTO test_schema_evolution_drop_middle VALUES (3, 4, 5)", 1);
        assertQuery(session, "SELECT * FROM test_schema_evolution_drop_middle", "VALUES(0, 2, NULL), (3, 4, 5)");
        dropTable(session, "test_schema_evolution_drop_middle");
    }

    private void testWithAllFileFormats(BiConsumer<Session, FileFormat> test)
    {
        test.accept(getSession(), FileFormat.PARQUET);
        test.accept(getSession(), FileFormat.ORC);
    }

    private void dropTable(Session session, String table)
    {
        assertUpdate(session, "DROP TABLE " + table);
        assertFalse(getQueryRunner().tableExists(session, table));
    }
}
