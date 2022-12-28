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
package io.trino.plugin.ignite;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.spi.TrinoException;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import io.trino.tpch.TpchTable;
import org.intellij.lang.annotations.Language;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static io.trino.plugin.ignite.IgniteQueryRunner.createIgniteQueryRunner;
import static io.trino.plugin.ignite.TestIgniteClient.JDBC_CLIENT;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.REGION;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertFalse;

public class TestIgniteConnectorTest
        extends BaseJdbcConnectorTest
{
    private TestingIgniteServer igniteServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.igniteServer = closeAfterClass(new TestingIgniteServer());
        DistributedQueryRunner queryRunner = createIgniteQueryRunner(
                igniteServer,
                ImmutableMap.of(),
                ImmutableMap.of(),
                true);

        copyTpchTable(queryRunner);
        return queryRunner;
    }

    private void copyTpchTable(QueryRunner queryRunner)
    {
        String copySql = "CREATE TABLE IF NOT EXISTS %s AS SELECT * FROM tpch.tiny.%s";
        for (TpchTable tpchTable : ImmutableList.of(CUSTOMER, NATION, ORDERS, REGION)) {
            queryRunner.execute(format(copySql, tpchTable.getTableName(), tpchTable.getTableName()));
        }
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return igniteServer::execute;
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_DELETE:
            case SUPPORTS_CREATE_SCHEMA:
            case SUPPORTS_CREATE_TABLE_WITH_DATA:
            case SUPPORTS_CREATE_VIEW:
            case SUPPORTS_RENAME_TABLE:
            case SUPPORTS_COMMENT_ON_TABLE:
            case SUPPORTS_COMMENT_ON_COLUMN:
            case SUPPORTS_ADD_COLUMN_WITH_COMMENT:
            case SUPPORTS_ARRAY:
            case SUPPORTS_ROW_TYPE:
            case SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT:
            case SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT:
            case SUPPORTS_RENAME_COLUMN:
            case SUPPORTS_TRUNCATE:
                return false;

            case SUPPORTS_LIMIT_PUSHDOWN:
            case SUPPORTS_TOPN_PUSHDOWN:
            case SUPPORTS_TOPN_PUSHDOWN_WITH_VARCHAR:
            case SUPPORTS_NOT_NULL_CONSTRAINT:
                return true;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    public void testInsert()
    {
        String tableName = "test_insert_" + randomTableSuffix();
        @Language("SQL") String create = "CREATE TABLE " + tableName + "(phone varchar(15) , custkey bigint , acctbal double)";
        @Language("SQL") String query = "SELECT phone, custkey, acctbal FROM customer";

        assertUpdate(create);
        assertQuery("SELECT count(*) FROM " + tableName, "SELECT 0");

        assertUpdate("INSERT INTO " + tableName + " " + query, "SELECT count(*) FROM customer");

        assertQuery("SELECT * FROM " + tableName + "", query);

        assertUpdate("INSERT INTO " + tableName + " (custkey) VALUES (-1)", 1);
        assertUpdate("INSERT INTO " + tableName + " (custkey, phone) VALUES (-4, '3283-2001-01-01')", 1);
        assertUpdate("INSERT INTO " + tableName + " (custkey, phone) VALUES (-2, '3283-2001-01-02')", 1);
        assertUpdate("INSERT INTO " + tableName + " (phone, custkey) VALUES ('3283-2001-01-03', -3)", 1);
        assertUpdate("INSERT INTO " + tableName + " (acctbal, custkey) VALUES (1234, -2234)", 1);

        assertQuery("SELECT * FROM " + tableName + "", query
                + " UNION ALL SELECT null, -1, null"
                + " UNION ALL SELECT '3283-2001-01-01', -4, null"
                + " UNION ALL SELECT '3283-2001-01-02', -2, null"
                + " UNION ALL SELECT '3283-2001-01-03', -3, null"
                + " UNION ALL SELECT null, -2234, 1234");

        // UNION query produces columns in the opposite order
        // of how they are declared in the table schema
        // max custkey => 1500
        assertUpdate(
                "INSERT INTO " + tableName + " (custkey, phone, acctbal) " +
                        "SELECT custkey + 1600, phone, acctbal FROM customer " +
                        "UNION ALL " +
                        "SELECT 100000 + custkey, phone, acctbal FROM customer",
                "SELECT 2 * count(*) FROM customer");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateTableWithAllProperties()
    {
        String tableWithAllProperties = "test_create_with_all_properties";
        assertUpdate("CREATE TABLE IF NOT EXISTS " + tableWithAllProperties + " (a bigint, b double, c varchar, d date) WITH (" +
                "primary_key = ARRAY['a', 'b']," +
                "affinity_key = 'a'," +
                "template = 'PARTITIONED'," +
                "write_synchronization_mode = 'FULL_ASYNC'," +
                "cache_group = 'test_group'," +
                "cache_name = 'test_name'," +
                "data_region = 'default')");
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                onRemoteDatabase(),
                "public.tbl",
                "(col_required bigint," +
                        "col_nullable bigint," +
                        "col_default bigint DEFAULT 43," +
                        "col_nonnull_default bigint DEFAULT 42," +
                        "col_required2 bigint NOT NULL, " +
                        "ignite_dummy_id varchar NOT NULL primary key)");
    }

    @Override
    public void testShowCreateTable()
    {
        String catalog = getSession().getCatalog().orElseThrow();
        String schema = getSession().getSchema().orElseThrow();
        try (TestTable testTable = new TestTable(
                igniteServer::execute,
                "show_create_table",
                "(orderkey bigint primary key , custkey bigint, orderstatus varchar(1), totalprice double, orderdate date, orderpriority varchar(15), clerk varchar(15), shippriority integer, comment varchar(79))",
                List.of())) {
            String pattern = "CREATE TABLE %s.%s.%s (\n" +
                    "   orderkey bigint,\n" +
                    "   custkey bigint,\n" +
                    "   orderstatus varchar(1),\n" +
                    "   totalprice double,\n" +
                    "   orderdate date,\n" +
                    "   orderpriority varchar(15),\n" +
                    "   clerk varchar(15),\n" +
                    "   shippriority integer,\n" +
                    "   comment varchar(79)\n" +
                    ")\n" +
                    "WITH (\n" +
                    "   backups = 0,\n" +
                    "   cache_group = 'SQL_%s_%s',\n" +
                    "   cache_name = 'SQL_%4$s_%5$s',\n" +
                    "   primary_key = ARRAY['orderkey'],\n" +
                    "   template = 'PARTITIONED',\n" +
                    "   write_synchronization_mode = 'FULL_SYNC'\n" +
                    ")";
            String tableName = testTable.getName();
            assertThat((String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue())
                    .isEqualTo(format(pattern, catalog, schema, tableName, schema.toUpperCase(Locale.ENGLISH), tableName.toUpperCase(Locale.ENGLISH)));
        }
    }

    @Test
    public void testAvgDecimalExceedingSupportedPrecision()
    {
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_avg_decimal_exceeding_supported_precision",
                "(a decimal(38, 38), b bigint)",
                List.of(
                        "CAST ('0.12345671234567123456712345671234567121' AS decimal(38, 38)), 1",
                        "CAST ('0.12345671234567123456712345671234567122' AS decimal(38, 38)), 2",
                        "CAST ('0.12345671234567123456712345671234567123' AS decimal(38, 38)), 3",
                        "CAST ('0.12345671234567123456712345671234567124' AS decimal(38, 38)), 4",
                        "CAST ('0.12345671234567123456712345671234567125' AS decimal(38, 38)), 5",
                        "CAST ('0.12345671234567123456712345671234567126' AS decimal(38, 38)), 6",
                        "CAST ('0.12345671234567123456712345671234567127' AS decimal(38, 38)), 7"))) {
            assertThat(query("SELECT avg(a) avg_a  FROM " + testTable.getName()))
                    .matches("SELECT CAST ('0.12345671234567123456712345671234567124' AS decimal(38, 38))");
            assertThat(query(format("SELECT avg(a) avg_a FROM %s WHERE b <= 2", testTable.getName())))
                    .matches("SELECT CAST ('0.123456712345671234567123456712345671215' AS decimal(38, 38))");
        }
    }

    @Override
    protected TestTable createAggregationTestTable(String name, List<String> rows)
    {
        return new TestTable(onRemoteDatabase(),
                name,
                "(short_decimal decimal(9, 3), long_decimal decimal(30, 10), t_double double, a_bigint bigint primary key)",
                rows);
    }

    @Override
    protected TestTable createTableWithDoubleAndRealColumns(String name, List<String> rows)
    {
        return new TestTable(onRemoteDatabase(),
                name,
                "(t_double double, u_double double, v_real real, w_real real primary key)",
                rows);
    }

    @Override
    protected Optional<String> filterColumnNameTestData(String columnName)
    {
        // https://issues.apache.org/jira/browse/IGNITE-18102
        if (columnName.contains(".")) {
            return Optional.empty();
        }

        if (columnName.contains("`") || columnName.contains("\"")) {
            return Optional.empty();
        }

        return Optional.of(columnName);
    }

    @Override
    public void testNativeQuerySimple()
    {
        // table function disabled for Ignite, because it doesn't provide ResultSetMetaData, so the result relation type cannot be determined
        assertQueryFails("SELECT * FROM TABLE(system.query(query => 'SELECT 1'))", "line 1:21: Table function system.query not registered");
    }

    @Override
    public void testNativeQueryParameters()
    {
        // table function disabled for Ignite, because it doesn't provide ResultSetMetaData, so the result relation type cannot be determined
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query_simple", "SELECT * FROM TABLE(system.query(query => ?))")
                .addPreparedStatement("my_query", "SELECT * FROM TABLE(system.query(query => format('SELECT %s FROM %s', ?, ?)))")
                .build();
        assertQueryFails(session, "EXECUTE my_query_simple USING 'SELECT 1 a'", "line 1:21: Table function system.query not registered");
        assertQueryFails(session, "EXECUTE my_query USING 'a', '(SELECT 2 a) t'", "line 1:21: Table function system.query not registered");
    }

    @Override
    public void testNativeQuerySelectFromNation()
    {
        // table function disabled for Ignite, because it doesn't provide ResultSetMetaData, so the result relation type cannot be determined
        assertQueryFails(
                format("SELECT * FROM TABLE(system.query(query => 'SELECT name FROM %s.nation WHERE nationkey = 0'))", getSession().getSchema().orElseThrow()),
                "line 1:21: Table function system.query not registered");
    }

    @Override
    public void testNativeQuerySelectFromTestTable()
    {
        // table function disabled for Ignite, because it doesn't provide ResultSetMetaData, so the result relation type cannot be determined
        try (TestTable testTable = simpleTable()) {
            assertQueryFails(
                    format("SELECT * FROM TABLE(system.query(query => 'SELECT * FROM %s'))", testTable.getName()),
                    "line 1:21: Table function system.query not registered");
        }
    }

    @Override
    public void testNativeQuerySelectUnsupportedType()
    {
        // table function disabled for Ignite, because it doesn't provide ResultSetMetaData, so the result relation type cannot be determined
        try (TestTable testTable = createTableWithUnsupportedColumn()) {
            String unqualifiedTableName = testTable.getName().replaceAll("^\\w+\\.", "");
            // Check that column 'two' is not supported.
            assertQuery("SELECT column_name FROM information_schema.columns WHERE table_name = '" + unqualifiedTableName + "'", "VALUES 'one', 'three'");
            assertUpdate("INSERT INTO " + testTable.getName() + " (one, three) VALUES (123, 'test')", 1);
            assertThatThrownBy(() -> query(format("SELECT * FROM TABLE(system.query(query => 'SELECT * FROM %s'))", testTable.getName())))
                    .hasMessage("line 1:21: Table function system.query not registered");
        }
    }

    @Override
    public void testNativeQueryCreateStatement()
    {
        // table function disabled for Ignite, because it doesn't provide ResultSetMetaData, so the result relation type cannot be determined
        assertFalse(getQueryRunner().tableExists(getSession(), "numbers"));
        assertThatThrownBy(() -> query("SELECT * FROM TABLE(system.query(query => 'CREATE TABLE numbers(n INTEGER)'))"))
                .hasMessage("line 1:21: Table function system.query not registered");
        assertFalse(getQueryRunner().tableExists(getSession(), "numbers"));
    }

    @Override
    public void testNativeQueryInsertStatementTableDoesNotExist()
    {
        // table function disabled for Ignite, because it doesn't provide ResultSetMetaData, so the result relation type cannot be determined
        assertFalse(getQueryRunner().tableExists(getSession(), "non_existent_table"));
        assertThatThrownBy(() -> query("SELECT * FROM TABLE(system.query(query => 'INSERT INTO non_existent_table VALUES (1)'))"))
                .hasMessage("line 1:21: Table function system.query not registered");
    }

    @Override
    public void testNativeQueryInsertStatementTableExists()
    {
        // table function disabled for Ignite, because it doesn't provide ResultSetMetaData, so the result relation type cannot be determined
        try (TestTable testTable = simpleTable()) {
            assertThatThrownBy(() -> query(format("SELECT * FROM TABLE(system.query(query => 'INSERT INTO %s VALUES (3, 4)'))", testTable.getName())))
                    .hasMessage("line 1:21: Table function system.query not registered");
            assertQuery("SELECT * FROM " + testTable.getName(), "VALUES (1, 1), (2, 2)");
        }
    }

    @Override
    protected TestTable simpleTable()
    {
        return new TestTable(onRemoteDatabase(), format("%s.simple_table", getSession().getSchema().orElseThrow()), "(col BIGINT, id bigint primary key)", ImmutableList.of("1, 1", "2, 2"));
    }

    @Override
    public void testNativeQueryIncorrectSyntax()
    {
        // table function disabled for Ignite, because it doesn't provide ResultSetMetaData, so the result relation type cannot be determined
        assertThatThrownBy(() -> query("SELECT * FROM TABLE(system.query(query => 'some wrong syntax'))"))
                .hasMessage("line 1:21: Table function system.query not registered");
    }

    protected void verifyAddNotNullColumnToNonEmptyTableFailurePermissible(Throwable e)
    {
        // Ignite will not throw any exception here.
    }

    @Test
    public void testCharVarcharComparison()
    {
        // Ignite will map char to varchar, skip
        throw new SkipException("Ignite map char to varchar, skip test");
    }

    @Override
    protected String errorMessageForInsertIntoNotNullColumn(String columnName)
    {
        return format("Failed to insert data: Null value is not allowed for column '%s'", columnName.toUpperCase(Locale.ENGLISH));
    }

    @Test
    public void testCharTrailingSpace()
    {
        throw new SkipException("Ignite not support char trailing space");
    }

    @Override
    public void testDropAndAddColumnWithSameName()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_drop_add_column", "(x int NOT NULL, y int, z int)", ImmutableList.of("1,2,3"))) {
            assertUpdate("ALTER TABLE " + table.getName() + " DROP COLUMN y");
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (1, 3)");

            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN y int");
            // Follow the behavior of Ignite
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (1, 3, 2)");
        }
    }

    @Override
    public void testCreateTableAsSelect()
    {
        assertThatThrownBy(() -> JDBC_CLIENT.beginCreateTable(null, null))
                .isInstanceOf(TrinoException.class)
                .hasMessage("This connector does not support creating tables with data");
    }

    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("timestamp(3) with time zone") ||
                typeName.equals("timestamp(6) with time zone")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }

        if (typeName.startsWith("time")) {
            return Optional.empty();
        }

        return Optional.of(dataMappingTestSetup);
    }
}
