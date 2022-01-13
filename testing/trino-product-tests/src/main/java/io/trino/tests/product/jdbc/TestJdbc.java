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
package io.trino.tests.product.jdbc;

import io.trino.jdbc.TrinoConnection;
import io.trino.tempto.ProductTest;
import io.trino.tempto.Requirement;
import io.trino.tempto.RequirementsProvider;
import io.trino.tempto.Requires;
import io.trino.tempto.configuration.Configuration;
import io.trino.tempto.fulfillment.table.hive.tpch.ImmutableTpchTablesRequirements.ImmutableNationTable;
import io.trino.tempto.query.QueryResult;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import static io.trino.tempto.Requirements.compose;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tempto.fulfillment.table.MutableTableRequirement.State.CREATED;
import static io.trino.tempto.fulfillment.table.MutableTablesState.mutableTablesState;
import static io.trino.tempto.fulfillment.table.TableRequirements.immutableTable;
import static io.trino.tempto.fulfillment.table.TableRequirements.mutableTable;
import static io.trino.tempto.fulfillment.table.hive.tpch.TpchTableDefinitions.NATION;
import static io.trino.tempto.internal.convention.SqlResultDescriptor.sqlResultDescriptorForResource;
import static io.trino.tests.product.TestGroups.JDBC;
import static io.trino.tests.product.TpchTableResults.PRESTO_NATION_RESULT;
import static io.trino.tests.product.utils.JdbcDriverUtils.getSessionProperty;
import static io.trino.tests.product.utils.JdbcDriverUtils.resetSessionProperty;
import static io.trino.tests.product.utils.JdbcDriverUtils.setSessionProperty;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.util.Locale.CHINESE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestJdbc
        extends ProductTest
{
    private static final String TABLE_NAME = "nation_table_name";

    private static class ImmutableAndMutableNationTable
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return compose(immutableTable(NATION), mutableTable(NATION, TABLE_NAME, CREATED));
        }
    }

    @Test(groups = JDBC)
    @Requires(ImmutableNationTable.class)
    public void shouldExecuteQuery()
            throws SQLException
    {
        try (Statement statement = connection().createStatement()) {
            QueryResult result = queryResult(statement, "select * from hive.default.nation");
            assertThat(result).matches(PRESTO_NATION_RESULT);
        }
    }

    @Test(groups = JDBC)
    @Requires(ImmutableAndMutableNationTable.class)
    public void shouldInsertSelectQuery()
            throws SQLException
    {
        String tableNameInDatabase = mutableTablesState().get(TABLE_NAME).getNameInDatabase();
        assertThat(onTrino().executeQuery("SELECT * FROM " + tableNameInDatabase)).hasNoRows();

        try (Statement statement = connection().createStatement()) {
            assertThat(statement.executeUpdate("insert into " + tableNameInDatabase + " select * from nation"))
                    .isEqualTo(25);
        }

        assertThat(onTrino().executeQuery("SELECT * FROM " + tableNameInDatabase)).matches(PRESTO_NATION_RESULT);
    }

    @Test(groups = JDBC)
    @Requires(ImmutableNationTable.class)
    public void shouldExecuteQueryWithSelectedCatalogAndSchema()
            throws SQLException
    {
        connection().setCatalog("hive");
        connection().setSchema("default");
        try (Statement statement = connection().createStatement()) {
            QueryResult result = queryResult(statement, "select * from nation");
            assertThat(result).matches(PRESTO_NATION_RESULT);
        }
    }

    @Test(groups = JDBC)
    public void shouldSetTimezone()
            throws SQLException
    {
        String timeZoneId = "Indian/Kerguelen";
        ((TrinoConnection) connection()).setTimeZoneId(timeZoneId); // TODO uew new connection rather than modifying a shared one
        assertConnectionTimezone(connection(), timeZoneId);
    }

    private void assertConnectionTimezone(Connection connection, String timeZoneId)
            throws SQLException
    {
        try (Statement statement = connection.createStatement()) {
            QueryResult result = queryResult(statement, "select current_timezone()");
            assertThat(result).contains(row(timeZoneId));
        }
    }

    @Test(groups = JDBC)
    public void shouldSetLocale()
            throws SQLException
    {
        ((TrinoConnection) connection()).setLocale(CHINESE); // TODO uew new connection rather than modifying a shared one
        try (Statement statement = connection().createStatement()) {
            QueryResult result = queryResult(statement, "SELECT date_format(TIMESTAMP '2001-01-09 09:04', '%M')");
            assertThat(result).contains(row("一月"));
        }
    }

    @Test(groups = JDBC)
    public void shouldGetSchemas()
            throws SQLException
    {
        QueryResult result = QueryResult.forResultSet(metaData().getSchemas("hive", null));
        assertThat(result).contains(row("default", "hive"));
    }

    @Test(groups = JDBC)
    @Requires(ImmutableNationTable.class)
    public void shouldGetTables()
            throws SQLException
    {
        QueryResult result = QueryResult.forResultSet(metaData().getTables("hive", null, null, null));
        assertThat(result).contains(row("hive", "default", "nation", "TABLE", null, null, null, null, null, null));
    }

    @Test(groups = JDBC)
    @Requires(ImmutableNationTable.class)
    public void shouldGetColumns()
            throws SQLException
    {
        QueryResult result = QueryResult.forResultSet(metaData().getColumns("hive", "default", "nation", null));
        assertThat(result).matches(sqlResultDescriptorForResource("io/trino/tests/product/jdbc/get_nation_columns.result"));
    }

    @Test(groups = JDBC)
    @Requires(ImmutableNationTable.class)
    public void shouldGetTableTypes()
            throws SQLException
    {
        QueryResult result = QueryResult.forResultSet(metaData().getTableTypes());
        assertThat(result).contains(row("TABLE"), row("VIEW"));
    }

    @Test(groups = JDBC)
    public void testSessionProperties()
            throws SQLException
    {
        String joinDistributionType = "join_distribution_type";
        String defaultValue = "AUTOMATIC";

        assertThat(getSessionProperty(connection(), joinDistributionType)).isEqualTo(defaultValue);
        setSessionProperty(connection(), joinDistributionType, "BROADCAST");
        assertThat(getSessionProperty(connection(), joinDistributionType)).isEqualTo("BROADCAST");
        resetSessionProperty(connection(), joinDistributionType);
        assertThat(getSessionProperty(connection(), joinDistributionType)).isEqualTo(defaultValue);
    }

    /**
     * Same as {@code io.trino.jdbc.TestJdbcPreparedStatement#testDeallocate()}. This one is run for EnterpriseJdbcDriver as well.
     */
    @Test(groups = JDBC)
    public void testDeallocate()
            throws Exception
    {
        try (Connection connection = connection()) {
            for (int i = 0; i < 200; i++) {
                try {
                    try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT '" + "a".repeat(300) + "'")) {
                        preparedStatement.executeQuery().close(); // Let's not assume when PREPARE actually happens
                    }
                }
                catch (Exception e) {
                    throw new RuntimeException("Failed at " + i, e);
                }
            }
        }
    }

    private QueryResult queryResult(Statement statement, String query)
            throws SQLException
    {
        return QueryResult.forResultSet(statement.executeQuery(query));
    }

    private DatabaseMetaData metaData()
            throws SQLException
    {
        return connection().getMetaData();
    }

    private Connection connection()
    {
        return onTrino().getConnection();
    }
}
