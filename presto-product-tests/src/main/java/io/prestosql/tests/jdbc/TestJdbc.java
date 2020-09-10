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
package io.prestosql.tests.jdbc;

import io.prestosql.jdbc.PrestoConnection;
import io.prestosql.tempto.ProductTest;
import io.prestosql.tempto.Requirement;
import io.prestosql.tempto.RequirementsProvider;
import io.prestosql.tempto.Requires;
import io.prestosql.tempto.configuration.Configuration;
import io.prestosql.tempto.fulfillment.table.hive.tpch.ImmutableTpchTablesRequirements.ImmutableNationTable;
import io.prestosql.tempto.query.QueryResult;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import static io.prestosql.tempto.Requirements.compose;
import static io.prestosql.tempto.assertions.QueryAssert.Row.row;
import static io.prestosql.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tempto.fulfillment.table.MutableTableRequirement.State.CREATED;
import static io.prestosql.tempto.fulfillment.table.MutableTablesState.mutableTablesState;
import static io.prestosql.tempto.fulfillment.table.TableRequirements.immutableTable;
import static io.prestosql.tempto.fulfillment.table.TableRequirements.mutableTable;
import static io.prestosql.tempto.fulfillment.table.hive.tpch.TpchTableDefinitions.NATION;
import static io.prestosql.tempto.internal.convention.SqlResultDescriptor.sqlResultDescriptorForResource;
import static io.prestosql.tempto.query.QueryExecutor.defaultQueryExecutor;
import static io.prestosql.tempto.query.QueryExecutor.query;
import static io.prestosql.tests.TestGroups.JDBC;
import static io.prestosql.tests.TpchTableResults.PRESTO_NATION_RESULT;
import static io.prestosql.tests.utils.JdbcDriverUtils.getSessionProperty;
import static io.prestosql.tests.utils.JdbcDriverUtils.resetSessionProperty;
import static io.prestosql.tests.utils.JdbcDriverUtils.setSessionProperty;
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
        assertThat(query("SELECT * FROM " + tableNameInDatabase)).hasNoRows();

        try (Statement statement = connection().createStatement()) {
            assertThat(statement.executeUpdate("insert into " + tableNameInDatabase + " select * from nation"))
                    .isEqualTo(25);
        }

        assertThat(query("SELECT * FROM " + tableNameInDatabase)).matches(PRESTO_NATION_RESULT);
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
        ((PrestoConnection) connection()).setTimeZoneId(timeZoneId); // TODO uew new connection rather than modifying a shared one
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
        ((PrestoConnection) connection()).setLocale(CHINESE); // TODO uew new connection rather than modifying a shared one
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
        assertThat(result).matches(sqlResultDescriptorForResource("io/prestosql/tests/jdbc/get_nation_columns.result"));
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
     * Same as {@code io.prestosql.jdbc.TestJdbcPreparedStatement#testDeallocate()}. This one is run for EnterpriseJdbcDriver as well.
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
        return defaultQueryExecutor().getConnection();
    }
}
