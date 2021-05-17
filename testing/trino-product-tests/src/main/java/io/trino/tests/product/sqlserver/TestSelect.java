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
package io.trino.tests.product.sqlserver;

import io.airlift.log.Logger;
import io.trino.tempto.AfterTestWithContext;
import io.trino.tempto.BeforeTestWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.Requirement;
import io.trino.tempto.RequirementsProvider;
import io.trino.tempto.configuration.Configuration;
import io.trino.tempto.query.QueryResult;
import org.testng.annotations.Test;

import java.sql.Date;
import java.sql.Timestamp;

import static io.trino.tempto.Requirements.compose;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tempto.fulfillment.table.TableRequirements.immutableTable;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.TestGroups.SQL_SERVER;
import static io.trino.tests.product.TpchTableResults.PRESTO_NATION_RESULT;
import static io.trino.tests.product.sqlserver.SqlServerDataTypesTableDefinition.SQLSERVER_ALL_TYPES;
import static io.trino.tests.product.sqlserver.SqlServerTpchTableDefinitions.NATION;
import static io.trino.tests.product.sqlserver.TestConstants.CONNECTOR_NAME;
import static io.trino.tests.product.sqlserver.TestConstants.KEY_SPACE;
import static io.trino.tests.product.utils.QueryExecutors.onSqlServer;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.sql.JDBCType.BIGINT;
import static java.sql.JDBCType.CHAR;
import static java.sql.JDBCType.DATE;
import static java.sql.JDBCType.DOUBLE;
import static java.sql.JDBCType.INTEGER;
import static java.sql.JDBCType.REAL;
import static java.sql.JDBCType.SMALLINT;
import static java.sql.JDBCType.TIMESTAMP;
import static java.sql.JDBCType.TINYINT;
import static java.sql.JDBCType.VARCHAR;
import static java.util.Collections.nCopies;

public class TestSelect
        extends ProductTest
        implements RequirementsProvider
{
    private static final Logger log = Logger.get(TestSelect.class);

    @Override
    public Requirement getRequirements(Configuration configuration)
    {
        return compose(
                immutableTable(NATION),
                immutableTable(SQLSERVER_ALL_TYPES));
    }

    private static final String CTAS_TABLE_NAME = "create_table_as_select";
    private static final String NATION_TABLE_NAME = format("%s.%s.%s", CONNECTOR_NAME, KEY_SPACE, NATION.getName());
    private static final String CREATE_TABLE_AS_SELECT = format("%s.%s.%s", CONNECTOR_NAME, KEY_SPACE, CTAS_TABLE_NAME);
    private static final String ALL_TYPES_TABLE_NAME = format("%s.%s.%s", CONNECTOR_NAME, KEY_SPACE, SQLSERVER_ALL_TYPES.getName());

    @BeforeTestWithContext
    @AfterTestWithContext
    public void dropTestTables()
    {
        try {
            onTrino().executeQuery(format("DROP TABLE IF EXISTS %s", CREATE_TABLE_AS_SELECT));
        }
        catch (Exception e) {
            log.warn(e, "failed to drop table");
        }
    }

    @Test(groups = {SQL_SERVER, PROFILE_SPECIFIC_TESTS})
    public void testSelectNation()
    {
        QueryResult queryResult = onTrino().executeQuery("SELECT n_nationkey, n_name, n_regionkey, n_comment FROM " + NATION_TABLE_NAME);
        assertThat(queryResult).matches(PRESTO_NATION_RESULT);
    }

    @Test(groups = {SQL_SERVER, PROFILE_SPECIFIC_TESTS})
    public void testNationSelfInnerJoin()
    {
        String sql = format(
                "SELECT n1.n_name, n2.n_regionkey FROM %s n1 JOIN " +
                        "%s n2 ON n1.n_nationkey = n2.n_regionkey " +
                        "WHERE n1.n_nationkey=3",
                NATION_TABLE_NAME,
                NATION_TABLE_NAME);
        QueryResult queryResult = onTrino()
                .executeQuery(sql);

        assertThat(queryResult).containsOnly(
                row("CANADA", 3),
                row("CANADA", 3),
                row("CANADA", 3),
                row("CANADA", 3),
                row("CANADA", 3));
    }

    @Test(groups = {SQL_SERVER, PROFILE_SPECIFIC_TESTS})
    public void testNationJoinRegion()
    {
        String sql = format(
                "SELECT c.n_name, t.name FROM %s c JOIN " +
                        "tpch.tiny.region t ON c.n_regionkey = t.regionkey " +
                        "WHERE c.n_nationkey=3",
                NATION_TABLE_NAME);
        QueryResult queryResult = onTrino()
                .executeQuery(sql);

        assertThat(queryResult).containsOnly(row("CANADA", "AMERICA"));
    }

    @Test(groups = {SQL_SERVER, PROFILE_SPECIFIC_TESTS})
    public void testAllDatatypes()
    {
        QueryResult queryResult = onTrino().executeQuery("SELECT * FROM " + ALL_TYPES_TABLE_NAME);
        assertThat(queryResult)
                .hasColumns(
                        BIGINT,
                        SMALLINT,
                        INTEGER,
                        TINYINT,
                        DOUBLE,
                        REAL,
                        CHAR,
                        VARCHAR,
                        VARCHAR,
                        CHAR,
                        VARCHAR,
                        VARCHAR,
                        DATE,
                        TIMESTAMP,
                        TIMESTAMP,
                        TIMESTAMP,
                        TIMESTAMP,
                        DOUBLE,
                        REAL)
                .containsOnly(
                        row(
                                Long.MIN_VALUE,
                                Short.MIN_VALUE,
                                Integer.MIN_VALUE,
                                Byte.MIN_VALUE,
                                Double.MIN_VALUE,
                                -3.40E+38f,
                                "\0   ",
                                "\0",
                                "\0",
                                "\0    ",
                                "\0",
                                "\0",
                                Date.valueOf("1953-01-02"),
                                Timestamp.valueOf("1953-01-01 00:00:00.000"),
                                Timestamp.valueOf("2001-01-01 00:00:00.123"),
                                Timestamp.valueOf("1970-01-01 00:00:00.000"),
                                Timestamp.valueOf("1960-01-01 00:00:00"),
                                Double.MIN_VALUE,
                                -3.40E+38f),
                        row(
                                Long.MAX_VALUE,
                                Short.MAX_VALUE,
                                Integer.MAX_VALUE,
                                Byte.MAX_VALUE,
                                Double.MAX_VALUE,
                                Float.MAX_VALUE,
                                "abcd",
                                "abcdef",
                                "abcd",
                                "abcde",
                                "abcdefg",
                                "abcd",
                                Date.valueOf("9999-12-31"),
                                Timestamp.valueOf("9999-12-31 23:59:59.997"),
                                Timestamp.valueOf("9999-12-31 23:59:59.999"),
                                Timestamp.valueOf("9999-12-31 23:59:59.999"),
                                Timestamp.valueOf("2079-06-06 00:00:00"),
                                12345678912.3456756,
                                12345678.6557f),
                        row(nCopies(19, null).toArray()));
    }

    @Test(groups = {SQL_SERVER, PROFILE_SPECIFIC_TESTS})
    public void testCreateTableAsSelect()
    {
        onTrino().executeQuery(format("CREATE TABLE %s AS SELECT * FROM %s", CREATE_TABLE_AS_SELECT, NATION_TABLE_NAME));

        QueryResult queryResult = onSqlServer()
                .executeQuery(format("SELECT n_nationkey, n_name, n_regionkey, n_comment FROM %s.%s.%s", "master", KEY_SPACE, CTAS_TABLE_NAME));

        assertThat(queryResult).matches(PRESTO_NATION_RESULT);
    }
}
