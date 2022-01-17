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
package io.trino.sql.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.Identity;
import io.trino.spi.security.ViewExpression;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.VarcharType;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.TestingAccessControlManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Optional;

import static io.trino.connector.MockConnectorEntities.TPCH_NATION_DATA;
import static io.trino.connector.MockConnectorEntities.TPCH_NATION_SCHEMA;
import static io.trino.connector.MockConnectorEntities.TPCH_NATION_WITH_HIDDEN_COLUMN;
import static io.trino.connector.MockConnectorEntities.TPCH_WITH_HIDDEN_COLUMN_DATA;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestRowFilter
{
    private static final String CATALOG = "local";
    private static final String MOCK_CATALOG = "mock";
    private static final String USER = "user";
    private static final String VIEW_OWNER = "view-owner";
    private static final String RUN_AS_USER = "run-as-user";

    private static final Session SESSION = testSessionBuilder()
            .setCatalog(CATALOG)
            .setSchema(TINY_SCHEMA_NAME)
            .setIdentity(Identity.forUser(USER).build())
            .build();

    private QueryAssertions assertions;
    private TestingAccessControlManager accessControl;

    @BeforeAll
    public void init()
    {
        LocalQueryRunner runner = LocalQueryRunner.builder(SESSION).build();

        runner.createCatalog(CATALOG, new TpchConnectorFactory(1), ImmutableMap.of());

        ConnectorViewDefinition view = new ConnectorViewDefinition(
                "SELECT nationkey, name FROM local.tiny.nation",
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(new ConnectorViewDefinition.ViewColumn("nationkey", BigintType.BIGINT.getTypeId()), new ConnectorViewDefinition.ViewColumn("name", VarcharType.createVarcharType(25)
                        .getTypeId())),
                Optional.empty(),
                Optional.of(VIEW_OWNER),
                false);

        MockConnectorFactory mock = MockConnectorFactory.builder()
                .withGetViews((s, prefix) -> ImmutableMap.<SchemaTableName, ConnectorViewDefinition>builder()
                        .put(new SchemaTableName("default", "nation_view"), view)
                        .buildOrThrow())
                .withGetColumns(schemaTableName -> {
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation"))) {
                        return TPCH_NATION_SCHEMA;
                    }
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_with_hidden_column"))) {
                        return TPCH_NATION_WITH_HIDDEN_COLUMN;
                    }
                    throw new UnsupportedOperationException();
                })
                .withData(schemaTableName -> {
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation"))) {
                        return TPCH_NATION_DATA;
                    }
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_with_hidden_column"))) {
                        return TPCH_WITH_HIDDEN_COLUMN_DATA;
                    }
                    throw new UnsupportedOperationException();
                })
                .build();

        runner.createCatalog(MOCK_CATALOG, mock, ImmutableMap.of());

        assertions = new QueryAssertions(runner);
        accessControl = assertions.getQueryRunner().getAccessControl();
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testSimpleFilter()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                USER,
                new ViewExpression(USER, Optional.empty(), Optional.empty(), "orderkey < 10"));
        assertThat(assertions.query("SELECT count(*) FROM orders")).matches("VALUES BIGINT '7'");

        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                USER,
                new ViewExpression(USER, Optional.empty(), Optional.empty(), "NULL"));
        assertThat(assertions.query("SELECT count(*) FROM orders")).matches("VALUES BIGINT '0'");
    }

    @Test
    public void testMultipleFilters()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                USER,
                new ViewExpression(USER, Optional.empty(), Optional.empty(), "orderkey < 10"));

        accessControl.rowFilter(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                USER,
                new ViewExpression(USER, Optional.empty(), Optional.empty(), "orderkey > 5"));

        assertThat(assertions.query("SELECT count(*) FROM orders")).matches("VALUES BIGINT '2'");
    }

    @Test
    public void testCorrelatedSubquery()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                USER,
                new ViewExpression(USER, Optional.of(CATALOG), Optional.of("tiny"), "EXISTS (SELECT 1 FROM nation WHERE nationkey = orderkey)"));
        assertThat(assertions.query("SELECT count(*) FROM orders")).matches("VALUES BIGINT '7'");
    }

    @Test
    public void testView()
    {
        // filter on the underlying table for view owner when running query as different user
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(CATALOG, "tiny", "nation"),
                VIEW_OWNER,
                new ViewExpression(VIEW_OWNER, Optional.empty(), Optional.empty(), "nationkey = 1"));

        assertThat(assertions.query(
                Session.builder(SESSION)
                        .setIdentity(Identity.forUser(RUN_AS_USER).build())
                        .build(),
                "SELECT name FROM mock.default.nation_view"))
                .matches("VALUES CAST('ARGENTINA' AS VARCHAR(25))");

        // filter on the underlying table for view owner when running as themselves
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(CATALOG, "tiny", "nation"),
                VIEW_OWNER,
                new ViewExpression(VIEW_OWNER, Optional.of(CATALOG), Optional.of("tiny"), "nationkey = 1"));

        assertThat(assertions.query(
                Session.builder(SESSION)
                        .setIdentity(Identity.forUser(VIEW_OWNER).build())
                        .build(),
                "SELECT name FROM mock.default.nation_view"))
                .matches("VALUES CAST('ARGENTINA' AS VARCHAR(25))");

        // filter on the underlying table for user running the query (different from view owner) should not be applied
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(CATALOG, "tiny", "nation"),
                RUN_AS_USER,
                new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "nationkey = 1"));

        Session session = Session.builder(SESSION)
                .setIdentity(Identity.forUser(RUN_AS_USER).build())
                .build();

        assertThat(assertions.query(session, "SELECT count(*) FROM mock.default.nation_view")).matches("VALUES BIGINT '25'");

        // filter on the view
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(MOCK_CATALOG, "default", "nation_view"),
                USER,
                new ViewExpression(USER, Optional.of(CATALOG), Optional.of("tiny"), "nationkey = 1"));
        assertThat(assertions.query("SELECT name FROM mock.default.nation_view")).matches("VALUES CAST('ARGENTINA' AS VARCHAR(25))");
    }

    @Test
    public void testTableReferenceInWithClause()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                USER,
                new ViewExpression(USER, Optional.empty(), Optional.empty(), "orderkey = 1"));
        assertThat(assertions.query("WITH t AS (SELECT count(*) FROM orders) SELECT * FROM t")).matches("VALUES BIGINT '1'");
    }

    @Test
    public void testOtherSchema()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                USER,
                new ViewExpression(USER, Optional.of(CATALOG), Optional.of("sf1"), "(SELECT count(*) FROM customer) = 150000")); // Filter is TRUE only if evaluating against sf1.customer
        assertThat(assertions.query("SELECT count(*) FROM orders")).matches("VALUES BIGINT '15000'");
    }

    @Test
    public void testDifferentIdentity()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                RUN_AS_USER,
                new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "orderkey = 1"));

        accessControl.rowFilter(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                USER,
                new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "orderkey IN (SELECT orderkey FROM orders)"));

        assertThat(assertions.query("SELECT count(*) FROM orders")).matches("VALUES BIGINT '1'");
    }

    @Test
    public void testRecursion()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                USER,
                new ViewExpression(USER, Optional.of(CATALOG), Optional.of("tiny"), "orderkey IN (SELECT orderkey FROM orders)"));

        assertThatThrownBy(() -> assertions.query("SELECT count(*) FROM orders"))
                .hasMessageMatching(".*\\QRow filter for 'local.tiny.orders' is recursive\\E.*");

        // different reference style to same table
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                USER,
                new ViewExpression(USER, Optional.of(CATALOG), Optional.of("tiny"), "orderkey IN (SELECT local.tiny.orderkey FROM orders)"));
        assertThatThrownBy(() -> assertions.query("SELECT count(*) FROM orders"))
                .hasMessageMatching(".*\\QRow filter for 'local.tiny.orders' is recursive\\E.*");

        // mutual recursion
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                RUN_AS_USER,
                new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "orderkey IN (SELECT orderkey FROM orders)"));

        accessControl.rowFilter(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                USER,
                new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "orderkey IN (SELECT orderkey FROM orders)"));

        assertThatThrownBy(() -> assertions.query("SELECT count(*) FROM orders"))
                .hasMessageMatching(".*\\QRow filter for 'local.tiny.orders' is recursive\\E.*");
    }

    @Test
    public void testLimitedScope()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(CATALOG, "tiny", "customer"),
                USER,
                new ViewExpression(USER, Optional.of(CATALOG), Optional.of("tiny"), "orderkey = 1"));
        assertThatThrownBy(() -> assertions.query(
                "SELECT (SELECT min(name) FROM customer WHERE customer.custkey = orders.custkey) FROM orders"))
                .hasMessage("line 1:31: Invalid row filter for 'local.tiny.customer': Column 'orderkey' cannot be resolved");
    }

    @Test
    public void testSqlInjection()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(CATALOG, "tiny", "nation"),
                USER,
                new ViewExpression(USER, Optional.of(CATALOG), Optional.of("tiny"), "regionkey IN (SELECT regionkey FROM region WHERE name = 'ASIA')"));
        assertThat(assertions.query(
                "WITH region(regionkey, name) AS (VALUES (0, 'ASIA'), (1, 'ASIA'), (2, 'ASIA'), (3, 'ASIA'), (4, 'ASIA'))" +
                        "SELECT name FROM nation ORDER BY name LIMIT 1"))
                .matches("VALUES CAST('CHINA' AS VARCHAR(25))"); // if sql-injection would work then query would return ALGERIA
    }

    @Test
    public void testInvalidFilter()
    {
        // parse error
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                USER,
                new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "$$$"));

        assertThatThrownBy(() -> assertions.query("SELECT count(*) FROM orders"))
                .hasMessage("line 1:22: Invalid row filter for 'local.tiny.orders': mismatched input '$'. Expecting: <expression>");

        // unknown column
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                USER,
                new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "unknown_column"));

        assertThatThrownBy(() -> assertions.query("SELECT count(*) FROM orders"))
                .hasMessage("line 1:22: Invalid row filter for 'local.tiny.orders': Column 'unknown_column' cannot be resolved");

        // invalid type
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                USER,
                new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "1"));

        assertThatThrownBy(() -> assertions.query("SELECT count(*) FROM orders"))
                .hasMessage("line 1:22: Expected row filter for 'local.tiny.orders' to be of type BOOLEAN, but was integer");

        // aggregation
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                USER,
                new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "count(*) > 0"));

        assertThatThrownBy(() -> assertions.query("SELECT count(*) FROM orders"))
                .hasMessage("line 1:10: Row filter for 'local.tiny.orders' cannot contain aggregations, window functions or grouping operations: [count(*)]");

        // window function
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                USER,
                new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "row_number() OVER () > 0"));

        assertThatThrownBy(() -> assertions.query("SELECT count(*) FROM orders"))
                .hasMessage("line 1:22: Row filter for 'local.tiny.orders' cannot contain aggregations, window functions or grouping operations: [row_number() OVER ()]");

        // window function
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                USER,
                new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "grouping(orderkey) = 0"));

        assertThatThrownBy(() -> assertions.query("SELECT count(*) FROM orders"))
                .hasMessage("line 1:20: Row filter for 'local.tiny.orders' cannot contain aggregations, window functions or grouping operations: [GROUPING (orderkey)]");
    }

    @Test
    public void testShowStats()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                USER,
                new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "orderkey = 0"));

        assertThat(assertions.query("SHOW STATS FOR (SELECT * FROM tiny.orders)"))
                .containsAll(
                        "VALUES " +
                                "(VARCHAR 'orderkey', 0e1, 0e1, 1e0, CAST(NULL AS double), CAST(NULL AS varchar), CAST(NULL AS varchar))," +
                                "(VARCHAR 'custkey', 0e1, 0e1, 1e0, CAST(NULL AS double), CAST(NULL AS varchar), CAST(NULL AS varchar))," +
                                "(NULL, NULL, NULL, NULL, 0e1, NULL, NULL)");
    }

    @Test
    public void testDelete()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(MOCK_CATALOG, "tiny", "nation"),
                USER,
                new ViewExpression(USER, Optional.empty(), Optional.empty(), "nationkey < 10"));

        // Within allowed row filter
        assertions.query("DELETE FROM mock.tiny.nation WHERE nationkey < 3")
                .assertThat()
                .matches("SELECT BIGINT '3'");
        assertions.query("DELETE FROM mock.tiny.nation WHERE nationkey IN (1, 2, 3)")
                .assertThat()
                .matches("SELECT BIGINT '3'");

        // Outside allowed row filter, only readable rows were dropped
        assertions.query("DELETE FROM mock.tiny.nation")
                .assertThat()
                .matches("SELECT BIGINT '10'");
        assertions.query("DELETE FROM mock.tiny.nation WHERE nationkey IN (1, 11)")
                .assertThat()
                .matches("SELECT BIGINT '1'");
        assertions.query("DELETE FROM mock.tiny.nation WHERE nationkey >= 10")
                .assertThat()
                .matches("SELECT BIGINT '0'");
    }

    @Test
    public void testUpdate()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(MOCK_CATALOG, "tiny", "nation"),
                USER,
                new ViewExpression(USER, Optional.empty(), Optional.empty(), "nationkey < 10"));

        // Within allowed row filter
        assertThatThrownBy(() -> assertions.query("UPDATE mock.tiny.nation SET regionkey = regionkey * 2 WHERE nationkey < 3"))
                .hasMessage("line 1:1: Updating a table with a row filter is not supported");
        assertThatThrownBy(() -> assertions.query("UPDATE mock.tiny.nation SET regionkey = regionkey * 2 WHERE nationkey IN (1, 2, 3)"))
                .hasMessage("line 1:1: Updating a table with a row filter is not supported");

        // Outside allowed row filter, only readable rows were update
        assertThatThrownBy(() -> assertions.query("UPDATE mock.tiny.nation SET regionkey = regionkey * 2"))
                .hasMessage("line 1:1: Updating a table with a row filter is not supported");
        assertThatThrownBy(() -> assertions.query("UPDATE mock.tiny.nation SET regionkey = regionkey * 2 WHERE nationkey IN (1, 11)"))
                .hasMessage("line 1:1: Updating a table with a row filter is not supported");

        assertThatThrownBy(() -> assertions.query("UPDATE mock.tiny.nation SET regionkey = regionkey * 2 WHERE nationkey = 11"))
                .hasMessage("line 1:1: Updating a table with a row filter is not supported");

        // Within allowed row filter, but updated rows are outside the row filter
        assertThatThrownBy(() -> assertions.query("UPDATE mock.tiny.nation SET nationkey = 10 WHERE nationkey < 3"))
                .hasMessage("line 1:1: Updating a table with a row filter is not supported");
        assertThatThrownBy(() -> assertions.query("UPDATE mock.tiny.nation SET nationkey = null WHERE nationkey < 3"))
                .hasMessage("line 1:1: Updating a table with a row filter is not supported");

        // Outside allowed row filter, but updated rows are outside the row filter
        assertThatThrownBy(() -> assertions.query("UPDATE mock.tiny.nation SET nationkey = 10 WHERE nationkey = 10"))
                .hasMessage("line 1:1: Updating a table with a row filter is not supported");
        assertThatThrownBy(() -> assertions.query("UPDATE mock.tiny.nation SET nationkey = null WHERE nationkey = null "))
                .hasMessage("line 1:1: Updating a table with a row filter is not supported");
    }

    @Test
    public void testInsert()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(MOCK_CATALOG, "tiny", "nation"),
                USER,
                new ViewExpression(USER, Optional.empty(), Optional.empty(), "nationkey > 100"));

        // Within allowed row filter
        assertions.query("INSERT INTO mock.tiny.nation VALUES (101, 'POLAND', 0, 'No comment')")
                .assertThat()
                .skippingTypesCheck()
                .matches("SELECT BIGINT '1'");

        // Outside allowed row filter
        assertThatThrownBy(() -> assertions.query("INSERT INTO mock.tiny.nation VALUES (26, 'POLAND', 0, 'No comment')"))
                .hasMessage("Access Denied: Cannot insert row that does not match to a row filter");
        assertThatThrownBy(() -> assertions.query("INSERT INTO mock.tiny.nation VALUES "
                + "(26, 'POLAND', 0, 'No comment'),"
                + "(27, 'HOLLAND', 0, 'A comment')"))
                .hasMessage("Access Denied: Cannot insert row that does not match to a row filter");
        assertThatThrownBy(() -> assertions.query("INSERT INTO mock.tiny.nation VALUES "
                + "(26, 'POLAND', 0, 'No comment'),"
                + "(27, 'HOLLAND', 0, 'A comment')"))
                .hasMessage("Access Denied: Cannot insert row that does not match to a row filter");
        assertThatThrownBy(() -> assertions.query("INSERT INTO mock.tiny.nation(nationkey) VALUES (null)"))
                .hasMessage("Access Denied: Cannot insert row that does not match to a row filter");
        assertThatThrownBy(() -> assertions.query("INSERT INTO mock.tiny.nation(regionkey) VALUES (0)"))
                .hasMessage("Access Denied: Cannot insert row that does not match to a row filter");
    }

    @Test
    public void testRowFilterWithHiddenColumns()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(MOCK_CATALOG, "tiny", "nation_with_hidden_column"),
                USER,
                new ViewExpression(USER, Optional.empty(), Optional.empty(), "nationkey < 1"));

        assertions.query("SELECT * FROM mock.tiny.nation_with_hidden_column")
                .assertThat()
                .skippingTypesCheck()
                .matches("VALUES (BIGINT '0', 'ALGERIA', BIGINT '0', ' haggle. carefully final deposits detect slyly agai')");
        assertThatThrownBy(() -> assertions.query("INSERT INTO mock.tiny.nation_with_hidden_column VALUES (101, 'POLAND', 0, 'No comment')"))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Access Denied: Cannot insert row that does not match to a row filter");
        assertions.query("INSERT INTO mock.tiny.nation_with_hidden_column VALUES (0, 'POLAND', 0, 'No comment')")
                .assertThat()
                .skippingTypesCheck()
                .matches("VALUES BIGINT '1'");
        assertThatThrownBy(() -> assertions.query("UPDATE mock.tiny.nation_with_hidden_column SET name = 'POLAND'"))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Updating a table with a row filter is not supported");
        assertions.query("DELETE FROM mock.tiny.nation_with_hidden_column WHERE regionkey < 5")
                .assertThat()
                .skippingTypesCheck()
                .matches("SELECT BIGINT '1'");
        assertions.query("DELETE FROM mock.tiny.nation_with_hidden_column WHERE \"$hidden\" IS NOT NULL")
                .assertThat()
                .skippingTypesCheck()
                .matches("SELECT BIGINT '1'");
    }

    @Test
    public void testRowFilterOnHiddenColumn()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(MOCK_CATALOG, "tiny", "nation_with_hidden_column"),
                USER,
                new ViewExpression(USER, Optional.empty(), Optional.empty(), "\"$hidden\" < 1"));

        assertions.query("SELECT count(*) FROM mock.tiny.nation_with_hidden_column")
                .assertThat()
                .skippingTypesCheck()
                .matches("VALUES BIGINT '25'");
        // TODO https://github.com/trinodb/trino/issues/10006 - support insert into a table with row filter that is using hidden columns
        assertThatThrownBy(() -> assertions.query("INSERT INTO mock.tiny.nation_with_hidden_column VALUES (101, 'POLAND', 0, 'No comment')"))
                .isInstanceOf(ArrayIndexOutOfBoundsException.class)
                .hasMessage("Index 4 out of bounds for length 4");
        assertThatThrownBy(() -> assertions.query("UPDATE mock.tiny.nation_with_hidden_column SET name = 'POLAND'"))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Updating a table with a row filter is not supported");
        assertions.query("DELETE FROM mock.tiny.nation_with_hidden_column WHERE regionkey < 5")
                .assertThat()
                .skippingTypesCheck()
                .matches("SELECT BIGINT '25'");
    }
}
