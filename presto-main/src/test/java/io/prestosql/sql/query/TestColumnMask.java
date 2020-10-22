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
package io.prestosql.sql.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.connector.MockConnectorFactory;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.plugin.tpch.TpchConnectorFactory;
import io.prestosql.spi.connector.ConnectorViewDefinition;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.ViewExpression;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.testing.LocalQueryRunner;
import io.prestosql.testing.TestingAccessControlManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Test(singleThreaded = true)
public class TestColumnMask
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

    @BeforeClass
    public void init()
    {
        LocalQueryRunner runner = LocalQueryRunner.builder(SESSION).build();

        runner.createCatalog(CATALOG, new TpchConnectorFactory(1), ImmutableMap.of());

        ConnectorViewDefinition view = new ConnectorViewDefinition(
                "SELECT nationkey, name FROM local.tiny.nation",
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(new ConnectorViewDefinition.ViewColumn("nationkey", BigintType.BIGINT.getTypeId()), new ConnectorViewDefinition.ViewColumn("name", VarcharType.createVarcharType(25).getTypeId())),
                Optional.empty(),
                Optional.of(VIEW_OWNER),
                false);

        MockConnectorFactory mock = MockConnectorFactory.builder()
                .withGetViews((s, prefix) -> ImmutableMap.<SchemaTableName, ConnectorViewDefinition>builder()
                        .put(new SchemaTableName("default", "nation_view"), view)
                        .build())
                .build();

        runner.createCatalog(MOCK_CATALOG, mock, ImmutableMap.of());

        assertions = new QueryAssertions(runner);
        accessControl = assertions.getQueryRunner().getAccessControl();
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testSimpleMask()
    {
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                "custkey",
                USER,
                new ViewExpression(USER, Optional.empty(), Optional.empty(), "-custkey"));
        assertThat(assertions.query("SELECT custkey FROM orders WHERE orderkey = 1")).matches("VALUES BIGINT '-370'");

        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                "custkey",
                USER,
                new ViewExpression(USER, Optional.empty(), Optional.empty(), "NULL"));
        assertThat(assertions.query("SELECT custkey FROM orders WHERE orderkey = 1")).matches("VALUES CAST(NULL AS BIGINT)");
    }

    @Test
    public void testMultipleMasksOnSameColumn()
    {
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                "custkey",
                USER,
                new ViewExpression(USER, Optional.empty(), Optional.empty(), "-custkey"));

        accessControl.columnMask(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                "custkey",
                USER,
                new ViewExpression(USER, Optional.empty(), Optional.empty(), "custkey * 2"));

        assertThat(assertions.query("SELECT custkey FROM orders WHERE orderkey = 1")).matches("VALUES BIGINT '-740'");
    }

    @Test
    public void testMultipleMasksOnDifferentColumns()
    {
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                "custkey",
                USER,
                new ViewExpression(USER, Optional.empty(), Optional.empty(), "-custkey"));

        accessControl.columnMask(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                "orderstatus",
                USER,
                new ViewExpression(USER, Optional.empty(), Optional.empty(), "'X'"));

        assertThat(assertions.query("SELECT custkey, orderstatus FROM orders WHERE orderkey = 1"))
                .matches("VALUES (BIGINT '-370', 'X')");
    }

    @Test
    public void testReferenceInUsingClause()
    {
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                "orderkey",
                USER,
                new ViewExpression(USER, Optional.empty(), Optional.empty(), "IF(orderkey = 1, -orderkey)"));

        accessControl.columnMask(
                new QualifiedObjectName(CATALOG, "tiny", "lineitem"),
                "orderkey",
                USER,
                new ViewExpression(USER, Optional.empty(), Optional.empty(), "IF(orderkey = 1, -orderkey)"));

        assertThat(assertions.query("SELECT count(*) FROM orders JOIN lineitem USING (orderkey)")).matches("VALUES BIGINT '6'");
    }

    @Test
    public void testCoercibleType()
    {
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                "clerk",
                USER,
                new ViewExpression(USER, Optional.empty(), Optional.empty(), "CAST(clerk AS VARCHAR(5))"));
        assertThat(assertions.query("SELECT clerk FROM orders WHERE orderkey = 1")).matches("VALUES CAST('Clerk' AS VARCHAR(15))");
    }

    @Test
    public void testSubquery()
    {
        // uncorrelated
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                "clerk",
                USER,
                new ViewExpression(USER, Optional.of(CATALOG), Optional.of("tiny"), "(SELECT cast(max(name) AS VARCHAR(15)) FROM nation)"));
        assertThat(assertions.query("SELECT clerk FROM orders WHERE orderkey = 1")).matches("VALUES CAST('VIETNAM' AS VARCHAR(15))");

        // correlated
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                "clerk",
                USER,
                new ViewExpression(USER, Optional.of(CATALOG), Optional.of("tiny"), "(SELECT cast(max(name) AS VARCHAR(15)) FROM nation WHERE nationkey = orderkey)"));
        assertThat(assertions.query("SELECT clerk FROM orders WHERE orderkey = 1")).matches("VALUES CAST('ARGENTINA' AS VARCHAR(15))");
    }

    @Test
    public void testView()
    {
        // mask on the underlying table for view owner when running query as different user
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(CATALOG, "tiny", "nation"),
                "name",
                VIEW_OWNER,
                new ViewExpression(VIEW_OWNER, Optional.empty(), Optional.empty(), "reverse(name)"));

        assertThat(assertions.query(
                Session.builder(SESSION)
                        .setIdentity(Identity.forUser(RUN_AS_USER).build())
                        .build(),
                "SELECT name FROM mock.default.nation_view WHERE nationkey = 1"))
                .matches("VALUES CAST('ANITNEGRA' AS VARCHAR(25))");

        // mask on the underlying table for view owner when running as themselves
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(CATALOG, "tiny", "nation"),
                "name",
                VIEW_OWNER,
                new ViewExpression(VIEW_OWNER, Optional.of(CATALOG), Optional.of("tiny"), "reverse(name)"));

        assertThat(assertions.query(
                Session.builder(SESSION)
                        .setIdentity(Identity.forUser(VIEW_OWNER).build())
                        .build(),
                "SELECT name FROM mock.default.nation_view WHERE nationkey = 1"))
                .matches("VALUES CAST('ANITNEGRA' AS VARCHAR(25))");

        // mask on the underlying table for user running the query (different from view owner) should not be applied
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(CATALOG, "tiny", "nation"),
                "name",
                RUN_AS_USER,
                new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "reverse(name)"));

        assertThat(assertions.query(
                Session.builder(SESSION)
                        .setIdentity(Identity.forUser(RUN_AS_USER).build())
                        .build(),
                "SELECT name FROM mock.default.nation_view WHERE nationkey = 1"))
                .matches("VALUES CAST('ARGENTINA' AS VARCHAR(25))");

        // mask on the view
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(MOCK_CATALOG, "default", "nation_view"),
                "name",
                USER,
                new ViewExpression(USER, Optional.of(CATALOG), Optional.of("tiny"), "reverse(name)"));
        assertThat(assertions.query("SELECT name FROM mock.default.nation_view WHERE nationkey = 1")).matches("VALUES CAST('ANITNEGRA' AS VARCHAR(25))");
    }

    @Test
    public void testTableReferenceInWithClause()
    {
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                "custkey",
                USER,
                new ViewExpression(USER, Optional.empty(), Optional.empty(), "-custkey"));
        assertThat(assertions.query("WITH t AS (SELECT custkey FROM orders WHERE orderkey = 1) SELECT * FROM t")).matches("VALUES BIGINT '-370'");
    }

    @Test
    public void testOtherSchema()
    {
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                "orderkey",
                USER,
                new ViewExpression(USER, Optional.of(CATALOG), Optional.of("sf1"), "(SELECT count(*) FROM customer)")); // count is 15000 only when evaluating against sf1
        assertThat(assertions.query("SELECT max(orderkey) FROM orders")).matches("VALUES BIGINT '150000'");
    }

    @Test
    public void testDifferentIdentity()
    {
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                "orderkey",
                RUN_AS_USER,
                new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "100"));

        accessControl.columnMask(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                "orderkey",
                USER,
                new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "(SELECT sum(orderkey) FROM orders)"));

        assertThat(assertions.query("SELECT max(orderkey) FROM orders")).matches("VALUES BIGINT '1500000'");
    }

    @Test
    public void testRecursion()
    {
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                "orderkey",
                USER,
                new ViewExpression(USER, Optional.of(CATALOG), Optional.of("tiny"), "(SELECT orderkey FROM orders)"));

        assertThatThrownBy(() -> assertions.query("SELECT orderkey FROM orders"))
                .hasMessageMatching(".*\\QColumn mask for 'local.tiny.orders.orderkey' is recursive\\E.*");

        // different reference style to same table
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                "orderkey",
                USER,
                new ViewExpression(USER, Optional.of(CATALOG), Optional.of("tiny"), "(SELECT orderkey FROM local.tiny.orders)"));

        assertThatThrownBy(() -> assertions.query("SELECT orderkey FROM orders"))
                .hasMessageMatching(".*\\QColumn mask for 'local.tiny.orders.orderkey' is recursive\\E.*");

        // mutual recursion
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                "orderkey",
                RUN_AS_USER,
                new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "(SELECT orderkey FROM orders)"));

        accessControl.columnMask(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                "orderkey",
                USER,
                new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "(SELECT orderkey FROM orders)"));

        assertThatThrownBy(() -> assertions.query("SELECT orderkey FROM orders"))
                .hasMessageMatching(".*\\QColumn mask for 'local.tiny.orders.orderkey' is recursive\\E.*");
    }

    @Test
    public void testLimitedScope()
    {
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(CATALOG, "tiny", "customer"),
                "custkey",
                USER,
                new ViewExpression(USER, Optional.of(CATALOG), Optional.of("tiny"), "orderkey"));
        assertThatThrownBy(() -> assertions.query(
                "SELECT (SELECT min(custkey) FROM customer WHERE customer.custkey = orders.custkey) FROM orders"))
                .hasMessageMatching("\\Qline 1:34: Invalid column mask for 'local.tiny.customer.custkey': Column 'orderkey' cannot be resolved\\E");
    }

    @Test
    public void testSqlInjection()
    {
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(CATALOG, "tiny", "nation"),
                "name",
                USER,
                new ViewExpression(USER, Optional.of(CATALOG), Optional.of("tiny"), "(SELECT name FROM region WHERE regionkey = 0)"));
        assertThat(assertions.query(
                "WITH region(regionkey, name) AS (VALUES (0, 'ASIA'))" +
                        "SELECT name FROM nation ORDER BY name LIMIT 1"))
                .matches("VALUES CAST('AFRICA' AS VARCHAR(25))"); // if sql-injection would work then query would return ASIA
    }

    @Test
    public void testInvalidMasks()
    {
        // parse error
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                "orderkey",
                USER,
                new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "$$$"));

        assertThatThrownBy(() -> assertions.query("SELECT orderkey FROM orders"))
                .hasMessageMatching("\\Qline 1:22: Invalid column mask for 'local.tiny.orders.orderkey': mismatched input '$'. Expecting: <expression>\\E");

        // unknown column
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                "orderkey",
                USER,
                new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "unknown_column"));

        assertThatThrownBy(() -> assertions.query("SELECT orderkey FROM orders"))
                .hasMessageMatching("\\Qline 1:22: Invalid column mask for 'local.tiny.orders.orderkey': Column 'unknown_column' cannot be resolved\\E");

        // invalid type
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                "orderkey",
                USER,
                new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "'foo'"));

        assertThatThrownBy(() -> assertions.query("SELECT orderkey FROM orders"))
                .hasMessageMatching("\\Qline 1:22: Expected column mask for 'local.tiny.orders.orderkey' to be of type bigint, but was varchar(3)\\E");

        // aggregation
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                "orderkey",
                USER,
                new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "count(*) > 0"));

        assertThatThrownBy(() -> assertions.query("SELECT orderkey FROM orders"))
                .hasMessageMatching("\\Qline 1:10: Column mask for 'orders.orderkey' cannot contain aggregations, window functions or grouping operations: [count(*)]\\E");

        // window function
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                "orderkey",
                USER,
                new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "row_number() OVER () > 0"));

        assertThatThrownBy(() -> assertions.query("SELECT orderkey FROM orders"))
                .hasMessageMatching("\\Qline 1:22: Column mask for 'orders.orderkey' cannot contain aggregations, window functions or grouping operations: [row_number() OVER ()]\\E");

        // grouping function
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                "orderkey",
                USER,
                new ViewExpression(USER, Optional.of(CATALOG), Optional.of("tiny"), "grouping(orderkey) = 0"));

        assertThatThrownBy(() -> assertions.query("SELECT orderkey FROM orders"))
                .hasMessageMatching("\\Qline 1:20: Column mask for 'orders.orderkey' cannot contain aggregations, window functions or grouping operations: [GROUPING (orderkey)]\\E");
    }

    @Test
    public void testShowStats()
    {
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                "orderkey",
                USER,
                new ViewExpression(USER, Optional.of(CATALOG), Optional.of("tiny"), "7"));

        assertThatThrownBy(() -> assertions.query("SHOW STATS FOR (SELECT * FROM orders)"))
                .hasMessageMatching("\\QSHOW STATS for table with column masking is not supported: orderkey");
        assertThatThrownBy(() -> assertions.query("SHOW STATS FOR (SELECT orderkey FROM orders)"))
                .hasMessageMatching("\\QSHOW STATS for table with column masking is not supported: orderkey");
        assertThat(assertions.query("SHOW STATS FOR (SELECT clerk FROM orders)"))
                .matches("VALUES " +
                        "(cast('clerk' AS varchar), cast(15000 AS double), cast(1000 AS double), cast(0 AS double), cast(null AS double), cast(null AS varchar), cast(null AS varchar))," +
                        "(null, null, null, null, 15000, null, null)");
    }

    @Test
    public void testJoin()
    {
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                "orderkey",
                USER,
                new ViewExpression(USER, Optional.of(CATALOG), Optional.of("tiny"), "orderkey + 1"));
        assertThat(assertions.query("SELECT count(*) FROM orders JOIN orders USING (orderkey)")).matches("VALUES BIGINT '15000'");

        // multiple masks
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                "orderkey",
                USER,
                new ViewExpression(USER, Optional.empty(), Optional.empty(), "-orderkey"));

        accessControl.columnMask(
                new QualifiedObjectName(CATALOG, "tiny", "orders"),
                "orderkey",
                USER,
                new ViewExpression(USER, Optional.empty(), Optional.empty(), "orderkey * 2"));

        assertThat(assertions.query("SELECT count(*) FROM orders JOIN orders USING (orderkey)")).matches("VALUES BIGINT '15000'");
    }
}
