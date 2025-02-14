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
import io.trino.connector.MockConnectorPlugin;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.Identity;
import io.trino.spi.security.ViewExpression;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.VarcharType;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import io.trino.testing.TestingAccessControlManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.time.Duration;
import java.util.Optional;

import static io.trino.connector.MockConnectorEntities.TPCH_NATION_WITH_HIDDEN_COLUMN;
import static io.trino.connector.MockConnectorEntities.TPCH_WITH_HIDDEN_COLUMN_DATA;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
public class TestColumnMask
{
    private static final String LOCAL_CATALOG = "local";
    private static final String MOCK_CATALOG = "mock";
    private static final String USER = "user";
    private static final String VIEW_OWNER = "view-owner";
    private static final String RUN_AS_USER = "run-as-user";

    private static final Session SESSION = testSessionBuilder()
            .setCatalog(LOCAL_CATALOG)
            .setSchema(TINY_SCHEMA_NAME)
            .setIdentity(Identity.forUser(USER).build())
            .build();

    private final QueryAssertions assertions;
    private final TestingAccessControlManager accessControl;

    public TestColumnMask()
    {
        QueryRunner runner = new StandaloneQueryRunner(SESSION);
        runner.installPlugin(new TpchPlugin());
        runner.createCatalog(LOCAL_CATALOG, "tpch", ImmutableMap.of("tpch.splits-per-node", "1"));

        ConnectorViewDefinition view = new ConnectorViewDefinition(
                "SELECT nationkey, name FROM local.tiny.nation",
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(
                        new ConnectorViewDefinition.ViewColumn("nationkey", BigintType.BIGINT.getTypeId(), Optional.empty()),
                        new ConnectorViewDefinition.ViewColumn("name", VarcharType.createVarcharType(25).getTypeId(), Optional.empty())),
                Optional.empty(),
                Optional.of(VIEW_OWNER),
                false,
                ImmutableList.of());

        ConnectorViewDefinition viewWithNested = new ConnectorViewDefinition(
                """
                SELECT * FROM (
                    VALUES
                        ROW(ROW(1,2), 0),
                        ROW(ROW(3,4), 1)
                ) t(nested, id)
                """,
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(
                        new ConnectorViewDefinition.ViewColumn("nested", RowType.from(ImmutableList.of(
                                RowType.field(INTEGER),
                                RowType.field(INTEGER))).getTypeId(),
                                Optional.empty()),
                        new ConnectorViewDefinition.ViewColumn("id", INTEGER.getTypeId(), Optional.empty())),
                Optional.empty(),
                Optional.of(VIEW_OWNER),
                false,
                ImmutableList.of());

        ConnectorMaterializedViewDefinition materializedView = new ConnectorMaterializedViewDefinition(
                "SELECT * FROM local.tiny.nation",
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(
                        new ConnectorMaterializedViewDefinition.Column("nationkey", BigintType.BIGINT.getTypeId(), Optional.empty()),
                        new ConnectorMaterializedViewDefinition.Column("name", VarcharType.createVarcharType(25).getTypeId(), Optional.empty()),
                        new ConnectorMaterializedViewDefinition.Column("regionkey", BigintType.BIGINT.getTypeId(), Optional.empty()),
                        new ConnectorMaterializedViewDefinition.Column("comment", VarcharType.createVarcharType(152).getTypeId(), Optional.empty())),
                Optional.of(Duration.ZERO),
                Optional.empty(),
                Optional.of(VIEW_OWNER),
                ImmutableList.of());

        ConnectorMaterializedViewDefinition freshMaterializedView = new ConnectorMaterializedViewDefinition(
                "SELECT * FROM local.tiny.nation",
                Optional.of(new CatalogSchemaTableName("local", "tiny", "nation")),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(
                        new ConnectorMaterializedViewDefinition.Column("nationkey", BigintType.BIGINT.getTypeId(), Optional.empty()),
                        new ConnectorMaterializedViewDefinition.Column("name", VarcharType.createVarcharType(25).getTypeId(), Optional.empty()),
                        new ConnectorMaterializedViewDefinition.Column("regionkey", BigintType.BIGINT.getTypeId(), Optional.empty()),
                        new ConnectorMaterializedViewDefinition.Column("comment", VarcharType.createVarcharType(152).getTypeId(), Optional.empty())),
                Optional.of(Duration.ZERO),
                Optional.empty(),
                Optional.of(VIEW_OWNER),
                ImmutableList.of());

        ConnectorMaterializedViewDefinition materializedViewWithCasts = new ConnectorMaterializedViewDefinition(
                "SELECT nationkey, cast(name as varchar(1)) as name, regionkey, comment FROM local.tiny.nation",
                Optional.of(new CatalogSchemaTableName("local", "tiny", "nation")),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(
                        new ConnectorMaterializedViewDefinition.Column("nationkey", BigintType.BIGINT.getTypeId(), Optional.empty()),
                        new ConnectorMaterializedViewDefinition.Column("name", VarcharType.createVarcharType(2).getTypeId(), Optional.empty()),
                        new ConnectorMaterializedViewDefinition.Column("regionkey", BigintType.BIGINT.getTypeId(), Optional.empty()),
                        new ConnectorMaterializedViewDefinition.Column("comment", VarcharType.createVarcharType(152).getTypeId(), Optional.empty())),
                Optional.of(Duration.ZERO),
                Optional.empty(),
                Optional.of(VIEW_OWNER),
                ImmutableList.of());

        runner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withGetColumns(schemaTableName -> {
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_with_hidden_column"))) {
                        return TPCH_NATION_WITH_HIDDEN_COLUMN;
                    }
                    throw new UnsupportedOperationException();
                })
                .withData(schemaTableName -> {
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_with_hidden_column"))) {
                        return TPCH_WITH_HIDDEN_COLUMN_DATA;
                    }
                    throw new UnsupportedOperationException();
                })
                .withGetViews((s, prefix) -> ImmutableMap.of(
                        new SchemaTableName("default", "nation_view"), view,
                        new SchemaTableName("default", "view_with_nested"), viewWithNested))
                .withGetMaterializedViews((s, prefix) -> ImmutableMap.of(
                        new SchemaTableName("default", "nation_materialized_view"), materializedView,
                        new SchemaTableName("default", "nation_fresh_materialized_view"), freshMaterializedView,
                        new SchemaTableName("default", "materialized_view_with_casts"), materializedViewWithCasts))
                .build()));
        runner.createCatalog(MOCK_CATALOG, "mock", ImmutableMap.of());

        assertions = new QueryAssertions(runner);
        accessControl = assertions.getQueryRunner().getAccessControl();
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testSimpleMask()
    {
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                "custkey",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("-custkey")
                        .build());
        assertThat(assertions.query("SELECT custkey FROM orders WHERE orderkey = 1")).matches("VALUES BIGINT '-370'");

        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                "custkey",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("NULL")
                        .build());
        assertThat(assertions.query("SELECT custkey FROM orders WHERE orderkey = 1")).matches("VALUES CAST(NULL AS BIGINT)");
    }

    @Test
    public void testConditionalMask()
    {
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                "custkey",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("IF (orderkey < 2, null, -custkey)")
                        .build());
        assertThat(assertions.query("SELECT custkey FROM orders LIMIT 2"))
                .matches("VALUES (NULL), CAST('-781' AS BIGINT)");
    }

    @Test
    public void testMultipleMasksOnDifferentColumns()
    {
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                "custkey",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("-custkey").build());

        accessControl.columnMask(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                "orderstatus",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("'X'")
                        .build());

        assertThat(assertions.query("SELECT custkey, orderstatus FROM orders WHERE orderkey = 1"))
                .matches("VALUES (BIGINT '-370', 'X')");
    }

    @Test
    public void testReferenceInUsingClause()
    {
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                "orderkey",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("IF(orderkey = 1, -orderkey)")
                        .build());

        accessControl.columnMask(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "lineitem"),
                "orderkey",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("IF(orderkey = 1, -orderkey)")
                        .build());

        assertThat(assertions.query("SELECT count(*) FROM orders JOIN lineitem USING (orderkey)")).matches("VALUES BIGINT '6'");
    }

    @Test
    public void testCoercibleType()
    {
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                "clerk",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("CAST(clerk AS VARCHAR(5))")
                        .build());
        assertThat(assertions.query("SELECT clerk FROM orders WHERE orderkey = 1")).matches("VALUES CAST('Clerk' AS VARCHAR(15))");
    }

    @Test
    public void testSubquery()
    {
        // uncorrelated
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                "clerk",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("(SELECT cast(max(name) AS VARCHAR(15)) FROM nation)")
                        .build());
        assertThat(assertions.query("SELECT clerk FROM orders WHERE orderkey = 1")).matches("VALUES CAST('VIETNAM' AS VARCHAR(15))");

        // correlated
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                "clerk",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("(SELECT cast(max(name) AS VARCHAR(15)) FROM nation WHERE nationkey = orderkey)")
                        .build());
        assertThat(assertions.query("SELECT clerk FROM orders WHERE orderkey = 1")).matches("VALUES CAST('ARGENTINA' AS VARCHAR(15))");
    }

    @Test
    public void testMaterializedView()
    {
        // mask materialized view columns
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(MOCK_CATALOG, "default", "nation_fresh_materialized_view"),
                "name",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("reverse(name)")
                        .build());
        accessControl.columnMask(
                new QualifiedObjectName(MOCK_CATALOG, "default", "nation_materialized_view"),
                "name",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("reverse(name)")
                        .build());
        accessControl.columnMask(
                new QualifiedObjectName(MOCK_CATALOG, "default", "materialized_view_with_casts"),
                "name",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("reverse(name)")
                        .build());

        assertThat(assertions.query(
                Session.builder(SESSION)
                        .setIdentity(Identity.forUser(USER).build())
                        .build(),
                "SELECT name FROM mock.default.nation_fresh_materialized_view WHERE nationkey = 1"))
                .matches("VALUES CAST('ANITNEGRA' AS VARCHAR(25))");

        assertThat(assertions.query(
                Session.builder(SESSION)
                        .setIdentity(Identity.forUser(USER).build())
                        .build(),
                "SELECT name FROM mock.default.nation_materialized_view WHERE nationkey = 1"))
                .matches("VALUES CAST('ANITNEGRA' AS VARCHAR(25))");

        assertThat(assertions.query(
                Session.builder(SESSION)
                        .setIdentity(Identity.forUser(USER).build())
                        .build(),
                "SELECT name FROM mock.default.materialized_view_with_casts WHERE nationkey = 1"))
                .matches("VALUES 'RA'");
    }

    @Test
    public void testView()
    {
        // mask on the underlying table for view owner when running query as different user
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "nation"),
                "name",
                VIEW_OWNER,
                ViewExpression.builder()
                        .identity(VIEW_OWNER)
                        .expression("reverse(name)")
                        .build());

        assertThat(assertions.query(
                Session.builder(SESSION)
                        .setIdentity(Identity.forUser(RUN_AS_USER).build())
                        .build(),
                "SELECT name FROM mock.default.nation_view WHERE nationkey = 1"))
                .matches("VALUES CAST('ANITNEGRA' AS VARCHAR(25))");

        // mask on the underlying table for view owner when running as themselves
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "nation"),
                "name",
                VIEW_OWNER,
                ViewExpression.builder()
                        .identity(VIEW_OWNER)
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("reverse(name)")
                        .build());

        assertThat(assertions.query(
                Session.builder(SESSION)
                        .setIdentity(Identity.forUser(VIEW_OWNER).build())
                        .build(),
                "SELECT name FROM mock.default.nation_view WHERE nationkey = 1"))
                .matches("VALUES CAST('ANITNEGRA' AS VARCHAR(25))");

        // mask on the underlying table for user running the query (different from view owner) should not be applied
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "nation"),
                "name",
                RUN_AS_USER,
                ViewExpression.builder()
                        .identity(RUN_AS_USER)
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("reverse(name)")
                        .build());

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
                ViewExpression.builder()
                        .identity(USER)
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("reverse(name)")
                        .build());
        assertThat(assertions.query("SELECT name FROM mock.default.nation_view WHERE nationkey = 1")).matches("VALUES CAST('ANITNEGRA' AS VARCHAR(25))");
    }

    @Test
    public void testTableReferenceInWithClause()
    {
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                "custkey",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("-custkey")
                        .build());
        assertThat(assertions.query("WITH t AS (SELECT custkey FROM orders WHERE orderkey = 1) SELECT * FROM t")).matches("VALUES BIGINT '-370'");
    }

    @Test
    public void testOtherSchema()
    {
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                "orderkey",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .catalog(LOCAL_CATALOG)
                        .schema("sf1")  // count is 15000 only when evaluating against sf1
                        .expression("(SELECT count(*) FROM customer)")
                        .build());
        assertThat(assertions.query("SELECT max(orderkey) FROM orders")).matches("VALUES BIGINT '150000'");
    }

    @Test
    public void testDifferentIdentity()
    {
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                "orderkey",
                RUN_AS_USER,
                ViewExpression.builder()
                        .identity(RUN_AS_USER)
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("100")
                        .build());

        accessControl.columnMask(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                "orderkey",
                USER,
                ViewExpression.builder()
                        .identity(RUN_AS_USER)
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("(SELECT sum(orderkey) FROM orders)")
                        .build());

        assertThat(assertions.query("SELECT max(orderkey) FROM orders")).matches("VALUES BIGINT '1500000'");
    }

    @Test
    public void testRecursion()
    {
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                "orderkey",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("(SELECT orderkey FROM orders)")
                        .build());

        assertThat(assertions.query("SELECT orderkey FROM orders"))
                .failure().hasMessageMatching(".*\\QColumn mask for 'local.tiny.orders.orderkey' is recursive\\E.*");

        // different reference style to same table
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                "orderkey",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("(SELECT orderkey FROM local.tiny.orders)")
                        .build());

        assertThat(assertions.query("SELECT orderkey FROM orders"))
                .failure().hasMessageMatching(".*\\QColumn mask for 'local.tiny.orders.orderkey' is recursive\\E.*");

        // mutual recursion
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                "orderkey",
                RUN_AS_USER,
                ViewExpression.builder()
                        .identity(RUN_AS_USER)
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("(SELECT orderkey FROM orders)")
                        .build());

        accessControl.columnMask(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                "orderkey",
                USER,
                ViewExpression.builder()
                        .identity(RUN_AS_USER)
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("(SELECT orderkey FROM orders)")
                        .build());

        assertThat(assertions.query("SELECT orderkey FROM orders"))
                .failure().hasMessageMatching(".*\\QColumn mask for 'local.tiny.orders.orderkey' is recursive\\E.*");
    }

    @Test
    public void testLimitedScope()
    {
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "customer"),
                "custkey",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("orderkey")
                        .build());
        assertThat(assertions.query(
                "SELECT (SELECT min(custkey) FROM customer WHERE customer.custkey = orders.custkey) FROM orders"))
                .failure().hasMessage("line 1:34: Invalid column mask for 'local.tiny.customer.custkey': Column 'orderkey' cannot be resolved");
    }

    @Test
    public void testSqlInjection()
    {
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "nation"),
                "name",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("(SELECT name FROM region WHERE regionkey = 0)")
                        .build());
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
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                "orderkey",
                USER,
                ViewExpression.builder()
                        .identity(RUN_AS_USER)
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("$$$")
                        .build());

        assertThat(assertions.query("SELECT orderkey FROM orders"))
                .failure().hasMessage("line 1:22: Invalid column mask for 'local.tiny.orders.orderkey': mismatched input '$'. Expecting: <expression>");

        // unknown column
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                "orderkey",
                USER,
                ViewExpression.builder()
                        .identity(RUN_AS_USER)
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("unknown_column")
                        .build());

        assertThat(assertions.query("SELECT orderkey FROM orders"))
                .failure().hasMessage("line 1:22: Invalid column mask for 'local.tiny.orders.orderkey': Column 'unknown_column' cannot be resolved");

        // invalid type
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                "orderkey",
                USER,
                ViewExpression.builder()
                        .identity(RUN_AS_USER)
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("'foo'")
                        .build());

        assertThat(assertions.query("SELECT orderkey FROM orders"))
                .failure().hasMessage("line 1:22: Expected column mask for 'local.tiny.orders.orderkey' to be of type bigint, but was varchar(3)");

        // aggregation
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                "orderkey",
                USER,
                ViewExpression.builder()
                        .identity(RUN_AS_USER)
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("count(*) > 0")
                        .build());

        assertThat(assertions.query("SELECT orderkey FROM orders"))
                .failure().hasMessage("line 1:10: Column mask for 'orders.orderkey' cannot contain aggregations, window functions or grouping operations: [count(*)]");

        // window function
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                "orderkey",
                USER,
                ViewExpression.builder()
                        .identity(RUN_AS_USER)
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("row_number() OVER () > 0")
                        .build());

        assertThat(assertions.query("SELECT orderkey FROM orders"))
                .failure().hasMessage("line 1:22: Column mask for 'orders.orderkey' cannot contain aggregations, window functions or grouping operations: [row_number() OVER ()]");

        // grouping function
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                "orderkey",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("grouping(orderkey) = 0")
                        .build());

        assertThat(assertions.query("SELECT orderkey FROM orders"))
                .failure().hasMessage("line 1:20: Column mask for 'orders.orderkey' cannot contain aggregations, window functions or grouping operations: [GROUPING (orderkey)]");
    }

    @Test
    public void testShowStats()
    {
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                "orderkey",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("7")
                        .build());

        assertThat(assertions.query("SHOW STATS FOR (SELECT * FROM orders)"))
                .containsAll(
                        """
                        VALUES
                         (VARCHAR 'orderkey', CAST(NULL AS double), 1e0, 0e1, NULL, '7', '7'),
                         (VARCHAR 'clerk', 15e3, 1e3, 0e1, NULL, CAST(NULL AS varchar), CAST(NULL AS varchar)),
                         (NULL, NULL, NULL, NULL, 15e3, NULL, NULL)
                        """);
        assertThat(assertions.query("SHOW STATS FOR (SELECT orderkey FROM orders)"))
                .matches(
                        """
                        VALUES
                         (VARCHAR 'orderkey', CAST(NULL AS double), 1e0, 0e1, NULL, VARCHAR '7', VARCHAR '7'),
                         (NULL, NULL, NULL, NULL, 15e3, NULL, NULL)
                        """);
        assertThat(assertions.query("SHOW STATS FOR (SELECT clerk FROM orders)"))
                .matches(
                        """
                        VALUES
                         (VARCHAR 'clerk', 15e3, 1e3, 0e1, NULL, CAST(NULL AS varchar), CAST(NULL AS varchar)),
                         (NULL, NULL, NULL, NULL, 15e3, NULL, NULL)
                        """);
    }

    @Test
    public void testJoin()
    {
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                "orderkey",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("orderkey + 1")
                        .build());
        assertThat(assertions.query("SELECT count(*) FROM orders JOIN orders USING (orderkey)")).matches("VALUES BIGINT '15000'");
    }

    @Test
    public void testColumnMaskingUsingRestrictedColumn()
    {
        accessControl.reset();
        accessControl.deny(privilege("orders.custkey", SELECT_COLUMN));
        accessControl.columnMask(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                "orderkey",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("custkey")
                        .build());
        assertThat(assertions.query("SELECT orderkey FROM orders"))
                .failure().hasMessage("Access Denied: Cannot select from columns [orderkey, custkey] in table or view local.tiny.orders");
    }

    @Test
    public void testInsertWithColumnMasking()
    {
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                "clerk",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("clerk")
                        .build());
        assertThat(assertions.query("INSERT INTO orders SELECT * FROM orders"))
                .failure().hasMessage("line 1:1: Insert into table with column masks is not supported");
    }

    @Test
    public void testDeleteWithColumnMasking()
    {
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                "clerk",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("clerk")
                        .build());
        assertThat(assertions.query("DELETE FROM orders"))
                .failure().hasMessage("line 1:1: Delete from table with column mask");
    }

    @Test
    public void testUpdateWithColumnMasking()
    {
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                "clerk",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("clerk")
                        .build());
        assertThat(assertions.query("UPDATE orders SET clerk = 'X'"))
                .failure().hasMessage("line 1:1: Updating a table with column masks is not supported");
        assertThat(assertions.query("UPDATE orders SET orderkey = -orderkey"))
                .failure().hasMessage("line 1:1: Updating a table with column masks is not supported");
        assertThat(assertions.query("UPDATE orders SET clerk = 'X', orderkey = -orderkey"))
                .failure().hasMessage("line 1:1: Updating a table with column masks is not supported");
    }

    @Test
    public void testNotReferencedAndDeniedColumnMasking()
    {
        // mask on not used varchar column
        accessControl.reset();
        accessControl.deny(privilege("orders.clerk", SELECT_COLUMN));
        accessControl.columnMask(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                "clerk",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("clerk")
                        .build());
        assertThat(assertions.query("SELECT orderkey FROM orders WHERE orderkey = 1")).matches("VALUES BIGINT '1'");

        // mask on long column
        accessControl.reset();
        accessControl.deny(privilege("orders.totalprice", SELECT_COLUMN));
        accessControl.columnMask(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                "totalprice",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("totalprice")
                        .build());
        assertThat(assertions.query("SELECT orderkey FROM orders WHERE orderkey = 1")).matches("VALUES BIGINT '1'");

        // mask on not used varchar column with subquery masking
        accessControl.reset();
        accessControl.deny(privilege("orders.clerk", SELECT_COLUMN));
        accessControl.deny(privilege("orders.orderstatus", SELECT_COLUMN));
        accessControl.columnMask(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                "clerk",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("(SELECT orderstatus FROM local.tiny.orders)")
                        .build());
        assertThat(assertions.query("SELECT orderkey FROM orders WHERE orderkey = 1")).matches("VALUES BIGINT '1'");
    }

    @Test
    public void testColumnMaskWithHiddenColumns()
    {
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(MOCK_CATALOG, "tiny", "nation_with_hidden_column"),
                "name",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("'POLAND'")
                        .build());

        assertions.query("SELECT * FROM mock.tiny.nation_with_hidden_column WHERE nationkey = 1")
                .assertThat()
                .skippingTypesCheck()
                .matches("VALUES (BIGINT '1', 'POLAND', BIGINT '1', 'al foxes promise slyly according to the regular accounts. bold requests alon')");
        assertions.query("SELECT DISTINCT name FROM mock.tiny.nation_with_hidden_column WHERE nationkey = 1")
                .assertThat()
                .skippingTypesCheck()
                .matches("VALUES 'POLAND'");
        assertThat(assertions.query("INSERT INTO mock.tiny.nation_with_hidden_column SELECT * FROM mock.tiny.nation_with_hidden_column"))
                .failure().hasMessage("line 1:1: Insert into table with column masks is not supported");
        assertThat(assertions.query("DELETE FROM mock.tiny.nation_with_hidden_column"))
                .failure().hasMessage("line 1:1: Delete from table with column mask");
        assertThat(assertions.query("UPDATE mock.tiny.nation_with_hidden_column SET name = 'X'"))
                .failure().hasMessage("line 1:1: Updating a table with column masks is not supported");
    }

    @Test
    public void testMultipleMasksUsingOtherMaskedColumns()
    {
        // Showcase original row values
        String query = "SELECT comment, orderstatus, clerk FROM orders WHERE orderkey = 1";
        String expected = "VALUES (CAST('nstructions sleep furiously among ' as varchar(79)), 'O', 'Clerk#000000951')";
        accessControl.reset();
        assertThat(assertions.query(query)).matches(expected);

        // Mask "clerk" and "orderstatus" using "comment" ("comment" appears after "clerk" and "orderstatus" in table definition)
        // Nothing changes for "clerk" and "orderstatus" since the condition on "clerk" is not satisfied
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(TEST_CATALOG_NAME, "tiny", "orders"),
                "comment",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("cast(regexp_replace(comment,'(password: [^ ]+)','password: ****') as varchar(79))")
                        .build());

        accessControl.columnMask(
                new QualifiedObjectName(TEST_CATALOG_NAME, "tiny", "orders"),
                "orderstatus",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("if(regexp_extract(comment,'(country: [^ ]+)') IN ('country: 1'), '*', orderstatus)")
                        .build());

        accessControl.columnMask(
                new QualifiedObjectName(TEST_CATALOG_NAME, "tiny", "orders"),
                "clerk",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("if(regexp_extract(comment,'(country: [^ ]+)') IN ('country: 1'), '***', clerk)")
                        .build());

        assertThat(assertions.query(query)).matches(expected);

        // Mask "comment" using "clerk" ("clerk" column appears before "comment" in table definition)
        // Nothing changes for "comment" since the condition on "clerk" is not satisfied
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(TEST_CATALOG_NAME, "tiny", "orders"),
                "clerk",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("cast(regexp_replace(clerk,'(password: [^ ]+)','password: ****') as varchar(15))")
                        .build());

        accessControl.columnMask(
                new QualifiedObjectName(TEST_CATALOG_NAME, "tiny", "orders"),
                "comment",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("if(regexp_extract(clerk,'(country: [^ ]+)') IN ('country: 1'), '***', comment)")
                        .build());

        assertThat(assertions.query(query)).matches(expected);

        // Mask "orderstatus" and "comment" using "clerk"
        // Nothing changes for "orderstatus" and "comment" since the condition on "clerk" is not satisfied
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(TEST_CATALOG_NAME, "tiny", "orders"),
                "clerk",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("cast(regexp_replace(clerk,'(password: [^ ]+)','password: ****') as varchar(15))")
                        .build());

        accessControl.columnMask(
                new QualifiedObjectName(TEST_CATALOG_NAME, "tiny", "orders"),
                "orderstatus",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("if(regexp_extract(clerk,'(country: [^ ]+)') IN ('country: 1'), '*', orderstatus)")
                        .build());

        accessControl.columnMask(
                new QualifiedObjectName(TEST_CATALOG_NAME, "tiny", "orders"),
                "comment",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("if(regexp_extract(clerk,'(country: [^ ]+)') IN ('country: 1'), '***', comment)")
                        .build());

        assertThat(assertions.query(query)).matches(expected);

        // Mask "comment" using "clerk" ("clerk" appears before "comment" in table definition)
        // "comment" is masked as the condition on "clerk" is satisfied
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                "clerk",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("cast(regexp_replace(clerk,'(Clerk#)','***#') as varchar(15))")
                        .build());

        accessControl.columnMask(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                "comment",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("if(regexp_extract(clerk,'([1-9]+)') IN ('951'), '***', comment)")
                        .build());

        assertThat(assertions.query(query))
                .matches("VALUES (CAST('***' as varchar(79)), 'O', CAST('***#000000951' as varchar(15)))");

        // Mask "comment" and "orderstatus" using "clerk" ("clerk" appears between "orderstatus" and "comment" in table definition)
        // "comment" and "orderstatus" are masked as the condition on "clerk" is satisfied
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                "clerk",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("cast('###' as varchar(15))")
                        .build());

        accessControl.columnMask(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                "orderstatus",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("if(regexp_extract(clerk,'([1-9]+)') IN ('951'), '*', orderstatus)")
                        .build());

        accessControl.columnMask(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                "comment",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("if(regexp_extract(clerk,'([1-9]+)') IN ('951'), '***', comment)")
                        .build());

        assertThat(assertions.query(query))
                .matches("VALUES (CAST('***' as varchar(79)), '*', CAST('###' as varchar(15)))");
    }

    @Test
    public void testColumnAliasing()
    {
        accessControl.reset();
        accessControl.columnMask(
                new QualifiedObjectName(MOCK_CATALOG, "default", "view_with_nested"),
                "nested",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .expression("if(id = 0, nested)")
                        .build());

        assertThat(assertions.query("SELECT nested[1] FROM mock.default.view_with_nested"))
                .matches("VALUES 1, NULL");
    }
}
