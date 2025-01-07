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
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.Identity;
import io.trino.spi.security.ViewExpression;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.VarcharType;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import io.trino.testing.TestingAccessControlManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Optional;

import static io.trino.connector.MockConnectorEntities.TPCH_NATION_DATA;
import static io.trino.connector.MockConnectorEntities.TPCH_NATION_SCHEMA;
import static io.trino.connector.MockConnectorEntities.TPCH_NATION_WITH_HIDDEN_COLUMN;
import static io.trino.connector.MockConnectorEntities.TPCH_NATION_WITH_OPTIONAL_COLUMN;
import static io.trino.connector.MockConnectorEntities.TPCH_WITH_HIDDEN_COLUMN_DATA;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.EXPRESSION_NOT_SCALAR;
import static io.trino.spi.StandardErrorCode.INVALID_ROW_FILTER;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestRowFilter
{
    private static final String LOCAL_CATALOG = "local";
    private static final String MOCK_CATALOG = "mock";
    private static final String MOCK_CATALOG_MISSING_COLUMNS = "mockmissingcolumns";
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

    public TestRowFilter()
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

        runner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withGetViews((s, prefix) -> ImmutableMap.of(new SchemaTableName("default", "nation_view"), view))
                .withGetColumns(schemaTableName -> {
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation"))) {
                        return TPCH_NATION_SCHEMA;
                    }
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_with_hidden_column"))) {
                        return TPCH_NATION_WITH_HIDDEN_COLUMN;
                    }
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_with_optional_column"))) {
                        return TPCH_NATION_WITH_OPTIONAL_COLUMN;
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
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_with_optional_column"))) {
                        return TPCH_NATION_DATA;
                    }
                    throw new UnsupportedOperationException();
                })
                .build()));
        runner.createCatalog(MOCK_CATALOG, "mock", ImmutableMap.of());

        runner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withName("mockmissingcolumns")
                .withGetViews((s, prefix) -> ImmutableMap.of(
                        new SchemaTableName("default", "nation_view"), view))
                .withGetColumns(schemaTableName -> {
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_with_optional_column"))) {
                        return TPCH_NATION_WITH_OPTIONAL_COLUMN;
                    }
                    throw new UnsupportedOperationException();
                })
                .withData(schemaTableName -> {
                    if (schemaTableName.equals(new SchemaTableName("tiny", "nation_with_optional_column"))) {
                        return TPCH_NATION_DATA;
                    }
                    throw new UnsupportedOperationException();
                })
                .withAllowMissingColumnsOnInsert(true)
                .build()));

        runner.createCatalog(MOCK_CATALOG_MISSING_COLUMNS, "mockmissingcolumns", ImmutableMap.of());

        assertions = new QueryAssertions(runner);
        accessControl = assertions.getQueryRunner().getAccessControl();
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testSimpleFilter()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder().expression("orderkey < 10").build());
        assertThat(assertions.query("SELECT count(*) FROM orders")).matches("VALUES BIGINT '7'");

        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder().expression("NULL").build());
        assertThat(assertions.query("SELECT count(*) FROM orders")).matches("VALUES BIGINT '0'");
    }

    @Test
    public void testMultipleFilters()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder().expression("orderkey < 10").build());

        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder().expression("orderkey > 5").build());

        assertThat(assertions.query("SELECT count(*) FROM orders")).matches("VALUES BIGINT '2'");
    }

    @Test
    public void testCorrelatedSubquery()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder()
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("EXISTS (SELECT 1 FROM nation WHERE nationkey = orderkey)")
                        .build());
        assertThat(assertions.query("SELECT count(*) FROM orders")).matches("VALUES BIGINT '7'");
    }

    @Test
    public void testView()
    {
        // filter on the underlying table for view owner when running query as different user
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "nation"),
                VIEW_OWNER,
                ViewExpression.builder().expression("nationkey = 1").build());

        assertThat(assertions.query(
                Session.builder(SESSION)
                        .setIdentity(Identity.forUser(RUN_AS_USER).build())
                        .build(),
                "SELECT name FROM mock.default.nation_view"))
                .matches("VALUES CAST('ARGENTINA' AS VARCHAR(25))");

        // filter on the underlying table for view owner when running as themselves
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "nation"),
                VIEW_OWNER,
                ViewExpression.builder()
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("nationkey = 1")
                        .build());

        assertThat(assertions.query(
                Session.builder(SESSION)
                        .setIdentity(Identity.forUser(VIEW_OWNER).build())
                        .build(),
                "SELECT name FROM mock.default.nation_view"))
                .matches("VALUES CAST('ARGENTINA' AS VARCHAR(25))");

        // filter on the underlying table for user running the query (different from view owner) should not be applied
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "nation"),
                RUN_AS_USER,
                ViewExpression.builder()
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("nationkey = 1")
                        .build());

        Session session = Session.builder(SESSION)
                .setIdentity(Identity.forUser(RUN_AS_USER).build())
                .build();

        assertThat(assertions.query(session, "SELECT count(*) FROM mock.default.nation_view")).matches("VALUES BIGINT '25'");

        // filter on the view
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(MOCK_CATALOG, "default", "nation_view"),
                USER,
                ViewExpression.builder()
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("nationkey = 1")
                        .build());
        assertThat(assertions.query("SELECT name FROM mock.default.nation_view")).matches("VALUES CAST('ARGENTINA' AS VARCHAR(25))");
    }

    @Test
    public void testTableReferenceInWithClause()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder().expression("orderkey = 1").build());
        assertThat(assertions.query("WITH t AS (SELECT count(*) FROM orders) SELECT * FROM t")).matches("VALUES BIGINT '1'");
    }

    @Test
    public void testOtherSchema()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder()
                        .catalog(LOCAL_CATALOG)
                        .schema("sf1") // Filter is TRUE only if evaluating against sf1.customer
                        .expression("(SELECT count(*) FROM customer) = 150000")
                        .build());
        assertThat(assertions.query("SELECT count(*) FROM orders")).matches("VALUES BIGINT '15000'");
    }

    @Test
    public void testDifferentIdentity()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                RUN_AS_USER,
                ViewExpression.builder()
                        .identity(RUN_AS_USER)
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("orderkey = 1")
                        .build());

        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder()
                        .identity(RUN_AS_USER)
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny").expression("orderkey IN (SELECT orderkey FROM orders)")
                        .build());

        assertThat(assertions.query("SELECT count(*) FROM orders")).matches("VALUES BIGINT '1'");
    }

    @Test
    public void testRecursion()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder()
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("orderkey IN (SELECT orderkey FROM orders)")
                        .build());

        assertThat(assertions.query("SELECT count(*) FROM orders"))
                .failure()
                .hasErrorCode(INVALID_ROW_FILTER)
                .hasMessageMatching(".*\\QRow filter for 'local.tiny.orders' is recursive\\E.*");

        // different reference style to same table
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder()
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("orderkey IN (SELECT local.tiny.orderkey FROM orders)")
                        .build());
        assertThat(assertions.query("SELECT count(*) FROM orders"))
                .failure()
                .hasErrorCode(INVALID_ROW_FILTER)
                .hasMessageMatching(".*\\QRow filter for 'local.tiny.orders' is recursive\\E.*");

        // mutual recursion
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                RUN_AS_USER,
                ViewExpression.builder()
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("orderkey IN (SELECT orderkey FROM orders)")
                        .build());

        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder()
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("orderkey IN (SELECT orderkey FROM orders)")
                        .build());

        assertThat(assertions.query("SELECT count(*) FROM orders"))
                .failure()
                .hasErrorCode(INVALID_ROW_FILTER)
                .hasMessageMatching(".*\\QRow filter for 'local.tiny.orders' is recursive\\E.*");
    }

    @Test
    public void testLimitedScope()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "customer"),
                USER,
                ViewExpression.builder()
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("orderkey = 1")
                        .build());
        assertThat(assertions.query(
                "SELECT (SELECT min(name) FROM customer WHERE customer.custkey = orders.custkey) FROM orders"))
                .failure()
                .hasErrorCode(COLUMN_NOT_FOUND)
                .hasMessage("line 1:31: Invalid row filter for 'local.tiny.customer': Column 'orderkey' cannot be resolved");
    }

    @Test
    public void testSqlInjection()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "nation"),
                USER,
                ViewExpression.builder()
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("regionkey IN (SELECT regionkey FROM region WHERE name = 'ASIA')")
                        .build());
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
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder()
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("$$$")
                        .build());

        assertThat(assertions.query("SELECT count(*) FROM orders"))
                .failure()
                .hasErrorCode(INVALID_ROW_FILTER)
                .hasMessage("line 1:22: Invalid row filter for 'local.tiny.orders': mismatched input '$'. Expecting: <expression>");

        // unknown column
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder()
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("unknown_column")
                        .build());

        assertThat(assertions.query("SELECT count(*) FROM orders"))
                .failure()
                .hasErrorCode(COLUMN_NOT_FOUND)
                .hasMessage("line 1:22: Invalid row filter for 'local.tiny.orders': Column 'unknown_column' cannot be resolved");

        // invalid type
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder()
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("1")
                        .build());

        assertThat(assertions.query("SELECT count(*) FROM orders"))
                .failure()
                .hasErrorCode(TYPE_MISMATCH)
                .hasMessage("line 1:22: Expected row filter for 'local.tiny.orders' to be of type BOOLEAN, but was integer");

        // aggregation
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder()
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("count(*) > 0")
                        .build());

        assertThat(assertions.query("SELECT count(*) FROM orders"))
                .failure()
                .hasErrorCode(EXPRESSION_NOT_SCALAR)
                .hasMessage("line 1:10: Row filter for 'local.tiny.orders' cannot contain aggregations, window functions or grouping operations: [count(*)]");

        // window function
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder()
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("row_number() OVER () > 0")
                        .build());

        assertThat(assertions.query("SELECT count(*) FROM orders"))
                .failure()
                .hasErrorCode(EXPRESSION_NOT_SCALAR)
                .hasMessage("line 1:22: Row filter for 'local.tiny.orders' cannot contain aggregations, window functions or grouping operations: [row_number() OVER ()]");

        // window function
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder()
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("grouping(orderkey) = 0")
                        .build());

        assertThat(assertions.query("SELECT count(*) FROM orders"))
                .failure()
                .hasErrorCode(EXPRESSION_NOT_SCALAR)
                .hasMessage("line 1:20: Row filter for 'local.tiny.orders' cannot contain aggregations, window functions or grouping operations: [GROUPING (orderkey)]");
    }

    @Test
    public void testShowStats()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(LOCAL_CATALOG, "tiny", "orders"),
                USER,
                ViewExpression.builder()
                        .identity(RUN_AS_USER)
                        .catalog(LOCAL_CATALOG)
                        .schema("tiny")
                        .expression("orderkey = 0")
                        .build());

        assertThat(assertions.query("SHOW STATS FOR (SELECT * FROM tiny.orders)"))
                .containsAll(
                        "VALUES " +
                                "(VARCHAR 'orderkey', 0e1, 0e1, 1e0, CAST(NULL AS double), CAST(NULL AS varchar), CAST(NULL AS varchar))," +
                                "(VARCHAR 'custkey', 0e1, 0e1, 1e0, CAST(NULL AS double), CAST(NULL AS varchar), CAST(NULL AS varchar))," +
                                "(NULL, NULL, NULL, NULL, 0e1, NULL, NULL)");
    }

    /**
     * @see #testMergeDelete()
     */
    @Test
    public void testDelete()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(MOCK_CATALOG, "tiny", "nation"),
                USER,
                ViewExpression.builder().expression("nationkey < 10").build());

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

    /**
     * Like {@link #testDelete()} but using the MERGE statement.
     */
    @Test
    public void testMergeDelete()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(MOCK_CATALOG, "tiny", "nation"),
                USER,
                ViewExpression.builder().expression("nationkey < 10").build());

        // Within allowed row filter
        assertThat(assertions.query(
                """
                MERGE INTO mock.tiny.nation USING (VALUES 1,2) t(x) ON nationkey = x
                WHEN MATCHED THEN DELETE
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Cannot merge into a table with row filters");

        // Outside allowed row filter
        assertThat(assertions.query(
                """
                MERGE INTO mock.tiny.nation USING (VALUES 1,2,3,4,5) t(x) ON regionkey = x
                WHEN MATCHED THEN DELETE
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Cannot merge into a table with row filters");
        assertThat(assertions.query(
                """
                MERGE INTO mock.tiny.nation USING (VALUES 1,11) t(x) ON nationkey = x
                WHEN MATCHED THEN DELETE
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Cannot merge into a table with row filters");
        assertThat(assertions.query(
                """
                MERGE INTO mock.tiny.nation USING (VALUES 11,12,13,14,15) t(x) ON nationkey = x
                WHEN MATCHED THEN DELETE
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Cannot merge into a table with row filters");
    }

    /**
     * @see #testMergeUpdate()
     */
    @Test
    public void testUpdate()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(MOCK_CATALOG, "tiny", "nation"),
                USER,
                ViewExpression.builder().expression("nationkey < 10").build());

        // Within allowed row filter
        assertThat(assertions.query("UPDATE mock.tiny.nation SET regionkey = regionkey * 2 WHERE nationkey < 3"))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Updating a table with a row filter is not supported");
        assertThat(assertions.query("UPDATE mock.tiny.nation SET regionkey = regionkey * 2 WHERE nationkey IN (1, 2, 3)"))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Updating a table with a row filter is not supported");

        // Outside allowed row filter
        assertThat(assertions.query("UPDATE mock.tiny.nation SET regionkey = regionkey * 2"))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Updating a table with a row filter is not supported");
        assertThat(assertions.query("UPDATE mock.tiny.nation SET regionkey = regionkey * 2 WHERE nationkey IN (1, 11)"))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Updating a table with a row filter is not supported");

        assertThat(assertions.query("UPDATE mock.tiny.nation SET regionkey = regionkey * 2 WHERE nationkey = 11"))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Updating a table with a row filter is not supported");

        // Within allowed row filter, but updated rows are outside the row filter
        assertThat(assertions.query("UPDATE mock.tiny.nation SET nationkey = 10 WHERE nationkey < 3"))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Updating a table with a row filter is not supported");
        assertThat(assertions.query("UPDATE mock.tiny.nation SET nationkey = null WHERE nationkey < 3"))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Updating a table with a row filter is not supported");

        // Outside allowed row filter, and updated rows are outside the row filter
        assertThat(assertions.query("UPDATE mock.tiny.nation SET nationkey = 10 WHERE nationkey = 10"))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Updating a table with a row filter is not supported");
        assertThat(assertions.query("UPDATE mock.tiny.nation SET nationkey = null WHERE nationkey = null "))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Updating a table with a row filter is not supported");
    }

    /**
     * Like {@link #testUpdate()} but using the MERGE statement.
     */
    @Test
    public void testMergeUpdate()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(MOCK_CATALOG, "tiny", "nation"),
                USER,
                ViewExpression.builder().expression("nationkey < 10").build());

        // Within allowed row filter
        assertThat(assertions.query(
                """
                MERGE INTO mock.tiny.nation USING (VALUES 5) t(x) ON nationkey = x
                WHEN MATCHED THEN UPDATE SET regionkey = regionkey * 2
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Cannot merge into a table with row filters");

        // Outside allowed row filter
        assertThat(assertions.query(
                """
                MERGE INTO mock.tiny.nation USING (VALUES 1,2,3,4,5,6) t(x) ON regionkey = x
                WHEN MATCHED THEN UPDATE SET regionkey = regionkey * 2
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Cannot merge into a table with row filters");
        assertThat(assertions.query(
                """
                MERGE INTO mock.tiny.nation USING (VALUES 1, 11) t(x) ON nationkey = x
                WHEN MATCHED THEN UPDATE SET regionkey = regionkey * 2
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Cannot merge into a table with row filters");
        assertThat(assertions.query(
                """
                MERGE INTO mock.tiny.nation USING (VALUES 11) t(x) ON nationkey = x
                WHEN MATCHED THEN UPDATE SET regionkey = regionkey * 2
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Cannot merge into a table with row filters");

        // Within allowed row filter, but updated rows are outside the row filter
        assertThat(assertions.query(
                """
                MERGE INTO mock.tiny.nation USING (VALUES 1,2,3) t(x) ON nationkey = x
                WHEN MATCHED THEN UPDATE SET nationkey = 10
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Cannot merge into a table with row filters");
        assertThat(assertions.query(
                """
                MERGE INTO mock.tiny.nation USING (VALUES 1,2,3) t(x) ON nationkey = x
                WHEN MATCHED THEN UPDATE SET nationkey = NULL
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Cannot merge into a table with row filters");

        // Outside allowed row filter, but updated rows are outside the row filter
        assertThat(assertions.query(
                """
                MERGE INTO mock.tiny.nation USING (VALUES 10) t(x) ON nationkey = x
                WHEN MATCHED THEN UPDATE SET nationkey = 13
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Cannot merge into a table with row filters");
        assertThat(assertions.query(
                """
                MERGE INTO mock.tiny.nation USING (VALUES 10) t(x) ON nationkey = x
                WHEN MATCHED THEN UPDATE SET nationkey = NULL
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Cannot merge into a table with row filters");
        assertThat(assertions.query(
                """
                MERGE INTO mock.tiny.nation USING (VALUES 10) t(x) ON nationkey IS NULL
                WHEN MATCHED THEN UPDATE SET nationkey = 13
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Cannot merge into a table with row filters");
    }

    /**
     * @see #testMergeInsert()
     */
    @Test
    public void testInsert()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(MOCK_CATALOG, "tiny", "nation"),
                USER,
                ViewExpression.builder().expression("nationkey > 100").build());

        // Within allowed row filter
        assertions.query("INSERT INTO mock.tiny.nation VALUES (101, 'POLAND', 0, 'No comment')")
                .assertThat()
                .skippingTypesCheck()
                .matches("SELECT BIGINT '1'");

        // Outside allowed row filter
        assertThat(assertions.query("INSERT INTO mock.tiny.nation VALUES (26, 'POLAND', 0, 'No comment')"))
                .failure()
                .hasErrorCode(PERMISSION_DENIED)
                .hasMessage("Access Denied: Cannot insert row that does not match a row filter");
        assertThat(assertions.query("INSERT INTO mock.tiny.nation VALUES "
                + "(26, 'POLAND', 0, 'No comment'),"
                + "(27, 'HOLLAND', 0, 'A comment')"))
                .failure()
                .hasErrorCode(PERMISSION_DENIED)
                .hasMessage("Access Denied: Cannot insert row that does not match a row filter");
        assertThat(assertions.query("INSERT INTO mock.tiny.nation(nationkey) VALUES (null)"))
                .failure()
                .hasErrorCode(PERMISSION_DENIED)
                .hasMessage("Access Denied: Cannot insert row that does not match a row filter");
        assertThat(assertions.query("INSERT INTO mock.tiny.nation(regionkey) VALUES (0)"))
                .failure()
                .hasErrorCode(PERMISSION_DENIED)
                .hasMessage("Access Denied: Cannot insert row that does not match a row filter");
    }

    /**
     * Like {@link #testInsert()} but using the MERGE statement.
     */
    @Test
    public void testMergeInsert()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(MOCK_CATALOG, "tiny", "nation"),
                USER,
                ViewExpression.builder().expression("nationkey > 100").build());

        // Within allowed row filter
        assertThat(assertions.query(
                """
                MERGE INTO mock.tiny.nation USING (VALUES 42) t(dummy) ON false
                WHEN NOT MATCHED THEN INSERT VALUES (101, 'POLAND', 0, 'No comment')
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Cannot merge into a table with row filters");

        // Outside allowed row filter
        assertThat(assertions.query(
                """
                MERGE INTO mock.tiny.nation USING (VALUES 42) t(dummy) ON false
                WHEN NOT MATCHED THEN INSERT VALUES (26, 'POLAND', 0, 'No comment')
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Cannot merge into a table with row filters");
        assertThat(assertions.query(
                """
                MERGE INTO mock.tiny.nation USING (VALUES (26, 'POLAND', 0, 'No comment'), (27, 'HOLLAND', 0, 'A comment')) t(a,b,c,d) ON nationkey = a
                WHEN NOT MATCHED THEN INSERT VALUES (a,b,c,d)
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Cannot merge into a table with row filters");

        assertThat(assertions.query(
                """
                MERGE INTO mock.tiny.nation USING (VALUES 42) t(dummy) ON false
                WHEN NOT MATCHED THEN INSERT (nationkey) VALUES (NULL)
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Cannot merge into a table with row filters");
        assertThat(assertions.query(
                """
                MERGE INTO mock.tiny.nation USING (VALUES 42) t(dummy) ON false
                WHEN NOT MATCHED THEN INSERT (nationkey) VALUES (0)
                """))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:1: Cannot merge into a table with row filters");
    }

    @Test
    public void testRowFilterWithHiddenColumns()
    {
        accessControl.reset();
        accessControl.rowFilter(
                new QualifiedObjectName(MOCK_CATALOG, "tiny", "nation_with_hidden_column"),
                USER,
                ViewExpression.builder().expression("nationkey < 1").build());

        assertions.query("SELECT * FROM mock.tiny.nation_with_hidden_column")
                .assertThat()
                .skippingTypesCheck()
                .matches("VALUES (BIGINT '0', 'ALGERIA', BIGINT '0', ' haggle. carefully final deposits detect slyly agai')");
        assertThat(assertions.query("INSERT INTO mock.tiny.nation_with_hidden_column VALUES (101, 'POLAND', 0, 'No comment')"))
                .failure()
                .hasErrorCode(PERMISSION_DENIED)
                .hasMessage("Access Denied: Cannot insert row that does not match a row filter");
        assertions.query("INSERT INTO mock.tiny.nation_with_hidden_column VALUES (0, 'POLAND', 0, 'No comment')")
                .assertThat()
                .skippingTypesCheck()
                .matches("VALUES BIGINT '1'");
        assertThat(assertions.query("UPDATE mock.tiny.nation_with_hidden_column SET name = 'POLAND'"))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
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
                ViewExpression.builder().expression("\"$hidden\" < 1").build());

        assertions.query("SELECT count(*) FROM mock.tiny.nation_with_hidden_column")
                .assertThat()
                .skippingTypesCheck()
                .matches("VALUES BIGINT '25'");
        // TODO https://github.com/trinodb/trino/issues/10006 - support insert into a table with row filter that is using hidden columns
        assertThat(assertions.query("INSERT INTO mock.tiny.nation_with_hidden_column VALUES (101, 'POLAND', 0, 'No comment')"))
                // TODO this should be TrinoException (assertTrinoExceptionThrownBy)
                .nonTrinoExceptionFailure()
                .hasStackTraceContaining("ArrayIndexOutOfBoundsException: Index 4 out of bounds for length 4");
        assertThat(assertions.query("UPDATE mock.tiny.nation_with_hidden_column SET name = 'POLAND'"))
                .failure()
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessageContaining("Updating a table with a row filter is not supported");
        assertions.query("DELETE FROM mock.tiny.nation_with_hidden_column WHERE regionkey < 5")
                .assertThat()
                .skippingTypesCheck()
                .matches("SELECT BIGINT '25'");
    }

    @Test
    public void testRowFilterOnOptionalColumn()
    {
        accessControl.reset();

        accessControl.rowFilter(
                new QualifiedObjectName(MOCK_CATALOG_MISSING_COLUMNS, "tiny", "nation_with_optional_column"),
                USER,
                ViewExpression.builder().expression("length(optional) > 2").build());

        assertions.query("INSERT INTO mockmissingcolumns.tiny.nation_with_optional_column(nationkey, name, regionkey, comment, optional) VALUES (0, 'POLAND', 0, 'No comment', 'some string')")
                .assertThat()
                .skippingTypesCheck()
                .matches("VALUES BIGINT '1'");

        assertThat(assertions.query("INSERT INTO mockmissingcolumns.tiny.nation_with_optional_column(nationkey, name, regionkey, comment, optional) VALUES (0, 'POLAND', 0, 'No comment', 'so')"))
                .failure()
                .hasErrorCode(PERMISSION_DENIED)
                .hasMessage("Access Denied: Cannot insert row that does not match a row filter");

        assertThat(assertions.query("INSERT INTO mockmissingcolumns.tiny.nation_with_optional_column(nationkey, name, regionkey, comment, optional) VALUES (0, 'POLAND', 0, 'No comment', null)"))
                .failure()
                .hasErrorCode(PERMISSION_DENIED)
                .hasMessage("Access Denied: Cannot insert row that does not match a row filter");
    }
}
