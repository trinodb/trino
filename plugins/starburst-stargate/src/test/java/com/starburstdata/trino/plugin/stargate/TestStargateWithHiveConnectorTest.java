/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate;

import io.trino.Session;
import io.trino.plugin.jdbc.JoinOperator;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.starburstdata.trino.plugin.stargate.StargateQueryRunner.createRemoteStarburstQueryRunnerWithHive;
import static java.lang.String.format;
import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertFalse;

public class TestStargateWithHiveConnectorTest
        extends BaseStargateConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Path tempDir = createTempDirectory("HiveCatalog");
        closeAfterClass(() -> deleteRecursively(tempDir, ALLOW_INSECURE));

        remoteStarburst = closeAfterClass(createRemoteStarburstQueryRunnerWithHive(
                tempDir,
                REQUIRED_TPCH_TABLES,
                Optional.empty()));
        return StargateQueryRunner.builder(remoteStarburst, getRemoteCatalogName())
                .build();
    }

    @Override
    protected String getRemoteCatalogName()
    {
        return "hive";
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_CREATE_TABLE:
            case SUPPORTS_RENAME_TABLE:
            case SUPPORTS_INSERT:
            case SUPPORTS_DELETE:
            case SUPPORTS_ADD_COLUMN:
                // Writes are not enabled
                return false;
            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    public void testCaseSensitiveAggregationPushdown()
    {
        // This is tested in TestStarburstRemoteWithMemoryWritesEnabledConnectorTest
        assertThatThrownBy(super::testCaseSensitiveAggregationPushdown)
                .hasMessageStartingWith("This connector does not support creating tables");
        throw new SkipException("tested elsewhere");
    }

    @Override
    public void testCaseSensitiveTopNPushdown()
    {
        // This is tested in TestStarburstRemoteWithMemoryWritesEnabledConnectorTest
        assertThatThrownBy(super::testCaseSensitiveTopNPushdown)
                .hasMessageStartingWith("This connector does not support creating tables");
        throw new SkipException("tested elsewhere");
    }

    @Override
    public void testNullSensitiveTopNPushdown()
    {
        // This is tested in TestStarburstRemoteWithMemoryWritesEnabledConnectorTest
        assertThatThrownBy(super::testNullSensitiveTopNPushdown)
                .hasMessageStartingWith("This connector does not support creating tables");
        throw new SkipException("tested elsewhere");
    }

    @Override
    public void testCommentColumn()
    {
        // TODO (https://starburstdata.atlassian.net/browse/SEP-4832) make sure this is tested
        assertThatThrownBy(super::testCommentColumn)
                .hasMessageStartingWith("This connector does not support creating tables");
        throw new SkipException("not supported");
    }

    @Override
    public void testCommentColumnName(String columnName)
    {
        // TODO (https://starburstdata.atlassian.net/browse/SEP-4832) make sure this is tested
        assertThatThrownBy(() -> super.testCommentColumnName(columnName))
                .hasMessageStartingWith("This connector does not support creating tables");
        throw new SkipException("not supported");
    }

    @Override
    public void testCommentColumnSpecialCharacter(String comment)
    {
        // TODO (https://starburstdata.atlassian.net/browse/SEP-4832) make sure this is tested
        assertThatThrownBy(() -> super.testCommentColumnSpecialCharacter(comment))
                .hasMessageStartingWith("This connector does not support creating tables");
        throw new SkipException("not supported");
    }

    @Test
    public void testReadFromRemoteHiveView()
    {
        onRemoteDatabase().execute("CREATE OR REPLACE VIEW hive.tiny.nation_count AS SELECT COUNT(*) cnt FROM tpch.tiny.nation");
        assertQuery("SELECT cnt FROM nation_count", "SELECT 25");
    }

    @Test
    public void testPredicatePushdown()
    {
        String catalog = getSession().getCatalog().orElseThrow();

        // varchar equality
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'ROMANIA'"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // varchar range
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name BETWEEN 'POLAND' AND 'RPA'"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // varchar IN without domain compaction
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name IN ('POLAND', 'ROMANIA', 'VIETNAM')"))
                .matches("VALUES " +
                        "(BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25))), " +
                        "(BIGINT '2', BIGINT '21', CAST('VIETNAM' AS varchar(25)))")
                .isFullyPushedDown();

        // varchar IN with small compaction threshold
        assertThat(query(
                Session.builder(getSession())
                        .setCatalogSessionProperty(catalog, "domain_compaction_threshold", "1")
                        .build(),
                "SELECT regionkey, nationkey, name FROM nation WHERE name IN ('POLAND', 'ROMANIA', 'VIETNAM')"))
                .matches("VALUES " +
                        "(BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25))), " +
                        "(BIGINT '2', BIGINT '21', CAST('VIETNAM' AS varchar(25)))")
                // Filter node is retained (and a range predicate is pushed into connector)
                // TODO verify constraints present on the table handle
                .isNotFullyPushedDown(FilterNode.class);

        // varchar different case
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'romania'"))
                .returnsEmptyResult()
                .isFullyPushedDown();

        // bigint equality
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE nationkey = 19"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // bigint equality with small compaction threshold
        assertThat(query(
                Session.builder(getSession())
                        .setCatalogSessionProperty(catalog, "domain_compaction_threshold", "1")
                        .build(),
                "SELECT regionkey, nationkey, name FROM nation WHERE nationkey IN (19, 21)"))
                .matches("VALUES " +
                        "(BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25))), " +
                        "(BIGINT '2', BIGINT '21', CAST('VIETNAM' AS varchar(25)))")
                .isNotFullyPushedDown(FilterNode.class);

        // bigint range, with decimal to bigint simplification
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE nationkey BETWEEN 18.5 AND 19.5"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // date equality
        assertThat(query("SELECT orderkey FROM orders WHERE orderdate = DATE '1992-09-29'"))
                .matches("VALUES BIGINT '1250', 34406, 38436, 57570")
                .isFullyPushedDown();

        // predicate over aggregation key (likely to be optimized before being pushed down into the connector)
        assertThat(query("SELECT * FROM (SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey) WHERE regionkey = 3"))
                .matches("VALUES (BIGINT '3', BIGINT '77')")
                .isFullyPushedDown();

        // predicate over aggregation result
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey HAVING sum(nationkey) = 77"))
                .matches("VALUES (BIGINT '3', BIGINT '77')")
                .isFullyPushedDown();

        // predicate over TopN result
        assertThat(query("" +
                "SELECT orderkey " +
                "FROM (SELECT * FROM orders ORDER BY orderdate DESC, orderkey ASC LIMIT 10)" +
                "WHERE orderdate = DATE '1998-08-01'"))
                .matches("VALUES BIGINT '27588', 22403, 37735")
                .ordered()
                .isFullyPushedDown();

        assertThat(query("" +
                "SELECT custkey " +
                "FROM (SELECT SUM(totalprice) as sum, custkey, COUNT(*) as cnt FROM orders GROUP BY custkey order by sum desc limit 10) " +
                "WHERE cnt > 30"))
                .matches("VALUES BIGINT '643', 898")
                .ordered()
                .isFullyPushedDown();

        // predicate over join
        Session joinPushdownEnabled = joinPushdownEnabled(getSession());
        assertThat(query(joinPushdownEnabled, "SELECT c.name, n.name FROM customer c JOIN nation n ON c.custkey = n.nationkey WHERE acctbal > 8000"))
                .isFullyPushedDown();

        // varchar predicate over join
        assertThat(query(joinPushdownEnabled, "SELECT c.name, n.name FROM customer c JOIN nation n ON c.custkey = n.nationkey WHERE address = 'TcGe5gaZNgVePxU5kRrvXBfkasDTea'"))
                .isFullyPushedDown();
        assertThat(query(joinPushdownEnabled, "SELECT c.name, n.name FROM customer c JOIN nation n ON c.custkey = n.nationkey WHERE address < 'TcGe5gaZNgVePxU5kRrvXBfkasDTea'"))
                .isFullyPushedDown();
    }

    @Test
    public void testDecimalPredicatePushdown()
    {
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "test_decimal_pushdown",
                "(short_decimal decimal(9, 3), long_decimal decimal(30, 10))",
                List.of("123.321, 123456789.987654321"))) {
            assertThat(query("SELECT * FROM " + table.getName() + " WHERE short_decimal <= 124"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + table.getName() + " WHERE short_decimal <= 124"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + table.getName() + " WHERE long_decimal <= 123456790"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + table.getName() + " WHERE short_decimal <= 123.321"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + table.getName() + " WHERE long_decimal <= 123456789.987654321"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + table.getName() + " WHERE short_decimal = 123.321"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + table.getName() + " WHERE long_decimal = 123456789.987654321"))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
        }
    }

    @Test
    public void testCharPredicatePushdown()
    {
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "test_char_pushdown",
                "(char_1 char(1), char_5 char(5), char_10 char(10))",
                List.of(
                        "'0', '0', '0'",
                        "'1', '12345', '1234567890'"))) {
            assertThat(query("SELECT * FROM " + table.getName() + " WHERE char_1 = '0' AND char_5 = '0'"))
                    .matches("VALUES (CHAR'0', CHAR'0    ', CHAR'0         ')")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + table.getName() + " WHERE char_5 = CHAR'12345' AND char_10 = '1234567890'"))
                    .matches("VALUES (CHAR'1', CHAR'12345', CHAR'1234567890')")
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + table.getName() + " WHERE char_10 = CHAR'0'"))
                    .matches("VALUES (CHAR'0', CHAR'0    ', CHAR'0         ')")
                    .isFullyPushedDown();
        }
    }

    @Test
    public void testOrPredicatePushdown()
    {
        assertThat(query("SELECT * FROM nation WHERE nationkey != 3 OR regionkey = 4")).isFullyPushedDown();
        assertThat(query("SELECT * FROM nation WHERE nationkey != 3 OR regionkey != 4")).isFullyPushedDown();
        assertThat(query("SELECT * FROM nation WHERE name = 'ALGERIA' OR regionkey = 4")).isFullyPushedDown();
        assertThat(query("SELECT * FROM nation WHERE name IS NULL OR regionkey = 4")).isFullyPushedDown();
        assertThat(query("SELECT * FROM nation WHERE name = NULL OR regionkey = 4")).isFullyPushedDown();
    }

    @Test
    public void testSubstringPredicatePushdown()
    {
        assertThat(query("SELECT nationkey, name FROM nation WHERE substring(name, 3, 1) = 'P'"))
                .isFullyPushedDown()
                .matches("VALUES (BIGINT '12', CAST('JAPAN' AS varchar(25)))");
    }

    @Test
    public void testLikePredicatePushdown()
    {
        assertThat(query("SELECT nationkey FROM nation WHERE name LIKE '%A%'"))
                .isFullyPushedDown();

        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "test_like_predicate_pushdown",
                "(id integer, a_varchar varchar(1))",
                List.of(
                        "1, 'A'",
                        "2, 'a'",
                        "3, 'B'",
                        "4, 'ą'",
                        "5, 'Ą'"))) {
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE a_varchar LIKE '%A%'"))
                    .isFullyPushedDown();
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE a_varchar LIKE '%ą%'"))
                    .isFullyPushedDown();
        }
    }

    @Test
    public void testLikeWithEscapePredicatePushdown()
    {
        assertThat(query("SELECT nationkey FROM nation WHERE name LIKE '%A%' ESCAPE '\\'"))
                .isFullyPushedDown();

        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "test_like_with_escape_predicate_pushdown",
                "(id integer, a_varchar varchar(4))",
                List.of(
                        "1, 'A%b'",
                        "2, 'Asth'",
                        "3, 'ą%b'",
                        "4, 'ąsth'"))) {
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE a_varchar LIKE '%A\\%%' ESCAPE '\\'"))
                    .isFullyPushedDown();
            assertThat(query("SELECT id FROM " + table.getName() + " WHERE a_varchar LIKE '%ą\\%%' ESCAPE '\\'"))
                    .isFullyPushedDown();
        }
    }

    @Test
    public void testRegexpLikePredicatePushdown()
    {
        assertThat(query("SELECT nationkey FROM nation WHERE regexp_like(name, '.*[PF].*')"))
                .isFullyPushedDown();
    }

    @Test
    public void testIsNullPredicatePushdown()
    {
        assertThat(query("SELECT nationkey FROM nation WHERE name IS NULL")).isFullyPushedDown();
        assertThat(query("SELECT nationkey FROM nation WHERE name IS NULL OR regionkey = 4")).isFullyPushedDown();

        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "test_is_null_predicate_pushdown",
                "(a_int integer, a_varchar varchar(1))",
                List.of(
                        "1, 'A'",
                        "2, 'B'",
                        "1, NULL",
                        "2, NULL"))) {
            assertThat(query("SELECT a_int FROM " + table.getName() + " WHERE a_varchar IS NULL OR a_int = 1")).isFullyPushedDown();
        }
    }

    @Test
    public void testIsNotNullPredicatePushdown()
    {
        assertThat(query("SELECT nationkey FROM nation WHERE name IS NOT NULL OR regionkey = 4")).isFullyPushedDown();

        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "test_is_not_null_predicate_pushdown",
                "(a_int integer, a_varchar varchar(1))",
                List.of(
                        "1, 'A'",
                        "2, 'B'",
                        "1, NULL",
                        "2, NULL"))) {
            assertThat(query("SELECT a_int FROM " + table.getName() + " WHERE a_varchar IS NOT NULL OR a_int = 1")).isFullyPushedDown();
        }
    }

    @Test
    public void testNullIfPredicatePushdown()
    {
        assertThat(query("SELECT nationkey FROM nation WHERE NULLIF(name, 'ALGERIA') IS NULL"))
                .matches("VALUES BIGINT '0'")
                .isFullyPushedDown();

        assertThat(query("SELECT name FROM nation WHERE NULLIF(nationkey, 0) IS NULL"))
                .matches("VALUES CAST('ALGERIA' AS varchar(25))")
                .isFullyPushedDown();

        assertThat(query("SELECT nationkey FROM nation WHERE NULLIF(name, 'Algeria') IS NULL"))
                .returnsEmptyResult()
                .isFullyPushedDown();

        // NULLIF returns the first argument because arguments aren't the same
        assertThat(query("SELECT nationkey FROM nation WHERE NULLIF(name, 'Name not found') = name"))
                .matches("SELECT nationkey FROM nation")
                .isFullyPushedDown();
    }

    @Test
    public void testNotExpressionPushdown()
    {
        assertThat(query("SELECT nationkey FROM nation WHERE NOT(name LIKE '%A%' ESCAPE '\\')")).isFullyPushedDown();

        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                "test_is_not_predicate_pushdown",
                "(a_int integer, a_varchar varchar(2))",
                List.of(
                        "1, 'Aa'",
                        "2, 'Bb'",
                        "1, NULL",
                        "2, NULL"))) {
            assertThat(query("SELECT a_int FROM " + table.getName() + " WHERE NOT(a_varchar LIKE 'A%') OR a_int = 2")).isFullyPushedDown();
            assertThat(query("SELECT a_int FROM " + table.getName() + " WHERE NOT(a_varchar LIKE 'A%' OR a_int = 2)")).isFullyPushedDown();
        }
    }

    /**
     * This test normally requires table creation, so we disable it here.
     * We will trust that
     * {@link TestStargateWithMemoryWritesEnabledConnectorTest} tests
     * join pushdown sufficiently until we can run this test without creating
     * tables using the Remote Starburst instance.
     */
    @Override
    @Test(dataProvider = "joinOperators")
    public void testJoinPushdown(JoinOperator joinOperator)
    {
        // Make sure that the test still fails how we expect it to, on table creation instead
        // of on join pushdown.
        assertThatThrownBy(() -> super.testJoinPushdown(joinOperator))
                .hasMessageMatching("This connector does not support creating tables.*");
        throw new SkipException("test requires table creation");
    }

    @Override
    public void testNativeQuerySimple()
    {
        assertQuery("SELECT * FROM TABLE(system.query(query => 'SELECT 1 a'))", "VALUES 1");
    }

    @Override
    public void testNativeQueryCreateStatement()
    {
        // SingleStore returns a ResultSet metadata with no columns for CREATE TABLE statement.
        // This is unusual, because other connectors don't produce a ResultSet metadata for CREATE TABLE at all.
        // The query fails because there are no columns, but even if columns were not required, the query would fail
        // to execute in this connector because the connector wraps it in additional syntax, which causes syntax error.
        assertFalse(getQueryRunner().tableExists(getSession(), "numbers"));
        assertThatThrownBy(() -> query("SELECT * FROM TABLE(system.query(query => 'CREATE TABLE numbers(n INTEGER)'))"))
                .hasMessageContaining("descriptor has no fields");
        assertFalse(getQueryRunner().tableExists(getSession(), "numbers"));
    }

    @Override
    public void testNativeQueryInsertStatementTableExists()
    {
        try (TestTable testTable = simpleTable()) {
            assertThatThrownBy(() -> query(format("SELECT * FROM TABLE(system.query(query => 'INSERT INTO %s VALUES (3)'))", testTable.getName())))
                    .hasMessageContaining("mismatched input 'INSERT'");
            assertQuery("SELECT * FROM " + testTable.getName(), "VALUES 1, 2");
        }
    }

    @Override
    protected OptionalInt maxSchemaNameLength()
    {
        // This value depends on metastore type
        return OptionalInt.of(128);
    }

    @Override
    protected void verifySchemaNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("Schema name must be shorter than or equal to '128' characters but got '129'");
    }

    @Override
    public void testAlterTableRenameColumnToLongName()
    {
        assertThatThrownBy(super::testAlterTableRenameColumnToLongName)
                .hasMessageMatching("This connector does not support creating tables with data");
        throw new SkipException("https://starburstdata.atlassian.net/browse/SEP-9764");
    }

    @Override
    public void testRenameColumnName(String columnName)
    {
        assertThatThrownBy(() -> super.testRenameColumnName(columnName))
                .hasMessageMatching("This connector does not support creating tables");
        throw new SkipException("https://starburstdata.atlassian.net/browse/SEP-9764");
    }
}
