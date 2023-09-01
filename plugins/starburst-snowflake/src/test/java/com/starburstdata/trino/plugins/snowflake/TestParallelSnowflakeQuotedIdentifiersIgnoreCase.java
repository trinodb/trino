/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import io.trino.testng.services.ManageTestResources;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.Optional;

import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.SNOWFLAKE_CATALOG;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.impersonationDisabled;
import static com.starburstdata.trino.plugins.snowflake.parallel.SnowflakeParallelSessionProperties.QUOTED_IDENTIFIERS_IGNORE_CASE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public class TestParallelSnowflakeQuotedIdentifiersIgnoreCase
        extends AbstractTestQueryFramework
{
    @ManageTestResources.Suppress(because = "Mock to remote service provider")
    private SnowflakeServer server;
    private String testDbName;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        server = new SnowflakeServer();
        TestDatabase testDb = closeAfterClass(server.createTestDatabase());
        testDbName = testDb.getName();
        return SnowflakeQueryRunner.parallelBuilder()
                .withServer(server)
                .withDatabase(Optional.of(testDbName))
                .withSchema(Optional.of(TEST_SCHEMA))
                .withConnectorProperties(impersonationDisabled())
                .build();
    }

    @Test
    public void testQuotedTableNames()
    {
        String tableName = "TaBle_" + randomNameSuffix();
        executeSnowflake("CREATE TABLE %s.\"%s\" AS SELECT 1 test".formatted(TEST_SCHEMA, tableName));
        String tableNameUpperCase = tableName.toUpperCase(ENGLISH);
        String tableNameLowerCase = tableName.toLowerCase(ENGLISH);

        String reachableTableName = "REACHABLE_TABLE_" + randomNameSuffix().toUpperCase(ENGLISH);
        executeSnowflake("CREATE TABLE %s.\"%s\" AS SELECT 1 test".formatted(TEST_SCHEMA, reachableTableName));

        assertQuerySucceeds("SELECT test FROM %s".formatted(reachableTableName));
        String failureRegex = "line 1:18: Table '%s.%s.%s' does not exist".formatted(SNOWFLAKE_CATALOG, TEST_SCHEMA, tableNameLowerCase);
        assertQueryFails(ignoreCase(getSession()), "SELECT test FROM %s".formatted(tableName), failureRegex);

        // Quoted identifier other than all caps becomes unreachable when executing with QUOTED_IDENTIFIERS_IGNORE_CASE=true
        executeSnowflakeIgnoreCase("SELECT test FROM %s.\"%s\"".formatted(TEST_SCHEMA, reachableTableName));
        // More about these cases: https://docs.snowflake.com/en/sql-reference/identifiers-syntax#impact-of-changing-the-parameter
        assertThatThrownBy(() -> executeSnowflakeIgnoreCase("SELECT test FROM %s.%s".formatted(TEST_SCHEMA, tableName)))
                .hasMessageContaining("does not exist or not authorized.");
        assertThatThrownBy(() -> executeSnowflakeIgnoreCase("SELECT test FROM %s.\"%s\"".formatted(TEST_SCHEMA, tableName)))
                .hasMessageContaining("does not exist or not authorized.");
        assertThatThrownBy(() -> executeSnowflakeIgnoreCase("SELECT test FROM %s.%s".formatted(TEST_SCHEMA, tableNameUpperCase)))
                .hasMessageContaining("does not exist or not authorized.");
        assertThatThrownBy(() -> executeSnowflakeIgnoreCase("SELECT test FROM %s.\"%s\"".formatted(TEST_SCHEMA, tableNameUpperCase)))
                .hasMessageContaining("does not exist or not authorized.");
        assertThatThrownBy(() -> executeSnowflakeIgnoreCase("SELECT test FROM %s.%s".formatted(TEST_SCHEMA, tableNameLowerCase)))
                .hasMessageContaining("does not exist or not authorized.");
        assertThatThrownBy(() -> executeSnowflakeIgnoreCase("SELECT test FROM %s.\"%s\"".formatted(TEST_SCHEMA, tableNameLowerCase)))
                .hasMessageContaining("does not exist or not authorized.");
    }

    @Test
    public void testQuotedColumnsCreatedWithProperty()
            throws SQLException
    {
        try (TestTable testTable = new TestTable(
                this::executeSnowflakeIgnoreCase,
                TEST_SCHEMA + ".test_quoted_columns_with_property_",
                "(\"AbC\" decimal(19, 0))",
                ImmutableList.of("1"))) {
            // when creating objects with SET QUOTED_IDENTIFIERS_IGNORE_CASE=true, Snowflake creates them in upper case
            server.executeOnDatabaseWithResultSetConsumer(testDbName, resultSet -> {
                try {
                    resultSet.next();
                    assertThat(resultSet.getString("name")).isEqualTo("ABC");
                }
                catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }, "DESCRIBE TABLE %s".formatted(testTable.getName()));

            assertQuery("SELECT abc FROM %s".formatted(testTable.getName()), "VALUES (1)");
            assertQuery(ignoreCase(getSession()), "SELECT abc FROM %s".formatted(testTable.getName()), "VALUES (1)");
        }
    }

    @Test
    public void testQuotedColumnsCreatedWithoutProperty()
    {
        try (TestTable testTable = new TestTable(
                this::executeSnowflake,
                TEST_SCHEMA + ".test_quoted_columns_",
                "(\"aBc\" decimal(19, 0), \"XyZ\" decimal(19, 0))",
                ImmutableList.of("1, 2"))) {
            assertQuery("SELECT abc, xyz FROM %s".formatted(testTable.getName()), "VALUES (1, 2)");
            assertQuery(ignoreCase(getSession()), "SELECT abc, xyz FROM %s".formatted(testTable.getName()), "VALUES (1, 2)");
        }
    }

    @Test
    public void testQuotedDuplicatedColumnsCreatedOutsideTrino()
    {
        try (TestTable testTable = new TestTable(
                this::executeSnowflake,
                TEST_SCHEMA + ".test_quoted_columns_",
                // It is impossible to query tables with duplicated column names in Trino (possible with Snowflake)
                "(\"aBc\" decimal(19, 0), \"AbC\" decimal(19, 0))",
                ImmutableList.of("1, 2"))) {
            // These queries won't even make it to result reading stage
            assertThatThrownBy(() -> assertQuery("SELECT * FROM %s".formatted(testTable.getName())))
                    .hasRootCauseMessage("Multiple entries with same key: abc=AbC:decimal(19,0):NUMBER and abc=aBc:decimal(19,0):NUMBER");
            assertThatThrownBy(() -> assertQuery(ignoreCase(getSession()), "SELECT * FROM %s".formatted(testTable.getName())))
                    .hasRootCauseMessage("Multiple entries with same key: abc=AbC:decimal(19,0):NUMBER and abc=aBc:decimal(19,0):NUMBER");
            // Same issue for queries including one of duplicate columns names, it blows up on gathering table stats
            assertThatThrownBy(() -> assertQuery("SELECT abc FROM %s".formatted(testTable.getName()), "VALUES (1)"))
                    .hasRootCauseMessage("Multiple entries with same key: abc=AbC:decimal(19,0):NUMBER and abc=aBc:decimal(19,0):NUMBER");
        }
    }

    private void executeSnowflake(String sql)
    {
        server.safeExecuteOnDatabase(testDbName, sql);
    }

    private void executeSnowflakeIgnoreCase(String sql)
    {
        server.safeExecuteOnDatabase(testDbName, "ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = true", sql);
    }

    private static Session ignoreCase(Session session)
    {
        return Session.builder(session)
                .setCatalogSessionProperty(SNOWFLAKE_CATALOG, QUOTED_IDENTIFIERS_IGNORE_CASE, "true")
                .build();
    }
}
