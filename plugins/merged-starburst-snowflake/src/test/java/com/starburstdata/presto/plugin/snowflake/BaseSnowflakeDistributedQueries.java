/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake;

import io.prestosql.Session;
import io.prestosql.testing.AbstractTestDistributedQueries;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.sql.TestTable;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.google.common.base.Strings.nullToEmpty;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.MaterializedResult.resultBuilder;
import static io.prestosql.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static io.prestosql.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;

public abstract class BaseSnowflakeDistributedQueries
        extends AbstractTestDistributedQueries
{
    SnowflakeServer server = new SnowflakeServer();

    @Override
    protected boolean supportsDelete()
    {
        return false;
    }

    @Override
    protected boolean supportsViews()
    {
        return false;
    }

    @Override
    protected boolean supportsArrays()
    {
        return false;
    }

    @Override
    protected boolean supportsCommentOnTable()
    {
        return false;
    }

    @Override
    protected boolean supportsCommentOnColumn()
    {
        return false;
    }

    @Override
    protected boolean isColumnNameRejected(Exception exception, String columnName, boolean delimited)
    {
        // Snowflake does not support column names containing double quotes
        return columnName.contains("\"") && nullToEmpty(exception.getMessage()).matches(".*(Snowflake columns cannot contain quotes).*");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        // Real: Snowflake does not have a REAL type, instead they are mapped to double. The round trip test fails because REAL '567.123' != DOUBLE '567.123'
        // Char: Snowflake does not have a CHAR type. They map it to varchar, which does not have the same fixed width semantics
        String name = dataMappingTestSetup.getPrestoTypeName();
        if (name.equals("real") || name.startsWith("char")) {
            return Optional.empty();
        }
        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                server::safeExecute,
                format("%s.test_table_with_default_columns", TEST_SCHEMA),
                "(col_required BIGINT NOT NULL," +
                        "col_nullable BIGINT," +
                        "col_default BIGINT DEFAULT 43," +
                        "col_nonnull_default BIGINT NOT NULL DEFAULT 42," +
                        "col_required2 BIGINT NOT NULL)");
    }

    @Override
    public void testShowColumns()
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");

        MaterializedResult expectedParametrizedVarchar = resultBuilder(getSession(),
                VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "decimal(38,0)", "", "")
                .row("custkey", "decimal(38,0)", "", "")
                .row("orderstatus", "varchar(1)", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar(15)", "", "")
                .row("clerk", "varchar(15)", "", "")
                .row("shippriority", "decimal(38,0)", "", "")
                .row("comment", "varchar(79)", "", "")
                .build();

        assertEquals(actual, expectedParametrizedVarchar);
    }

    @Override
    public void testDropColumn()
    {
        throw new SkipException("Drop column not yet implemented");
    }

    // Override needed because Snowflake casts INTEGER types up, allowing for large values which would normally fail coercion to pass.
    @Override
    public void testInsertWithCoercion()
    {
        String tableName = "test_insert_with_coercion_" + randomTableSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (" +
                "tinyint_column TINYINT, " +
                "integer_column INTEGER, " +
                "decimal_column DECIMAL(5, 3), " +
                "bounded_varchar_column VARCHAR(3), " +
                "unbounded_varchar_column VARCHAR, " +
                "date_column DATE)");

        assertUpdate("INSERT INTO " + tableName + " (tinyint_column, integer_column, decimal_column) VALUES (1e0, 2e0, 3e0)", 1);
        assertUpdate("INSERT INTO " + tableName + " (bounded_varchar_column, unbounded_varchar_column) VALUES (CAST('aa     ' AS varchar), CAST('aa     ' AS varchar))", 1);
        assertUpdate("INSERT INTO " + tableName + " (bounded_varchar_column, unbounded_varchar_column) VALUES (NULL, NULL)", 1);
        assertUpdate("INSERT INTO " + tableName + " (bounded_varchar_column, unbounded_varchar_column) VALUES (CAST(NULL AS varchar), CAST(NULL AS varchar))", 1);
        assertUpdate("INSERT INTO " + tableName + " (date_column) VALUES (TIMESTAMP '2019-11-18 22:13:40')", 1);

        assertQuery(
                "SELECT * FROM " + tableName,
                "VALUES " +
                        "(1, 2, 3, NULL, NULL, NULL), " +
                        "(NULL, NULL, NULL, 'aa ', 'aa     ', NULL), " +
                        "(NULL, NULL, NULL, NULL, NULL, NULL), " +
                        "(NULL, NULL, NULL, NULL, NULL, NULL), " +
                        "(NULL, NULL, NULL, NULL, NULL, DATE '2019-11-18')");

        assertQueryFails("INSERT INTO " + tableName + " (bounded_varchar_column) VALUES ('abcd')", "Cannot truncate non-space characters on INSERT");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDescribeInput()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "SELECT ? FROM nation WHERE nationkey = ? and name < ?")
                .build();
        MaterializedResult actual = computeActual(session, "DESCRIBE INPUT my_query");
        MaterializedResult expected = resultBuilder(session, BIGINT, VARCHAR)
                .row(0, "unknown")
                .row(1, "decimal(38,0)")
                .row(2, "varchar(25)")
                .build();
        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testDescribeOutput()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "SELECT * FROM nation")
                .build();

        MaterializedResult actual = computeActual(session, "DESCRIBE OUTPUT my_query");
        MaterializedResult expected = resultBuilder(session, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BOOLEAN)
                .row("nationkey", session.getCatalog().get(), session.getSchema().get(), "nation", "decimal(38,0)", 16, false)
                .row("name", session.getCatalog().get(), session.getSchema().get(), "nation", "varchar(25)", 0, false)
                .row("regionkey", session.getCatalog().get(), session.getSchema().get(), "nation", "decimal(38,0)", 16, false)
                .row("comment", session.getCatalog().get(), session.getSchema().get(), "nation", "varchar(152)", 0, false)
                .build();
        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testDescribeOutputNamedAndUnnamed()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "SELECT 1, name, regionkey AS my_alias FROM nation")
                .build();

        MaterializedResult actual = computeActual(session, "DESCRIBE OUTPUT my_query");
        MaterializedResult expected = resultBuilder(session, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BOOLEAN)
                .row("_col0", "", "", "", "integer", 4, false)
                .row("name", session.getCatalog().get(), session.getSchema().get(), "nation", "varchar(25)", 0, false)
                .row("my_alias", session.getCatalog().get(), session.getSchema().get(), "nation", "decimal(38,0)", 16, true)
                .build();
        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    @Override
    public void testInformationSchemaFiltering()
    {
        assertQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'orders' AND table_schema = 'test_schema' LIMIT 1",
                "SELECT 'orders'");
        assertQuery(
                "SELECT table_name FROM information_schema.columns WHERE data_type = 'decimal(38,0)' AND table_schema = 'test_schema' AND table_name = 'customer' and column_name = 'custkey' LIMIT 1",
                "SELECT 'customer'");
    }

    @Override
    public void testTableSampleBernoulli()
    {
        throw new SkipException("This test takes more than 10 minutes to finish.");
    }
}
