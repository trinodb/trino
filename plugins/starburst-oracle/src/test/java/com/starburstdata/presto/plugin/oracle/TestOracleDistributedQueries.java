/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.oracle;

import com.google.common.collect.ImmutableMap;
import io.prestosql.testing.AbstractTestDistributedQueries;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.sql.JdbcSqlExecutor;
import io.prestosql.testing.sql.TestTable;
import org.testng.SkipException;

import java.util.Optional;
import java.util.Properties;

import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.MaterializedResult.resultBuilder;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestOracleDistributedQueries
        extends AbstractTestDistributedQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return OracleQueryRunner.builder()
                .withConnectorProperties(ImmutableMap.<String, String>builder()
                        .putAll(TestingStarburstOracleServer.connectionProperties())
                        .put("oracle.connection-pool.enabled", "true")
                        .put("oracle.connection-pool.max-size", "10")
                        .put("allow-drop-table", "true")
                        .build())
                .build();
    }

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
    public void testShowColumns()
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");

        MaterializedResult expectedParametrizedVarchar = resultBuilder(getSession(),
                VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "decimal(19,0)", "", "")
                .row("custkey", "decimal(19,0)", "", "")
                .row("orderstatus", "varchar(1)", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "timestamp(3)", "", "")
                .row("orderpriority", "varchar(15)", "", "")
                .row("clerk", "varchar(15)", "", "")
                .row("shippriority", "decimal(10,0)", "", "")
                .row("comment", "varchar(79)", "", "")
                .build();

        assertEquals(actual, expectedParametrizedVarchar);
    }

    @Override
    public void testInformationSchemaFiltering()
    {
        assertQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'orders' LIMIT 1",
                "SELECT 'orders' table_name");
        assertQuery(
                "SELECT table_name FROM information_schema.columns WHERE data_type = 'decimal(19,0)' AND table_name = 'customer' and column_name = 'custkey' LIMIT 1",
                "SELECT 'customer' table_name");
    }

    @Override
    public void testCreateSchema()
    {
        // does not support creating schemas
        assertQueryFails("CREATE SCHEMA test_schema_create", "This connector does not support creating schemas");
    }

    @Override
    public void testInsertWithCoercion()
    {
        // super.testInsertWithCoercion assumes DATE and TIMESTAMP are different types, but in Oracle connector DATE is mapped to TIMESTAMP TODO reimplement test
        throw new SkipException("reimplement test");
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                createJdbcSqlExecutor(),
                "test_table_with_default_columns",
                "(col_required NUMBER(15) NOT NULL," +
                        "col_nullable NUMBER(15)," +
                        "col_default NUMBER(15) DEFAULT 43," +
                        "col_nonnull_default NUMBER(15) DEFAULT 42 NOT NULL," +
                        "col_required2 NUMBER(15) NOT NULL)");
    }

    @Override
    public void testColumnName(String columnName)
    {
        if (columnName.equals("a\"quote")) {
            // Quote is not supported within column name
            assertThatThrownBy(() -> super.testColumnName(columnName))
                    .hasMessageMatching("Oracle does not support escaping '\"' in identifiers");
            throw new SkipException("works incorrectly, column name is trimmed");
        }

        super.testColumnName(columnName);
    }

    @Override
    protected Optional<AbstractTestDistributedQueries.DataMappingTestSetup> filterDataMappingSmokeTestData(AbstractTestDistributedQueries.DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getPrestoTypeName();
        if (typeName.equals("time")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }

        return Optional.of(dataMappingTestSetup);
    }

    private JdbcSqlExecutor createJdbcSqlExecutor()
    {
        Properties properties = new Properties();
        properties.setProperty("user", OracleTestUsers.USER);
        properties.setProperty("password", OracleTestUsers.PASSWORD);
        return new JdbcSqlExecutor(TestingStarburstOracleServer.getJdbcUrl(), properties);
    }
}
