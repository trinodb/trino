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

import com.starburstdata.presto.testing.Closer;
import io.trino.plugin.jdbc.BaseJdbcConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Optional;

import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.impersonationEnabled;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.jdbcBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestJdbcSnowflakeWithImpersonationConnectorSmokeTest
        extends BaseJdbcConnectorSmokeTest
{
    private final SnowflakeServer server = new SnowflakeServer();
    private final Closer closer = Closer.create();
    private final TestDatabase databaseOne = closer.register(server.createDatabase("SMOKE_TEST_ONE"));
    private final TestDatabase databaseTwo = closer.register(server.createDatabase("SMOKE_TEST_TWO"));

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return jdbcBuilder()
                .withConnectorProperties(impersonationEnabled())
                .withDatabase(Optional.of(databaseOne.getName()))
                .withSchema(Optional.of(TEST_SCHEMA))
                .withServer(server)
                .withTpchTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
            throws IOException
    {
        closer.close();
    }

    @Test
    public void testAmbiguousTableInMultipleDatabases()
            throws SQLException
    {
        final String schemaName = "ambiguous_schema";
        final String tableName = "table_name";

        server.createSchema(databaseOne.getName(), schemaName);
        server.createSchema(databaseTwo.getName(), schemaName);

        String createTableDDL = "CREATE OR REPLACE TABLE %s.%s.%s as %s";
        server.executeOnDatabase(databaseOne.getName(), format(createTableDDL, databaseOne.getName(), schemaName, tableName, "SELECT 1 as col"));
        server.executeOnDatabase(databaseOne.getName(), format(createTableDDL, databaseTwo.getName(), schemaName, tableName, "SELECT 2 as col"));

        // Should choose the table in databaseOne since the snowflake catalog is configured to use databaseOne
        Object result = getQueryRunner().execute(format("select * from snowflake.%s.%s limit 1", schemaName, tableName))
                .getMaterializedRows().get(0).getField(0);
        assertThat((BigDecimal) result).isEqualTo(new BigDecimal(1));
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeActual("SHOW CREATE TABLE region").getOnlyValue())
                .isEqualTo("CREATE TABLE snowflake.test_schema_2.region (\n" +
                        "   regionkey decimal(19, 0),\n" +
                        "   name varchar(25),\n" +
                        "   comment varchar(152)\n" +
                        ")");
    }
}
