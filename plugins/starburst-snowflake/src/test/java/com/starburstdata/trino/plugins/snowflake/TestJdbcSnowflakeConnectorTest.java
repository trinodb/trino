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
import io.trino.spi.security.Identity;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.impersonationDisabled;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.jdbcBuilder;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeServer.TEST_DATABASE;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertFalse;

public class TestJdbcSnowflakeConnectorTest
        extends BaseSnowflakeConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return jdbcBuilder()
                .withServer(server)
                .withDatabase(Optional.of(testDatabase.getName()))
                .withSchema(Optional.of(TEST_SCHEMA))
                .withConnectorProperties(impersonationDisabled())
                .withConnectorProperties(Map.of("metadata.cache-ttl", "5m"))
                .withTpchTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    @Test
    public void testTimestampWithTimezoneValues()
    {
        // Snowflake's JDBC does not correctly represent datetimes with negative year
        testTimestampWithTimezoneValues(false);
    }

    @Test
    public void testSnowflakeUseDefaultUserWarehouseAndDatabase()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = jdbcBuilder()
                .withWarehouse(Optional.empty())
                .withDatabase(Optional.empty())
                .withConnectorProperties(impersonationDisabled())
                .build()) {
            Session session = Session.builder(queryRunner.getDefaultSession())
                    .setIdentity(Identity.ofUser(SnowflakeServer.USER))
                    .build();
            String tableName = TEST_SCHEMA + ".test_insert_";
            // this test uses the role: test_role whose default database is "TEST_DATABASE"
            try (TestTable testTable = new TestTable(sql -> server.safeExecuteOnDatabase(TEST_DATABASE, sql),
                    tableName,
                    "(x decimal(19, 0), y varchar(100))",
                    ImmutableList.of("123, 'test'"))) {
                queryRunner.execute(session, format("SELECT * FROM %s", testTable.getName()));
            }
        }
    }

    @Override
    public void testInsertRowConcurrently()
    {
        // TODO: Skip slow Snowflake insert tests (https://starburstdata.atlassian.net/browse/SEP-9214)
        throw new SkipException("Snowflake INSERTs are slow and the futures sometimes timeout in the test. See https://starburstdata.atlassian.net/browse/SEP-9214.");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        if (dataMappingTestSetup.getTrinoTypeName().equals("date")) {
            // TODO (https://starburstdata.atlassian.net/browse/SEP-7956) Fix incorrect date issue in Snowflake
            if (dataMappingTestSetup.getSampleValueLiteral().equals("DATE '1582-10-05'")) {
                return Optional.empty();
            }
        }
        return super.filterDataMappingSmokeTestData(dataMappingTestSetup);
    }

    // trino analyze stage passes without exceptions
    // Snowflake throws tested exception
    // TODO This is wrong !!! Trino should not allow query to execute on the underlying system
    @Override
    public void testNativeQueryCreateStatement()
    {
        String tableName = getSession().getSchema().orElseThrow() + ".numbers";
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
        assertThatThrownBy(() -> query(format("SELECT * FROM TABLE(system.query(query => 'CREATE TABLE %s(n INTEGER)'))", tableName)))
                .hasMessageContaining("unexpected 'CREATE'");
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    // trino analyze stage passes without exceptions
    // Snowflake throws tested exception
    // TODO This is wrong !!! Trino should not allow query to execute on the underlying system
    @Override
    public void testNativeQueryInsertStatementTableExists()
    {
        try (TestTable testTable = simpleTable()) {
            assertThatThrownBy(() -> query(format("SELECT * FROM TABLE(system.query(query => 'INSERT INTO %s VALUES (3)'))", testTable.getName())))
                    .hasMessageContaining("unexpected 'INSERT'");
            assertQuery("SELECT * FROM " + testTable.getName(), "VALUES 1, 2");
        }
    }
}
