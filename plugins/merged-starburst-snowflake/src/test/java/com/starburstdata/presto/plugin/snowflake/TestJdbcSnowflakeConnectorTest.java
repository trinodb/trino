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

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.spi.security.Identity;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.impersonationDisabled;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.jdbcBuilder;
import static java.lang.String.format;

public class TestJdbcSnowflakeConnectorTest
        extends BaseSnowflakeConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return jdbcBuilder()
                .withServer(server)
                .withConnectorProperties(impersonationDisabled())
                .withConnectionPooling()
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
            try (TestTable testTable = new TestTable(snowflakeExecutor, tableName, "(x decimal(19, 0), y varchar(100))", ImmutableList.of("123, 'test'"))) {
                queryRunner.execute(session, format("SELECT * FROM %s", testTable.getName()));
            }
        }
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
}
