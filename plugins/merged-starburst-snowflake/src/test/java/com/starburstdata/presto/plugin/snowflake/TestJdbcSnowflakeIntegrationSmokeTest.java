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
import io.prestosql.spi.security.Identity;
import io.prestosql.testing.QueryRunner;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.impersonationDisabled;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.jdbcBuilder;
import static io.prestosql.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;

public class TestJdbcSnowflakeIntegrationSmokeTest
        extends BaseSnowflakeIntegrationSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return jdbcBuilder()
                .withServer(server)
                .withAdditionalProperties(impersonationDisabled())
                .withConnectionPooling()
                .build();
    }

    // currently the JDBC connector cannot read timestamps greater than 9999-12-31 23:59:59.999
    @Override
    @Test
    public void testTimestampWithTimezoneValues()
    {
    }

    @Test
    public void testSnowflakeUseDefaultUserWarehouseAndDatabase()
            throws Exception
    {
        try (QueryRunner queryRunner = jdbcBuilder()
                .withWarehouse(Optional.empty())
                .withDatabase(Optional.empty())
                .withAdditionalProperties(impersonationDisabled())
                .build()) {
            Session session = Session.builder(queryRunner.getDefaultSession())
                    .setIdentity(Identity.ofUser(SnowflakeServer.USER))
                    .build();
            String tableName = "test_insert_" + randomTableSuffix();
            queryRunner.execute(session, format("CREATE TABLE test_schema.%s (x decimal(19, 0), y varchar(100))", tableName));
            queryRunner.execute(session, format("INSERT INTO %s VALUES (123, 'test')", tableName));
            queryRunner.execute(session, format("SELECT * FROM %s", tableName));
            queryRunner.execute(session, format("DROP TABLE test_schema.%s", tableName));
        }
    }
}
