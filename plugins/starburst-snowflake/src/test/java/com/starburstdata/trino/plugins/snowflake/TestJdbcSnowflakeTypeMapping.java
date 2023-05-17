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
import io.trino.testing.QueryRunner;
import io.trino.testing.datatype.SqlDataTypeTest;
import io.trino.testing.sql.TestTable;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.impersonationDisabled;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.jdbcBuilder;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestJdbcSnowflakeTypeMapping
        extends BaseSnowflakeTypeMappingTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createBuilder()
                .withServer(server)
                .withDatabase(Optional.of(testDatabase.getName()))
                .withSchema(Optional.of(TEST_SCHEMA))
                .withConnectorProperties(impersonationDisabled())
                .build();
    }

    protected SnowflakeQueryRunner.Builder createBuilder()
    {
        return jdbcBuilder();
    }

    @Deprecated // TODO https://starburstdata.atlassian.net/browse/SEP-10002
    @Test
    public void testTimestampMappingDataCorruption()
    {
        String expectedValue = "0000-01-01 00:00:00.000000000";
        String actualValue = "0001-01-01 00:00:00.000000000";
        // write by trino. in snowflake 0001
        try (TestTable table = new TestTable(sqls -> getQueryRunner().execute(sqls), "test_schema_2.test_timestamp", "(c1 timestamp(9))", ImmutableList.of(format("TIMESTAMP '%s'", expectedValue)))) {
            assertQuery("SELECT * FROM " + table.getName(), format("VALUES CAST('%s' AS timestamp(9))", actualValue)); // this test should fail
            // predicate passes because expectedValue 0000 is transformed to actual value 0001 on trino side
            assertQuery(format("SELECT count(*) FROM %s WHERE c1 = CAST('%s' AS timestamp(9))", table.getName(), expectedValue), "VALUES 1");
        }
        // write by snowflake. in snowflake 0000
        try (TestTable table = new TestTable(sqls -> getSqlExecutor().execute(sqls), "test_timestamp", "(c1 timestamp_ntz(9))", ImmutableList.of(format("'%s'", expectedValue)))) {
            assertQuery("SELECT * FROM " + table.getName(), format("VALUES CAST('%s' AS timestamp(9))", actualValue)); // this test should fail
            assertThat(getQueryRunner().execute(format("SELECT * FROM %s WHERE c1 = CAST('%s' AS timestamp(9))", table.getName(), expectedValue)).getRowCount()).isEqualTo(0); // this test should fail
        }
    }

    @Override
    @Deprecated // TODO https://starburstdata.atlassian.net/browse/SEP-9994
    @Test
    public void testTimestampMappingNegative()
    {
        super.testTimestampMappingNegative();
        SqlDataTypeTest.create()
                // data corruption - negative overflow
                .addRoundTrip("timestamp(9)", "TIMESTAMP '-0001-01-01 00:00:00.000000000'", createTimestampType(9), "TIMESTAMP '0002-01-01 00:00:00.000000000'")
                .execute(getQueryRunner(), trinoCreateAsSelect())
                .execute(getQueryRunner(), trinoCreateAndInsert());
    }
}
