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

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testng.services.ManageTestResources;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.distributedBuilder;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeQueryRunner.impersonationDisabled;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeServer.TEST_DATABASE;

public class TestDistributedSnowflakeLargeScan
        extends AbstractTestQueryFramework
{
    @ManageTestResources.Suppress(because = "Mock to remote server")
    protected final SnowflakeServer server = new SnowflakeServer();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return distributedBuilder()
            .withServer(server)
            .withDatabase(Optional.of(TEST_DATABASE))
            .withSchema(Optional.empty())
            .withConnectorProperties(impersonationDisabled())
            .build();
    }

    @Test
    public void testLargeTableScan()
    {
        // The rest of the tests use test TPCH data loaded into a temporary database that gets cleaned up
        // However because TPCH_SF10 is large this table was created manually in the static db TEST_DB
        // Use "CREATE TABLE TEST_DB.TPCH_SF10.lineitem AS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.lineitem" in Snowflake UI to create test table
        assertQuery("SELECT " +
                        "count(l_orderkey + 1), " +
                        "count(l_partkey + 1), " +
                        "count(l_suppkey + 1), " +
                        "count(l_linenumber + 1), " +
                        "count(l_quantity + 1), " +
                        "count(l_extendedprice + 1), " +
                        "count(l_discount + 1), " +
                        "count(l_tax + 1), " +
                        "count(l_returnflag || 'F'), " +
                        "count(l_linestatus || 'F'), " +
                        "count(l_shipdate + interval '1' day), " +
                        "count(l_commitdate + interval '1' day), " +
                        "count(l_receiptdate + interval '1' day), " +
                        "count(l_shipinstruct || 'F'), " +
                        "count(l_shipmode || 'F'), " +
                        "count(l_comment || 'F') " +
                        "FROM tpch_sf10.lineitem",
                "VALUES (59986052, 59986052, 59986052, 59986052, 59986052, 59986052, 59986052, 59986052, " +
                        "59986052, 59986052, 59986052, 59986052, 59986052, 59986052, 59986052, 59986052)");
    }
}
