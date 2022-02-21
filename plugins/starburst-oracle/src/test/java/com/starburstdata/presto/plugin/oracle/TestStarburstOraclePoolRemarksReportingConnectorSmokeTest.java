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
import io.trino.plugin.oracle.TestOraclePoolRemarksReportingConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

// Oracle tests with remarks enabled are executed single threaded due to ORA-12847
// that happens on concurrent DDL operations.
@Test(singleThreaded = true)
public class TestStarburstOraclePoolRemarksReportingConnectorSmokeTest
        extends TestOraclePoolRemarksReportingConnectorSmokeTest
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
                        .put("oracle.remarks-reporting.enabled", "true")
                        .buildOrThrow())
                .withTables(REQUIRED_TPCH_TABLES)
                .build();
    }
}
