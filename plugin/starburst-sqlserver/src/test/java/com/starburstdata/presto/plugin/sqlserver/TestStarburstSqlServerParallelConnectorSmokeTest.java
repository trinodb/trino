/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.sqlserver;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.sqlserver.BaseSqlServerConnectorSmokeTest;
import io.trino.plugin.sqlserver.TestingSqlServer;
import io.trino.testing.QueryRunner;

import static org.assertj.core.api.Assertions.assertThat;

public class TestStarburstSqlServerParallelConnectorSmokeTest
        extends BaseSqlServerConnectorSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingSqlServer sqlServer = closeAfterClass(new TestingSqlServer());
        return StarburstSqlServerQueryRunner.builder(sqlServer)
                .withEnterpriseFeatures()
                .withConnectorProperties(ImmutableMap.of("sqlserver.parallel.connections-count", "4"))
                .withPartitionedTables()
                .withTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .isEqualTo("""
                        CREATE TABLE sqlserver.dbo.region (
                           regionkey bigint,
                           name varchar(25),
                           comment varchar(152)
                        )""");
    }
}
