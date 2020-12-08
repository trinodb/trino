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
import com.starburstdata.presto.plugin.jdbc.redirection.AbstractTableScanRedirectionTest;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tpch.TpchTable;

import static com.starburstdata.presto.plugin.oracle.OracleTestUsers.USER;

public class TestOracleTableScanRedirection
        extends AbstractTableScanRedirectionTest
{
    @Override
    protected QueryRunner createQueryRunner(Iterable<TpchTable<?>> tables)
            throws Exception
    {
        return OracleQueryRunner.builder()
                .withConnectorProperties(ImmutableMap.<String, String>builder()
                        .putAll(TestingStarburstOracleServer.connectionProperties())
                        .putAll(getRedirectionProperties("oracle", USER))
                        .build())
                .withTables(tables)
                .withUnlockEnterpriseFeatures(true)
                .build();
    }
}
