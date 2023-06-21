/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.saphana;

import com.google.common.collect.ImmutableMap;
import com.starburstdata.presto.redirection.AbstractTableScanRedirectionTest;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

import static com.starburstdata.presto.plugin.saphana.SapHanaQueryRunner.createSapHanaQueryRunner;
import static io.trino.testing.TestingNames.randomNameSuffix;

public class TestSapHanaTableScanRedirection
        extends AbstractTableScanRedirectionTest
{
    protected TestingSapHanaServer server;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        String catalogName = "saphana_" + randomNameSuffix();
        server = closeAfterClass(TestingSapHanaServer.create());
        return createSapHanaQueryRunner(
                server,
                catalogName,
                getRedirectionProperties(catalogName, "tpch"),
                ImmutableMap.of(),
                TpchTable.getTables());
    }
}
