/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.prestoconnector;

import com.starburstdata.presto.plugin.jdbc.dynamicfiltering.AbstractDynamicFilteringTest;
import io.prestosql.testing.QueryRunner;

import java.util.List;
import java.util.Map;

import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.createPrestoConnectorLoopbackQueryRunner;
import static io.prestosql.tpch.TpchTable.ORDERS;

public class TestPrestoConnectorDynamicFiltering
        extends AbstractDynamicFilteringTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createPrestoConnectorLoopbackQueryRunner(
                3,
                Map.of(),
                false,
                Map.of(),
                List.of(ORDERS));
    }

    @Override
    protected boolean supportsSplitDynamicFiltering()
    {
        // JDBC connectors always generate single split
        // TODO https://starburstdata.atlassian.net/browse/PRESTO-4769 revisit in parallel Presto Connector
        return false;
    }
}
