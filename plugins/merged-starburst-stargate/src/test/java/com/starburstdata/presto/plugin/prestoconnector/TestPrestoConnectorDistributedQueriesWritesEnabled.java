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

import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tpch.TpchTable;

import java.util.Map;

import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.createPrestoConnectorQueryRunner;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.createRemotePrestoQueryRunner;
import static com.starburstdata.presto.plugin.prestoconnector.PrestoConnectorQueryRunner.prestoConnectorConnectionUrl;

public class TestPrestoConnectorDistributedQueriesWritesEnabled
        extends BasePrestoConnectorDistributedQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner remotePresto = closeAfterClass(createRemotePrestoQueryRunner(
                Map.of(),
                false,
                TpchTable.getTables()));
        return createPrestoConnectorQueryRunner(
                true, // TODO split out test class version with writes disabled (default option)
                Map.of(),
                Map.of(
                        "connection-url", prestoConnectorConnectionUrl(remotePresto, "memory"),
                        "allow-drop-table", "true"));
    }
}
