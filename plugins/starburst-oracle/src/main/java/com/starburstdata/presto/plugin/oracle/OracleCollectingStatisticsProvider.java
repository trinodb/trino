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

import com.starburstdata.managed.statistics.CollectedStatisticsType;
import com.starburstdata.presto.plugin.jdbc.statistics.JdbcCollectingStatisticsProvider;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcSplit;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.TupleDomain;

import javax.inject.Inject;

import java.util.Optional;

import static com.starburstdata.managed.statistics.CollectedStatisticsType.DISTINCT_VALUES;

public class OracleCollectingStatisticsProvider
        extends JdbcCollectingStatisticsProvider
{
    private static final OracleSplit SINGLE_SPLIT = new OracleSplit(Optional.empty(), Optional.empty(), TupleDomain.all());

    @Inject
    public OracleCollectingStatisticsProvider(JdbcClient client)
    {
        super(client);
    }

    @Override
    protected JdbcSplit getSplit()
    {
        return SINGLE_SPLIT;
    }

    @Override
    protected boolean isTypeApplicable(ConnectorSession session, JdbcColumnHandle column, CollectedStatisticsType collectedStatisticsType)
    {
        String jdbcTypeName = column.getJdbcTypeHandle().getJdbcTypeName().orElse("");
        // blob clob doesn't support DISTINCT keyword, so omit this stat
        if (jdbcTypeName.equals("BLOB") || jdbcTypeName.equals("CLOB") || jdbcTypeName.equals("NCLOB")) {
            if (collectedStatisticsType == DISTINCT_VALUES) {
                return false;
            }
        }
        return super.isTypeApplicable(session, column, collectedStatisticsType);
    }
}
