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
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

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
        Type columnType = column.getColumnType();
        if (collectedStatisticsType == DISTINCT_VALUES) {
            // unlimited varchar type presented in Oracle as clob type,
            // which doesn't support DISTINCT keyword, so omit this stat
            if (columnType instanceof VarcharType varcharType && varcharType.getLength().isEmpty()) {
                return false;
            }
            // varbinary presented as blob, which doesn't support DISTINCT keyword
            if (columnType instanceof VarbinaryType) {
                return false;
            }
        }
        return super.isTypeApplicable(session, column, collectedStatisticsType);
    }
}
