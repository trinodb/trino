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

import com.starburstdata.managed.statistics.CollectedStatisticsType;
import com.starburstdata.presto.plugin.jdbc.statistics.JdbcCollectingStatisticsProvider;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.spi.connector.ConnectorSession;

import javax.inject.Inject;

public class SqlServerCollectingStatisticsProvider
        extends JdbcCollectingStatisticsProvider
{
    private static final String DATA_SIZE_QUERY = "CAST(SUM(CAST(LEN(%s) AS %s)) AS %s)".formatted(COLUMN_PLACEHOLDER, COLUMN_TYPE_PLACEHOLDER, COLUMN_TYPE_PLACEHOLDER);

    @Inject
    public SqlServerCollectingStatisticsProvider(JdbcClient client)
    {
        super(client);
    }

    @Override
    protected boolean isTypeApplicable(ConnectorSession session, JdbcColumnHandle column, CollectedStatisticsType collectedStatisticsType)
    {
        // we cannot use LEN, COUNT on text columns in SqlServer, so omit this columns
        String jdbcTypeName = column.getJdbcTypeHandle().getJdbcTypeName().orElse("");
        if (jdbcTypeName.equals("text") || jdbcTypeName.equals("ntext")) {
            return false;
        }
        return super.isTypeApplicable(session, column, collectedStatisticsType);
    }

    @Override
    protected String getDataSizeQuery()
    {
        return DATA_SIZE_QUERY;
    }
}
