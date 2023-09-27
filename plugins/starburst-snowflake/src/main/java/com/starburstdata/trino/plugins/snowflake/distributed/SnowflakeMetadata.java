/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake.distributed;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.jdbc.DefaultJdbcMetadata;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcQueryEventListener;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.RetryMode;

import java.util.Optional;
import java.util.Set;

import static com.starburstdata.trino.plugins.snowflake.jdbc.SnowflakeClient.checkColumnsForInvalidCharacters;

public class SnowflakeMetadata
        extends DefaultJdbcMetadata
{
    public SnowflakeMetadata(JdbcClient jdbcClient, Set<JdbcQueryEventListener> jdbcQueryEventListeners)
    {
        super(jdbcClient, true, jdbcQueryEventListeners);
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode)
    {
        checkColumnsForInvalidCharacters(tableMetadata.getColumns());
        return super.beginCreateTable(session, tableMetadata, layout, retryMode);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        checkColumnsForInvalidCharacters(tableMetadata.getColumns());
        super.createTable(session, tableMetadata, ignoreExisting);
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle table, ColumnMetadata columnMetadata)
    {
        checkColumnsForInvalidCharacters(ImmutableList.of(columnMetadata));
        super.addColumn(session, table, columnMetadata);
    }
}
