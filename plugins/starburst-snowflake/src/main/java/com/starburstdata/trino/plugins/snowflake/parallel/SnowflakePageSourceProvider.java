/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake.parallel;

import io.airlift.log.Logger;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.RecordPageSource;

import javax.inject.Inject;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class SnowflakePageSourceProvider
        implements ConnectorPageSourceProvider
{
    private static final Logger log = Logger.get(SnowflakePageSourceProvider.class);
    private final ConnectorRecordSetProvider recordSetProvider;
    private final StarburstResultStreamProvider starburstResultStreamProvider;

    @Inject
    public SnowflakePageSourceProvider(ConnectorRecordSetProvider recordSetProvider, StarburstResultStreamProvider starburstResultStreamProvider)
    {
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
        this.starburstResultStreamProvider = requireNonNull(starburstResultStreamProvider, "starburstResultStreamProvider is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        log.debug("createPageSource(transaction=%s, session=%s, split=%s, table=%s, columns=%s)", transaction, session, split, table, columns);
        if (split instanceof SnowflakeArrowSplit snowflakeArrowSplit) {
            List<JdbcColumnHandle> jdbcColumnHandles = columns.stream()
                    .map(JdbcColumnHandle.class::cast)
                    .collect(toImmutableList());
            return new SnowflakeArrowPageSource(
                    snowflakeArrowSplit,
                    jdbcColumnHandles,
                    starburstResultStreamProvider);
        }
        return new RecordPageSource(recordSetProvider.getRecordSet(transaction, session, split, table, columns));
    }
}
