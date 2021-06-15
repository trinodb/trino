/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate;

import com.starburstdata.presto.plugin.jdbc.StarburstJdbcMetadataFactory;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcMetadata;
import io.trino.plugin.jdbc.JdbcMetadataConfig;

import javax.inject.Inject;

public class StarburstRemoteMetadataFactory
        extends StarburstJdbcMetadataFactory
{
    @Inject
    public StarburstRemoteMetadataFactory(JdbcClient jdbcClient, JdbcMetadataConfig config)
    {
        super(jdbcClient, config);
    }

    @Override
    protected JdbcMetadata create(JdbcClient transactionCachingJdbcClient, boolean allowDropTable)
    {
        return new StarburstRemoteMetadata(transactionCachingJdbcClient, allowDropTable);
    }
}
