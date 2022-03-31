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

import io.trino.plugin.jdbc.DefaultJdbcMetadataFactory;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcMetadata;

import javax.inject.Inject;

public class StargateMetadataFactory
        extends DefaultJdbcMetadataFactory
{
    @Inject
    public StargateMetadataFactory(JdbcClient jdbcClient)
    {
        super(jdbcClient);
    }

    @Override
    protected JdbcMetadata create(JdbcClient transactionCachingJdbcClient)
    {
        return new StargateMetadata(transactionCachingJdbcClient);
    }
}
