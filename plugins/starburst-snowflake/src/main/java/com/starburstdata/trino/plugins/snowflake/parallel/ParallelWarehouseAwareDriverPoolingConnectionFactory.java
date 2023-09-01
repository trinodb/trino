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

import com.google.common.collect.ImmutableMap;
import com.starburstdata.trino.plugins.jdbc.JdbcConnectionPoolConfig;
import com.starburstdata.trino.plugins.snowflake.jdbc.WarehouseAwareDriverPoolingConnectionFactory;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.IdentityCacheMapping;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.spi.connector.ConnectorSession;

import java.util.Map;
import java.util.Properties;

import static com.starburstdata.trino.plugins.snowflake.parallel.SnowflakeParallelSessionProperties.CLIENT_RESULT_CHUNK_SIZE;
import static com.starburstdata.trino.plugins.snowflake.parallel.SnowflakeParallelSessionProperties.QUOTED_IDENTIFIERS_IGNORE_CASE;
import static com.starburstdata.trino.plugins.snowflake.parallel.SnowflakeParallelSessionProperties.getQuotedIdentifiersIgnoreCase;
import static com.starburstdata.trino.plugins.snowflake.parallel.SnowflakeParallelSessionProperties.getResultChunkSize;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class ParallelWarehouseAwareDriverPoolingConnectionFactory
        extends WarehouseAwareDriverPoolingConnectionFactory
{
    public ParallelWarehouseAwareDriverPoolingConnectionFactory(
            String catalogName,
            Properties connectionProperties,
            BaseJdbcConfig config,
            JdbcConnectionPoolConfig poolConfig,
            CredentialProvider credentialProvider,
            IdentityCacheMapping identityCacheMapping)
    {
        super(
                catalogName,
                connectionProperties,
                config,
                poolConfig,
                credentialProvider,
                identityCacheMapping);
    }

    @Override
    protected Map<String, String> getConnectionProperties(ConnectorSession session)
    {
        ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.<String, String>builder()
                .putAll(super.getConnectionProperties(session));
        getResultChunkSize(session)
                .ifPresent(chunkSize -> propertiesBuilder.put(CLIENT_RESULT_CHUNK_SIZE, chunkSize.to(MEGABYTE).toString()));
        propertiesBuilder.put(QUOTED_IDENTIFIERS_IGNORE_CASE, Boolean.toString(getQuotedIdentifiersIgnoreCase(session)));
        return propertiesBuilder.buildKeepingLast();
    }
}
