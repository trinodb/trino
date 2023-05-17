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

import com.starburstdata.trino.plugins.snowflake.jdbc.WarehouseAwareDriverConnectionFactory;
import io.airlift.units.DataSize;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;

import java.sql.Driver;
import java.util.Properties;

import static com.starburstdata.trino.plugins.snowflake.jdbc.SnowflakeJdbcSessionProperties.WAREHOUSE;
import static com.starburstdata.trino.plugins.snowflake.jdbc.SnowflakeJdbcSessionProperties.getWarehouse;
import static com.starburstdata.trino.plugins.snowflake.parallel.SnowflakeParallelSessionProperties.CLIENT_RESULT_CHUNK_SIZE;
import static com.starburstdata.trino.plugins.snowflake.parallel.SnowflakeParallelSessionProperties.getResultChunkSize;

public class ParallelWarehouseAwareDriverConnectionFactory
        extends WarehouseAwareDriverConnectionFactory
{
    public ParallelWarehouseAwareDriverConnectionFactory(
            Driver driver,
            String connectionUrl,
            Properties connectionProperties,
            CredentialProvider credentialProvider)
    {
        super(driver, connectionUrl, connectionProperties, credentialProvider);
    }

    @Override
    protected Properties getConnectionProperties(ConnectorSession session)
    {
        ConnectorIdentity identity = session.getIdentity();
        Properties properties = new Properties();
        properties.putAll(connectionProperties);
        properties.putAll(credentialPropertiesProvider.getCredentialProperties(identity));
        getWarehouse(session).ifPresent(warehouse -> properties.put(WAREHOUSE, warehouse));
        getResultChunkSize(session).ifPresent(chunkSize -> properties.put(CLIENT_RESULT_CHUNK_SIZE, chunkSize.to(DataSize.Unit.MEGABYTE)));
        return properties;
    }
}
