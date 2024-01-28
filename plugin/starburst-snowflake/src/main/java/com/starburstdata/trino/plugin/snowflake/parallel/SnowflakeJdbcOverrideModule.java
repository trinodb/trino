/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.snowflake.parallel;

import com.starburstdata.trino.plugin.jdbc.JdbcConnectionPoolConfig;
import com.starburstdata.trino.plugin.snowflake.jdbc.SnowflakeJdbcClientModule;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.IdentityCacheMapping;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.spi.catalog.CatalogName;
import net.snowflake.client.jdbc.SnowflakeDriver;

import java.util.Properties;

import static com.starburstdata.trino.plugin.snowflake.SnowflakeConnectorFlavour.PARALLEL;

public class SnowflakeJdbcOverrideModule
        extends SnowflakeJdbcClientModule
{
    public SnowflakeJdbcOverrideModule()
    {
        super(PARALLEL);
    }

    @Override
    protected ConnectionFactory getDriverConnectionFactory(
            BaseJdbcConfig config,
            CredentialProvider credentialProvider,
            CatalogName catalogName,
            JdbcConnectionPoolConfig connectionPoolingConfig,
            IdentityCacheMapping identityCacheMapping,
            Properties connectionProperties)
    {
        if (connectionPoolingConfig.isConnectionPoolEnabled()) {
            return new ParallelWarehouseAwareDriverPoolingConnectionFactory(
                    catalogName.toString(),
                    connectionProperties,
                    config,
                    connectionPoolingConfig,
                    credentialProvider,
                    identityCacheMapping);
        }
        return new ParallelWarehouseAwareDriverConnectionFactory(
                new SnowflakeDriver(),
                config.getConnectionUrl(),
                connectionProperties,
                credentialProvider);
    }
}
