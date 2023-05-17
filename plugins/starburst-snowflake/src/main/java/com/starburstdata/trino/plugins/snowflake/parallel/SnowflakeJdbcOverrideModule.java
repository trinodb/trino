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

import com.starburstdata.trino.plugins.snowflake.SnowflakeConfig;
import com.starburstdata.trino.plugins.snowflake.jdbc.SnowflakeJdbcClientModule;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import net.snowflake.client.jdbc.SnowflakeDriver;

public class SnowflakeJdbcOverrideModule
        extends SnowflakeJdbcClientModule
{
    public SnowflakeJdbcOverrideModule()
    {
        super(false);
    }

    @Override
    protected ConnectionFactory getDriverConnectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider, SnowflakeConfig snowflakeConfig)
    {
        return new ParallelWarehouseAwareDriverConnectionFactory(
                new SnowflakeDriver(),
                config.getConnectionUrl(),
                getConnectionProperties(snowflakeConfig),
                credentialProvider);
    }
}
