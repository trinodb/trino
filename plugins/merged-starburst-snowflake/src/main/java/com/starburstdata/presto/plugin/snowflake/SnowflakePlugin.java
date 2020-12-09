/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.starburstdata.presto.license.LicenceCheckingConnectorFactory;
import com.starburstdata.presto.license.LicenseManager;
import com.starburstdata.presto.license.LicenseManagerProvider;
import com.starburstdata.presto.plugin.snowflake.distributed.SnowflakeDistributedConnectorFactory;
import com.starburstdata.presto.plugin.snowflake.jdbc.SnowflakeJdbcClientModule;
import io.prestosql.plugin.jdbc.JdbcConnectorFactory;
import io.prestosql.plugin.jdbc.JdbcConnectorFactory.JdbcModuleProvider;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.starburstdata.presto.license.StarburstPrestoFeature.SNOWFLAKE;
import static io.airlift.configuration.ConfigurationAwareModule.combine;

public class SnowflakePlugin
        implements Plugin
{
    static final String SNOWFLAKE_JDBC = "snowflake-jdbc";
    static final String SNOWFLAKE_DISTRIBUTED = "snowflake-distributed";

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return getConnectorFactoriesWithLicensing(new LicenseManagerProvider().get()).stream()
                .map(connectorFactory -> new LicenceCheckingConnectorFactory(SNOWFLAKE, connectorFactory))
                .collect(toImmutableList());
    }

    @VisibleForTesting
    List<ConnectorFactory> getConnectorFactoriesWithLicensing(LicenseManager licenseManager)
    {
        return ImmutableList.of(
                new JdbcConnectorFactory(
                        SNOWFLAKE_JDBC,
                        (JdbcModuleProvider) catalogName -> combine(
                                binder -> binder.bind(LicenseManager.class).toInstance(licenseManager),
                                new SnowflakeJdbcClientModule(catalogName, false))),
                new SnowflakeDistributedConnectorFactory(SNOWFLAKE_DISTRIBUTED, licenseManager));
    }
}
