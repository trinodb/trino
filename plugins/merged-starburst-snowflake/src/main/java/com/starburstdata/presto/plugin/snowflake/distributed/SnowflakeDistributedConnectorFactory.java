/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake.distributed;

import com.google.inject.Injector;
import com.starburstdata.presto.license.LicenseManager;
import io.airlift.bootstrap.Bootstrap;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.type.TypeManager;

import java.util.Map;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

public class SnowflakeDistributedConnectorFactory
        implements ConnectorFactory
{
    private final String name;
    private final LicenseManager licenseManager;

    public SnowflakeDistributedConnectorFactory(String name, LicenseManager licenseManager)
    {
        this.name = requireNonNull(name, "name is null");
        this.licenseManager = requireNonNull(licenseManager, "licenseManager is null");
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public Connector create(String catalogName, Map<String, String> requiredConfig, ConnectorContext context)
    {
        requireNonNull(requiredConfig, "requiredConfig is null");

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            Bootstrap app = new Bootstrap(
                    new SnowflakeDistributedModule(catalogName, licenseManager),
                    binder -> binder.bind(LicenseManager.class).toInstance(licenseManager),
                    binder -> binder.bind(TypeManager.class).toInstance(context.getTypeManager()));

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(requiredConfig)
                    .initialize();

            return injector.getInstance(Connector.class);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
