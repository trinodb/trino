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

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.starburstdata.trino.plugins.snowflake.SnowflakeConfig;
import com.starburstdata.trino.plugins.snowflake.SnowflakeProxyConfig;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.jdbc.ForJdbcDynamicFiltering;
import io.trino.plugin.jdbc.JdbcSplitManager;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import net.snowflake.client.core.HttpClientSettingsKey;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.OCSPMode;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.jdbc.JdbcModule.bindSessionPropertiesProvider;
import static java.lang.String.join;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class SnowflakeParallelModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        newOptionalBinder(binder, ConnectorPageSourceProvider.class)
                .setDefault().to(SnowflakePageSourceProvider.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, Key.get(ConnectorSplitManager.class, ForJdbcDynamicFiltering.class))
                .setBinding().to(SnowflakeSplitManager.class).in(SINGLETON);
        bindSessionPropertiesProvider(binder, SnowflakeParallelSessionProperties.class);
        binder.bind(SnowflakeParallelConnector.class).in(Scopes.SINGLETON);
        binder.bind(JdbcSplitManager.class).in(Scopes.SINGLETON);

        newOptionalBinder(binder, StarburstResultStreamProvider.class)
                .setDefault()
                .toProvider(DefaultStreamProvider.class)
                .in(SINGLETON);

        install(conditionalModule(
                SnowflakeConfig.class,
                SnowflakeConfig::isProxyEnabled,
                proxyBinder -> {
                    configBinder(proxyBinder).bindConfig(SnowflakeProxyConfig.class);
                    newOptionalBinder(proxyBinder, StarburstResultStreamProvider.class)
                            .setBinding()
                            .toProvider(ProxiedStreamProvider.class)
                            .in(SINGLETON);
                }));
    }

    public static class DefaultStreamProvider
            implements Provider<StarburstResultStreamProvider>
    {
        @Override
        public StarburstResultStreamProvider get()
        {
            return new StarburstResultStreamProvider(HttpUtil.getHttpClient(new HttpClientSettingsKey(OCSPMode.FAIL_OPEN)));
        }
    }

    public static class ProxiedStreamProvider
            implements Provider<StarburstResultStreamProvider>
    {
        private final SnowflakeProxyConfig snowflakeProxyConfig;

        @Inject
        ProxiedStreamProvider(SnowflakeProxyConfig snowflakeProxyConfig)
        {
            this.snowflakeProxyConfig = requireNonNull(snowflakeProxyConfig, "snowflakeProxyConfig is null");
        }

        @Override
        public StarburstResultStreamProvider get()
        {
            HttpClientSettingsKey clientSettingsKey = new HttpClientSettingsKey(
                    OCSPMode.FAIL_OPEN,
                    snowflakeProxyConfig.getProxyHost(),
                    snowflakeProxyConfig.getProxyPort(),
                    // see https://docs.snowflake.com/en/developer-guide/jdbc/jdbc-configure#bypassing-the-proxy-server
                    join("%7C", snowflakeProxyConfig.getNonProxyHosts()),
                    snowflakeProxyConfig.getUsername().orElse(null),
                    snowflakeProxyConfig.getPassword().orElse(null),
                    snowflakeProxyConfig.getProxyProtocol().name().toLowerCase(ENGLISH),
                    null,
                    false);

            return new StarburstResultStreamProvider(HttpUtil.getHttpClient(clientSettingsKey));
        }
    }
}
