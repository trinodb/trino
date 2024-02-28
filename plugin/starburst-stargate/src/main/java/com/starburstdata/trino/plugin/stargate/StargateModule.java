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

import com.google.inject.Binder;
import com.google.inject.BindingAnnotation;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.jdbc.ConfiguringConnectionFactory;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcJoinPushdownSupportModule;
import io.trino.plugin.jdbc.JdbcMetadataConfig;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.MaxDomainCompactionThreshold;
import io.trino.plugin.jdbc.ptf.Query;
import io.trino.spi.function.table.ConnectorTableFunction;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class StargateModule
        extends AbstractConfigurationAwareModule
{
    // Values below are set based on tests. Automated tests cover domains with up to 5_000 values.
    // We also did manual test with domain with 50_000 values.
    public static final int STARGATE_DEFAULT_DOMAIN_COMPACTION_THRESHOLD = 5_000;
    public static final int STARGATE_MAX_DOMAIN_COMPACTION_THRESHOLD = 50_000;

    @Override
    public void setup(Binder binder)
    {
        configBinder(binder).bindConfig(StargateConfig.class);
        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);
        configBinder(binder).bindConfig(StargateJdbcConfig.class);

        install(conditionalModule(
                StargateConfig.class,
                StargateConfig::isSslEnabled,
                new SslModule(),
                new NoSslModule()));

        configBinder(binder).bindConfigDefaults(JdbcMetadataConfig.class, config -> {
            config.setDomainCompactionThreshold(STARGATE_DEFAULT_DOMAIN_COMPACTION_THRESHOLD);
        });
        newOptionalBinder(binder, Key.get(int.class, MaxDomainCompactionThreshold.class)).setBinding().toInstance(STARGATE_MAX_DOMAIN_COMPACTION_THRESHOLD);

        install(new JdbcJoinPushdownSupportModule());

        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(Query.class).in(Scopes.SINGLETON);

        // Using optional binder for overriding StargateClient in SEP
        newOptionalBinder(binder, Key.get(JdbcClient.class, ForBaseJdbc.class))
                .setDefault()
                .to(StargateClient.class)
                .in(Scopes.SINGLETON);

        // Using optional binder for overriding ConnectionFactory in Galaxy
        newOptionalBinder(binder, Key.get(ConnectionFactory.class, ForBaseJdbc.class))
                .setDefault()
                .to(Key.get(ConnectionFactory.class, DefaultStargateBinding.class))
                .in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @DefaultStargateBinding
    public ConnectionFactory getConnectionFactory(@TransportConnectionFactory ConnectionFactory delegate, @EnableWrites boolean enableWrites)
    {
        requireNonNull(delegate, "delegate is null");
        if (enableWrites) {
            return delegate;
        }
        return new ConfiguringConnectionFactory(delegate, connection -> connection.setReadOnly(true));
    }

    private static class SslModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            configBinder(binder).bindConfig(StargateSslConfig.class);
        }
    }

    private static class NoSslModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            // Bind the config to an instance rather than using 'bindConfig' so the properties cannot be set.
            binder.bind(StargateSslConfig.class).toInstance(new StargateSslConfig());
        }
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD})
    @BindingAnnotation
    public @interface DefaultStargateBinding {}
}
