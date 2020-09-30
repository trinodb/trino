/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.saphana;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.sap.db.jdbc.Driver;
import com.starburstdata.presto.plugin.jdbc.auth.PassThroughCredentialProvider;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.plugin.jdbc.ForBaseJdbc;
import io.prestosql.plugin.jdbc.credential.CredentialProvider;
import io.prestosql.plugin.jdbc.credential.CredentialProviderModule;

import static io.airlift.configuration.ConditionalModule.installModuleIf;
import static java.util.Objects.requireNonNull;

public class SapHanaAuthenticationModule
        extends AbstractConfigurationAwareModule
{
    private final String catalogName;

    public SapHanaAuthenticationModule(String catalogName)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        install(installModuleIf(
                SapHanaAuthenticationConfig.class,
                config -> config.getAuthenticationType() == SapHanaAuthenticationType.PASSWORD,
                new PasswordModule()));

        install(installModuleIf(
                SapHanaAuthenticationConfig.class,
                config -> config.getAuthenticationType() == SapHanaAuthenticationType.PASSWORD_PASS_THROUGH,
                new PasswordPassThroughModule()));
    }

    private static class PasswordModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            install(new CredentialProviderModule());
        }

        @Provides
        @Singleton
        @ForBaseJdbc
        public ConnectionFactory getConnectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider)
        {
            return new DriverConnectionFactory(new Driver(), config, credentialProvider);
        }
    }

    private static class PasswordPassThroughModule
            implements Module
    {
        @Override
        public void configure(Binder binder) {}

        @Provides
        @Singleton
        @ForBaseJdbc
        public ConnectionFactory getConnectionFactory(BaseJdbcConfig config)
        {
            return new DriverConnectionFactory(new Driver(), config, new PassThroughCredentialProvider());
        }
    }
}
