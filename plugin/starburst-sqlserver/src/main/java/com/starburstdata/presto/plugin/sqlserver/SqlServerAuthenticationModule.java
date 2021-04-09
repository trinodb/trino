/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.sqlserver;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.starburstdata.presto.plugin.jdbc.auth.NoImpersonationModule;
import com.starburstdata.presto.plugin.jdbc.authtolocal.AuthToLocalModule;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.credential.CredentialProviderModule;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConditionalModule.installModuleIf;

public class SqlServerAuthenticationModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(new CredentialProviderModule());

        install(installModuleIf(
                SqlServerConfig.class,
                SqlServerConfig::isImpersonationEnabled,
                new ImpersonationModule(),
                new NoImpersonationModule()));
    }

    private static class ImpersonationModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.install(new AuthToLocalModule());
            binder.bind(ConnectionFactory.class).annotatedWith(ForBaseJdbc.class).to(SqlServerImpersonatingConnectionFactory.class).in(SINGLETON);
        }
    }
}
