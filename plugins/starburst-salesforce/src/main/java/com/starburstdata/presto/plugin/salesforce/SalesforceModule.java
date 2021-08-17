/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.salesforce;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.starburstdata.presto.plugin.jdbc.auth.ForImpersonation;
import com.starburstdata.presto.plugin.jdbc.dynamicfiltering.ForDynamicFiltering;
import com.starburstdata.presto.plugin.jdbc.redirection.JdbcTableScanRedirectionModule;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcMetadataConfig;
import io.trino.plugin.jdbc.JdbcRecordSetProvider;
import io.trino.plugin.jdbc.JdbcSplitManager;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.CredentialProviderModule;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSplitManager;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static com.starburstdata.presto.plugin.jdbc.auth.NoImpersonationModule.noImpersonationModuleWithCredentialProvider;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class SalesforceModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(SalesforceJdbcClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(SalesforceConfig.class);

        binder.bind(ConnectorSplitManager.class).annotatedWith(ForDynamicFiltering.class).to(JdbcSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorRecordSetProvider.class).annotatedWith(ForDynamicFiltering.class).to(JdbcRecordSetProvider.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, ConnectorPageSinkProvider.class).setBinding().to(SalesforceJdbcPageSinkProvider.class).in(Scopes.SINGLETON);
        newSetBinder(binder, SystemTableProvider.class).addBinding().to(SalesforceSystemTableProvider.class);

        install(new CredentialProviderModule());
        install(noImpersonationModuleWithCredentialProvider());
        install(new JdbcTableScanRedirectionModule());

        // Set the connection URL to some value as it is a required property in the JdbcModule
        // The actual connection URL is set via the SalesforceConnectionFactory
        configBinder(binder).bindConfigDefaults(BaseJdbcConfig.class, config -> config.setConnectionUrl("jdbc:salesforce:"));

        // Salesforce does not support transaction inserts -- set the default to true
        // Writes are currently only enabled for tests, so the code that users this property won't be exercised
        // If we do enable writes some day, users would get an odd error if they set this property by false
        // We may want to override begin/finish insert table rather than using this property
        configBinder(binder).bindConfigDefaults(JdbcMetadataConfig.class, config -> config.setNonTransactionalInsert(true));
    }

    @Provides
    @Singleton
    @ForImpersonation
    public ConnectionFactory getConnectionFactory(SalesforceConfig salesforceConfig, CredentialProvider credentialProvider)
    {
        return new SalesforceConnectionFactory(salesforceConfig, credentialProvider);
    }
}
