/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.dynamodb;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.starburstdata.presto.plugin.jdbc.auth.ForImpersonation;
import com.starburstdata.presto.plugin.jdbc.redirection.JdbcTableScanRedirectionModule;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.RetryingConnectionFactory;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.CredentialProviderModule;
import io.trino.spi.connector.ConnectorPageSinkProvider;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static com.starburstdata.presto.plugin.jdbc.auth.NoImpersonationModule.noImpersonationModuleWithCredentialProvider;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.jdbc.JdbcModule.bindSessionPropertiesProvider;
import static io.trino.plugin.jdbc.JdbcModule.bindTablePropertiesProvider;

public class DynamoDbModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(DynamoDbJdbcClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(DynamoDbConfig.class);

        newOptionalBinder(binder, ConnectorPageSinkProvider.class).setBinding().to(DynamoDbJdbcPageSinkProvider.class).in(Scopes.SINGLETON);

        bindTablePropertiesProvider(binder, DynamoDbTableProperties.class);
        bindSessionPropertiesProvider(binder, DynamoDbSessionProperties.class);

        install(new CredentialProviderModule());
        install(noImpersonationModuleWithCredentialProvider());
        install(new JdbcTableScanRedirectionModule());

        // Set the connection URL to some value as it is a required property in the JdbcModule
        // The actual connection URL is set via the DynamoDbConnectionFactory
        configBinder(binder).bindConfigDefaults(BaseJdbcConfig.class, config -> config.setConnectionUrl("jdbc:dynamodb:"));
    }

    @Provides
    @Singleton
    @ForImpersonation
    public ConnectionFactory getConnectionFactory(DynamoDbConfig dynamoDbConfig, CredentialProvider credentialProvider)
    {
        // The CData JDBC driver will intermittently throw an exception with no error message
        // Typically, retrying the query will cause it to proceed
        return new RetryingConnectionFactory(new DynamoDbConnectionFactory(dynamoDbConfig, credentialProvider));
    }
}
