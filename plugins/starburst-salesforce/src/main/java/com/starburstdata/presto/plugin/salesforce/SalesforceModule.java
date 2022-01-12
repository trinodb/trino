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
import io.trino.plugin.jdbc.JdbcRecordSetProvider;
import io.trino.plugin.jdbc.JdbcSplitManager;
import io.trino.plugin.jdbc.JdbcWriteConfig;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.CredentialProviderModule;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSplitManager;

import javax.inject.Inject;
import javax.inject.Provider;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static com.starburstdata.presto.plugin.jdbc.auth.NoImpersonationModule.noImpersonationModuleWithCredentialProvider;
import static com.starburstdata.presto.plugin.salesforce.SalesforceConfig.SalesforceAuthenticationType.OAUTH_JWT;
import static com.starburstdata.presto.plugin.salesforce.SalesforceConfig.SalesforceAuthenticationType.PASSWORD;
import static com.starburstdata.presto.plugin.salesforce.SalesforceConnectionFactory.CDATA_OEM_KEY;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

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

        install(conditionalModule(
                SalesforceConfig.class,
                config -> config.getAuthenticationType() == PASSWORD,
                authenticationBinder -> {
                    configBinder(authenticationBinder).bindConfig(SalesforcePasswordConfig.class);
                    binder.bind(ConnectionUrlProvider.class).to(PasswordConnectionUrlProvider.class).in(Scopes.SINGLETON);
                }));

        install(conditionalModule(
                SalesforceConfig.class,
                config -> config.getAuthenticationType() == OAUTH_JWT,
                authenticationBinder -> {
                    configBinder(authenticationBinder).bindConfig(SalesforceOAuthJwtConfig.class);
                    binder.bind(ConnectionUrlProvider.class).to(OAuthJwtConnectionUrlProvider.class).in(Scopes.SINGLETON);
                }));

        install(new JdbcTableScanRedirectionModule());

        // Set the connection URL to some value as it is a required property in the JdbcModule
        // The actual connection URL is set via the SalesforceConnectionFactory
        configBinder(binder).bindConfigDefaults(BaseJdbcConfig.class, config -> config.setConnectionUrl("jdbc:salesforce:"));

        // Salesforce does not support transaction inserts -- set the default to true
        // Writes are currently only enabled for tests, so the code that users this property won't be exercised
        // If we do enable writes some day, users would get an odd error if they set this property by false
        // We may want to override begin/finish insert table rather than using this property
        configBinder(binder).bindConfigDefaults(JdbcWriteConfig.class, config -> config.setNonTransactionalInsert(true));
    }

    public interface ConnectionUrlProvider
            extends Provider<String> {}

    public static class PasswordConnectionUrlProvider
            implements ConnectionUrlProvider
    {
        private final SalesforceConfig salesforceConfig;
        private final SalesforcePasswordConfig passwordConfig;

        @Inject
        public PasswordConnectionUrlProvider(SalesforceConfig salesforceConfig, SalesforcePasswordConfig passwordConfig)
        {
            this.salesforceConfig = requireNonNull(salesforceConfig, "salesforceConfig is null");
            this.passwordConfig = requireNonNull(passwordConfig, "passwordConfig is null");
        }

        @Override
        public String get()
        {
            StringBuilder builder = new StringBuilder("jdbc:salesforce:")
                    .append("User=\"").append(passwordConfig.getUser()).append("\";")
                    .append("Password=\"").append(passwordConfig.getPassword()).append("\";")
                    .append("UseSandbox=\"").append(salesforceConfig.isSandboxEnabled()).append("\";")
                    .append("OEMKey=\"").append(CDATA_OEM_KEY).append("\";");

            passwordConfig.getSecurityToken().ifPresent(token -> builder.append("SecurityToken=\"").append(token).append("\";"));

            if (salesforceConfig.isDriverLoggingEnabled()) {
                builder.append("LogFile=\"").append(salesforceConfig.getDriverLoggingLocation()).append("\";")
                        .append("Verbosity=\"").append(salesforceConfig.getDriverLoggingVerbosity()).append("\";");
            }

            salesforceConfig.getExtraJdbcProperties().ifPresent(builder::append);
            return builder.toString();
        }
    }

    public static class OAuthJwtConnectionUrlProvider
            implements ConnectionUrlProvider
    {
        private final SalesforceConfig salesforceConfig;
        private final SalesforceOAuthJwtConfig oAuthJwtConfig;

        @Inject
        public OAuthJwtConnectionUrlProvider(SalesforceConfig salesforceConfig, SalesforceOAuthJwtConfig oAuthJwtConfig)
        {
            this.salesforceConfig = requireNonNull(salesforceConfig, "salesforceConfig is null");
            this.oAuthJwtConfig = requireNonNull(oAuthJwtConfig, "oAuthJwtConfig is null");
        }

        @Override
        public String get()
        {
            StringBuilder builder = new StringBuilder("jdbc:salesforce:")
                    .append("AuthScheme=\"OAuthJWT\";")
                    .append("OAuthJWTCertSubject=\"").append(oAuthJwtConfig.getPkcs12CertificateSubject()).append("\";")
                    .append("OAuthJWTCertPassword=\"").append(oAuthJwtConfig.getPkcs12Password()).append("\";")
                    .append("OAuthJWTCert=\"").append(oAuthJwtConfig.getPkcs12Path()).append("\";")
                    .append("OAuthJWTCertType=\"PFXFILE\";")
                    .append("OAuthJWTIssuer=\"").append(oAuthJwtConfig.getJwtIssuer()).append("\";")
                    .append("OAuthJWTSubject=\"").append(oAuthJwtConfig.getJwtSubject()).append("\";")
                    .append("UseSandbox=\"").append(salesforceConfig.isSandboxEnabled()).append("\";")
                    .append("OEMKey=\"").append(CDATA_OEM_KEY).append("\";");

            if (salesforceConfig.isDriverLoggingEnabled()) {
                builder.append("LogFile=\"").append(salesforceConfig.getDriverLoggingLocation()).append("\";")
                        .append("Verbosity=\"").append(salesforceConfig.getDriverLoggingVerbosity()).append("\";");
            }

            salesforceConfig.getExtraJdbcProperties().ifPresent(builder::append);
            return builder.toString();
        }
    }

    @Provides
    @Singleton
    @ForImpersonation
    public ConnectionFactory getConnectionFactory(CredentialProvider credentialProvider, ConnectionUrlProvider connectionUrlProvider)
    {
        return new SalesforceConnectionFactory(credentialProvider, connectionUrlProvider);
    }
}
