/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.oracle;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.starburstdata.presto.license.LicenseManager;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.ForJdbcDynamicFiltering;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcDynamicFilteringSplitManager;
import io.trino.plugin.jdbc.JdbcJoinPushdownSupportModule;
import io.trino.plugin.jdbc.JdbcMetadataConfig;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.MaxDomainCompactionThreshold;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.ptf.Query;
import io.trino.plugin.oracle.OracleConfig;
import io.trino.plugin.oracle.OracleSessionProperties;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.ptf.ConnectorTableFunction;
import oracle.jdbc.driver.OracleDriver;

import javax.inject.Qualifier;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Optional;
import java.util.Properties;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.jdbc.JdbcModule.bindProcedure;
import static io.trino.plugin.jdbc.JdbcModule.bindSessionPropertiesProvider;
import static io.trino.plugin.oracle.OracleClient.ORACLE_MAX_LIST_EXPRESSIONS;
import static java.util.Objects.requireNonNull;
import static oracle.jdbc.OracleConnection.CONNECTION_PROPERTY_INCLUDE_SYNONYMS;
import static oracle.jdbc.OracleConnection.CONNECTION_PROPERTY_REPORT_REMARKS;
import static oracle.jdbc.OracleConnection.CONNECTION_PROPERTY_RESTRICT_GETTABLES;

public class StarburstOracleClientModule
        extends AbstractConfigurationAwareModule
{
    private final LicenseManager licenseManager;

    public StarburstOracleClientModule(LicenseManager licenseManager)
    {
        this.licenseManager = requireNonNull(licenseManager, "licenseManager is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        newOptionalBinder(binder, Key.get(ConnectorSplitManager.class, ForJdbcDynamicFiltering.class)).setBinding().to(OracleSplitManager.class).in(SINGLETON);
        newOptionalBinder(binder, ConnectorSplitManager.class).setBinding().to(JdbcDynamicFilteringSplitManager.class).in(SINGLETON);

        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(StarburstOracleClient.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, Key.get(int.class, MaxDomainCompactionThreshold.class)).setBinding().toInstance(ORACLE_MAX_LIST_EXPRESSIONS);

        bindProcedure(binder, AnalyzeProcedure.class);

        binder.bind(LicenseManager.class).toInstance(licenseManager);
        bindSessionPropertiesProvider(binder, StarburstOracleSessionProperties.class);
        bindSessionPropertiesProvider(binder, OracleSessionProperties.class);

        configBinder(binder).bindConfig(OracleConfig.class);
        configBinder(binder).bindConfig(StarburstOracleConfig.class);
        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);

        configBinder(binder).bindConfigDefaults(JdbcMetadataConfig.class, config -> config.setAggregationPushdownEnabled(licenseManager.hasLicense()));

        install(new JdbcJoinPushdownSupportModule());

        newOptionalBinder(binder, Key.get(ConnectionFactory.class, ForBaseJdbc.class))
                .setDefault()
                .to(Key.get(ConnectionFactory.class, DefaultOracleBinding.class))
                .in(Scopes.SINGLETON);

        @SuppressWarnings("TrinoExperimentalSpi")
        Class<ConnectorTableFunction> clazz = ConnectorTableFunction.class;
        newSetBinder(binder, clazz).addBinding().toProvider(Query.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @DefaultOracleBinding
    public static ConnectionFactory connectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider, OracleConfig oracleConfig, CatalogName catalogName)
    {
        if (oracleConfig.isConnectionPoolEnabled()) {
            return new OraclePoolingConnectionFactory(
                    catalogName,
                    config,
                    getConnectionProperties(oracleConfig),
                    Optional.of(credentialProvider),
                    oracleConfig);
        }

        return new DriverConnectionFactory(
                new OracleDriver(),
                config.getConnectionUrl(),
                getConnectionProperties(oracleConfig),
                credentialProvider);
    }

    public static Properties getConnectionProperties(OracleConfig oracleConfig)
    {
        Properties properties = new Properties();
        if (oracleConfig.isSynonymsEnabled()) {
            properties.setProperty(CONNECTION_PROPERTY_INCLUDE_SYNONYMS, "true");
            properties.setProperty(CONNECTION_PROPERTY_RESTRICT_GETTABLES, "true");
        }
        if (oracleConfig.isRemarksReportingEnabled()) {
            properties.setProperty(CONNECTION_PROPERTY_REPORT_REMARKS, "true");
        }
        return properties;
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD})
    @Qualifier
    public @interface DefaultOracleBinding {}
}
