/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.oracle;

import com.google.inject.Binder;
import com.google.inject.BindingAnnotation;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.oracle.OracleConfig;
import io.trino.spi.catalog.CatalogName;
import oracle.jdbc.driver.OracleDriver;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Optional;
import java.util.Properties;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static oracle.jdbc.OracleConnection.CONNECTION_PROPERTY_INCLUDE_SYNONYMS;
import static oracle.jdbc.OracleConnection.CONNECTION_PROPERTY_REPORT_REMARKS;
import static oracle.jdbc.OracleConnection.CONNECTION_PROPERTY_RESTRICT_GETTABLES;

public class UserPasswordModule
        extends AbstractConfigurationAwareModule
{
    @Override
    public void setup(Binder binder)
    {
        newOptionalBinder(binder, Key.get(ConnectionFactory.class, ForBaseJdbc.class))
                .setDefault()
                .to(Key.get(ConnectionFactory.class, DefaultOracleBinding.class))
                .in(Scopes.SINGLETON);
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
                    new PasswordAuthenticationConnectionProvider(),
                    oracleConfig);
        }

        return DriverConnectionFactory.builder(new OracleDriver(), config.getConnectionUrl(), credentialProvider)
                .setConnectionProperties(getConnectionProperties(oracleConfig))
                .build();
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
    @BindingAnnotation
    public @interface DefaultOracleBinding {}
}
