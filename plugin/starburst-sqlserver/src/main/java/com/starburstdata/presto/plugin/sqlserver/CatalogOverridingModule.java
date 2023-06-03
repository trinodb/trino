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
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.starburstdata.presto.plugin.jdbc.PreparingConnectionFactory;
import com.starburstdata.presto.plugin.jdbc.auth.ForImpersonation;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.spi.connector.ConnectorSession;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.sql.Connection;
import java.sql.SQLException;

import static com.google.inject.Scopes.SINGLETON;
import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerSessionProperties.getOverrideCatalog;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

public class CatalogOverridingModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        install(conditionalModule(
                StarburstSqlServerConfig.class,
                StarburstSqlServerConfig::isOverrideCatalogEnabled,
                moduleBinder -> moduleBinder.bind(ConnectionFactory.class)
                        .annotatedWith(ForImpersonation.class)
                        .to(CatalogOverridingConnectionFactory.class)
                        .in(SINGLETON),
                moduleBinder -> moduleBinder.bind(ConnectionFactory.class)
                        .annotatedWith(ForImpersonation.class)
                        .to(Key.get(ConnectionFactory.class, ForCatalogOverriding.class))
                        .in(SINGLETON)));
    }

    public static class CatalogOverridingConnectionFactory
            extends PreparingConnectionFactory
    {
        @Inject
        public CatalogOverridingConnectionFactory(@ForCatalogOverriding ConnectionFactory delegate)
        {
            super(delegate);
        }

        @Override
        protected void prepare(Connection connection, ConnectorSession session)
                throws SQLException
        {
            String overrideCatalog = getOverrideCatalog(session).orElse(null);

            if (overrideCatalog != null && !overrideCatalog.isBlank()) {
                connection.setCatalog(overrideCatalog);
            }
        }
    }

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    public @interface ForCatalogOverriding
    {
    }
}
