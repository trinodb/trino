/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake.distributed;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.starburstdata.trino.plugins.snowflake.jdbc.SnowflakeJdbcClientModule;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSourceProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitManager;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.s3.TrinoS3FileSystem;
import io.trino.plugin.hive.s3.TrinoS3FileSystemStats;
import io.trino.plugin.jdbc.JdbcMetadataFactory;
import io.trino.plugin.jdbc.JdbcModule;
import io.trino.plugin.jdbc.JdbcPageSinkProvider;
import io.trino.plugin.jdbc.ptf.Query;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.ptf.ConnectorTableFunction;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Qualifier;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.jdbc.JdbcModule.bindSessionPropertiesProvider;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class SnowflakeDistributedModule
        extends AbstractConfigurationAwareModule
{
    protected final String catalogName;

    public SnowflakeDistributedModule(String catalogName)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
    }

    // TODO: Reorganize this method
    @Override
    protected void setup(Binder binder)
    {
        newOptionalBinder(binder, ConnectorAccessControl.class);
        newSetBinder(binder, Procedure.class);
        bindSessionPropertiesProvider(binder, SnowflakeDistributedSessionProperties.class);
        // TODO: Make more bindings optional defaults so SEP and Galaxy can
        //       replace them if necessary.
        newOptionalBinder(binder, JdbcMetadataFactory.class).setBinding().to(SnowflakeMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(SnowflakeConnectionManager.class).in(Scopes.SINGLETON);
        binder.bind(SnowflakeSplitManager.class).in(Scopes.SINGLETON);

        newOptionalBinder(binder, Key.get(SnowflakePageSourceProvider.class, ForSnowflake.class))
                .setDefault()
                .to(SnowflakePageSourceProvider.class)
                .in(Scopes.SINGLETON);

        binder.bind(JdbcPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(Connector.class).to(SnowflakeDistributedConnector.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(SnowflakeDistributedConfig.class);

        binder.bind(SnowflakeExportStats.class).toInstance(new SnowflakeExportStats());
        newExporter(binder).export(SnowflakeExportStats.class).as(generator -> generator.generatedNameOf(SnowflakeExportStats.class, catalogName));

        binder.bind(TrinoS3FileSystemStats.class).toInstance(TrinoS3FileSystem.getFileSystemStats());
        newExporter(binder).export(TrinoS3FileSystemStats.class)
                .as(generator -> generator.generatedNameOf(TrinoS3FileSystem.class));
        binder.bind(FileFormatDataSourceStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(FileFormatDataSourceStats.class).withGeneratedName();

        install(new JdbcModule(catalogName));
        install(new SnowflakeJdbcClientModule(catalogName, true));

        // Override binding from JDBC module
        newOptionalBinder(binder, ConnectorSplitManager.class)
                .setBinding()
                .toProvider(SnowflakeDistributedSplitManagerProvider.class)
                .in(Scopes.SINGLETON);
        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(Query.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    public ConnectorPageSourceProvider createConnectorPageSourceProvider(
            @ForSnowflake SnowflakePageSourceProvider snowflakePageSourceProvider)
    {
        return new ClassLoaderSafeConnectorPageSourceProvider(snowflakePageSourceProvider, getClass().getClassLoader());
    }

    @Provides
    @Singleton
    public static ListeningExecutorService createListeningExecutorService()
    {
        return listeningDecorator(newCachedThreadPool(daemonThreadsNamed("snowflake-%s")));
    }

    public static class SnowflakeDistributedSplitManagerProvider
            implements Provider<ConnectorSplitManager>
    {
        private final SnowflakeSplitManager snowflakeSplitManager;

        @Inject
        public SnowflakeDistributedSplitManagerProvider(SnowflakeSplitManager snowflakeSplitManager)
        {
            this.snowflakeSplitManager = requireNonNull(snowflakeSplitManager, "snowflakeSplitManager is null");
        }

        @Override
        public ConnectorSplitManager get()
        {
            return new ClassLoaderSafeConnectorSplitManager(snowflakeSplitManager, getClass().getClassLoader());
        }
    }

    // TODO: When cleaning bindings, if this is still needed, move it to its own file
    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @Qualifier
    public @interface ForSnowflake {}
}
