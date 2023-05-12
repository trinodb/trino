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
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.starburstdata.trino.plugins.snowflake.SnowflakeSessionProperties;
import com.starburstdata.trino.plugins.snowflake.jdbc.SnowflakeJdbcClientModule;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.hdfs.TrinoHdfsFileSystemStats;
import io.trino.hdfs.s3.TrinoS3FileSystem;
import io.trino.hdfs.s3.TrinoS3FileSystemStats;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSourceProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitManager;
import io.trino.plugin.base.classloader.ForClassLoaderSafe;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.jdbc.ForJdbcDynamicFiltering;
import io.trino.plugin.jdbc.JdbcDynamicFilteringSplitManager;
import io.trino.plugin.jdbc.JdbcMetadataFactory;
import io.trino.plugin.jdbc.JdbcModule;
import io.trino.plugin.jdbc.JdbcPageSinkProvider;
import io.trino.plugin.jdbc.JdbcQueryEventListener;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.procedure.Procedure;

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
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class SnowflakeDistributedModule
        extends AbstractConfigurationAwareModule
{
    // TODO: Reorganize this method
    @Override
    protected void setup(Binder binder)
    {
        newOptionalBinder(binder, ConnectorAccessControl.class);
        newSetBinder(binder, Procedure.class);
        bindSessionPropertiesProvider(binder, SnowflakeSessionProperties.class);
        bindSessionPropertiesProvider(binder, SnowflakeDistributedSessionProperties.class);

        newOptionalBinder(binder, Key.get(JdbcMetadataFactory.class, ForSnowflake.class))
                .setDefault()
                .to(SnowflakeMetadataFactory.class)
                .in(Scopes.SINGLETON);

        newOptionalBinder(binder, JdbcMetadataFactory.class)
                .setBinding()
                .to(Key.get(JdbcMetadataFactory.class, ForSnowflake.class))
                .in(Scopes.SINGLETON);

        binder.bind(SnowflakeConnectionManager.class).in(Scopes.SINGLETON);
        newSetBinder(binder, JdbcQueryEventListener.class)
                .addBinding()
                .to(Key.get(SnowflakeConnectionManager.class))
                .in(Scopes.SINGLETON);

        newOptionalBinder(binder, Key.get(ConnectorSplitManager.class, ForSnowflake.class))
                .setDefault()
                .to(SnowflakeSplitManager.class)
                .in(Scopes.SINGLETON);

        newOptionalBinder(binder, Key.get(ConnectorSplitManager.class, ForJdbcDynamicFiltering.class))
                .setBinding()
                .to(Key.get(ConnectorSplitManager.class, ForSnowflake.class))
                .in(Scopes.SINGLETON);

        binder.bind(ConnectorSplitManager.class).annotatedWith(ForClassLoaderSafe.class).to(JdbcDynamicFilteringSplitManager.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, ConnectorSplitManager.class).setBinding().to(ClassLoaderSafeConnectorSplitManager.class).in(Scopes.SINGLETON);

        binder.bind(TrinoHdfsFileSystemStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(TrinoHdfsFileSystemStats.class).withGeneratedName();
        newOptionalBinder(binder, Key.get(SnowflakePageSourceProvider.class, ForSnowflake.class))
                .setDefault()
                .to(SnowflakePageSourceProvider.class)
                .in(Scopes.SINGLETON);

        binder.bind(JdbcPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(Connector.class).to(SnowflakeDistributedConnector.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(SnowflakeDistributedConfig.class);

        binder.bind(SnowflakeExportStats.class).toInstance(new SnowflakeExportStats());
        Provider<CatalogName> catalogName = binder.getProvider(CatalogName.class);
        newExporter(binder).export(SnowflakeExportStats.class).as(generator -> generator.generatedNameOf(SnowflakeExportStats.class, catalogName.get().toString()));

        binder.bind(TrinoS3FileSystemStats.class).toInstance(TrinoS3FileSystem.getFileSystemStats());
        newExporter(binder).export(TrinoS3FileSystemStats.class)
                .as(generator -> generator.generatedNameOf(TrinoS3FileSystem.class));
        binder.bind(FileFormatDataSourceStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(FileFormatDataSourceStats.class).withGeneratedName();

        install(new JdbcModule());
        install(new SnowflakeJdbcClientModule(true));
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

    // TODO: When cleaning bindings, if this is still needed, move it to its own file
    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @Qualifier
    public @interface ForSnowflake {}
}
