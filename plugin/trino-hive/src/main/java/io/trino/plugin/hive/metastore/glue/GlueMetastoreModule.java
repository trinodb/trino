/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hive.metastore.glue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.VerifyException;
import com.google.inject.Binder;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.multibindings.ProvidesIntoOptional;
import com.google.inject.util.Providers;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.units.Duration;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.instrumentation.awssdk.v2_2.AwsSdkTelemetry;
import io.trino.iam.aws.IAMSecurityMapping;
import io.trino.iam.aws.IAMSecurityMappingProvider;
import io.trino.iam.aws.IAMSecurityMappings;
import io.trino.iam.aws.IAMSecurityMappingsFileSource;
import io.trino.iam.aws.IAMSecurityMappingsUriSource;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.RawHiveMetastoreFactory;
import io.trino.metastore.cache.CachingHiveMetastoreConfig;
import io.trino.plugin.hive.AllowHiveTableRename;
import io.trino.plugin.hive.HideDeltaLakeTables;
import io.trino.spi.Node;
import io.trino.spi.catalog.CatalogName;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.EnumSet;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static com.google.inject.multibindings.ProvidesIntoOptional.Type.DEFAULT;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public final class GlueMetastoreModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(GlueHiveMetastoreConfig.class);

        if (buildConfigObject(GlueSecurityMappingEnabledConfig.class).isEnabled()) {
            install(new GlueMetastoreModule.GlueSecurityMappingModule());
        }
        else {
            newOptionalBinder(binder, new TypeLiteral<IAMSecurityMappingProvider<IAMSecurityMappings<IAMSecurityMapping>, IAMSecurityMapping, GlueSecurityMappingConfig>>() {})
                    .setDefault()
                    .toProvider(Providers.of(null));
        }

        binder.bind(GlueHiveMetastoreFactory.class).in(Scopes.SINGLETON);
        binder.bind(GlueMetastoreStats.class).in(Scopes.SINGLETON);
        binder.bind(GlueClientFactory.class).in(Scopes.SINGLETON);
        newExporter(binder).export(GlueMetastoreStats.class).withGeneratedName();
        newOptionalBinder(binder, Key.get(HiveMetastoreFactory.class, RawHiveMetastoreFactory.class))
                .setDefault()
                .to(GlueHiveMetastoreFactory.class)
                .in(Scopes.SINGLETON);
        binder.bind(Key.get(boolean.class, AllowHiveTableRename.class)).toInstance(false);

        Multibinder<ExecutionInterceptor> executionInterceptorMultibinder = newSetBinder(binder, ExecutionInterceptor.class, ForGlueHiveMetastore.class);
        executionInterceptorMultibinder.addBinding().toProvider(TelemetryExecutionInterceptorProvider.class).in(Scopes.SINGLETON);
        executionInterceptorMultibinder.addBinding().to(GlueHiveExecutionInterceptor.class).in(Scopes.SINGLETON);
        executionInterceptorMultibinder.addBinding().to(GlueCatalogIdInterceptor.class).in(Scopes.SINGLETON);
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GlueMetastoreModule;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }

    @ProvidesIntoOptional(DEFAULT)
    @Singleton
    public static Set<GlueHiveMetastore.TableKind> getTableKinds(@HideDeltaLakeTables boolean hideDeltaLakeTables)
    {
        if (hideDeltaLakeTables) {
            return EnumSet.complementOf(EnumSet.of(GlueHiveMetastore.TableKind.DELTA));
        }
        return EnumSet.allOf(GlueHiveMetastore.TableKind.class);
    }

    @Provides
    @Singleton
    public static GlueCache createGlueCache(CachingHiveMetastoreConfig config, CatalogName catalogName, Node currentNode)
    {
        Duration metadataCacheTtl = config.getMetastoreCacheTtl();
        Duration statsCacheTtl = config.getStatsCacheTtl();

        // Disable caching on workers, because there currently is no way to invalidate such a cache.
        // Note: while we could skip CachingHiveMetastoreModule altogether on workers, we retain it so that catalog
        // configuration can remain identical for all nodes, making cluster configuration easier.
        boolean enabled = currentNode.isCoordinator() &&
                (metadataCacheTtl.toMillis() > 0 || statsCacheTtl.toMillis() > 0);

        checkState(config.isPartitionCacheEnabled(), "Disabling partitions cache is not supported with Glue v2");
        checkState(config.isCacheMissing(), "Disabling cache missing is not supported with Glue v2");
        checkState(config.isCacheMissingPartitions(), "Disabling cache missing partitions is not supported with Glue v2");
        checkState(config.isCacheMissingStats(), "Disabling cache missing stats is not supported with Glue v2");

        if (enabled) {
            return new InMemoryGlueCache(
                    catalogName,
                    metadataCacheTtl,
                    statsCacheTtl,
                    config.getMetastoreRefreshInterval(),
                    config.getMaxMetastoreRefreshThreads(),
                    config.getMetastoreCacheMaximumSize());
        }
        return GlueCache.NOOP;
    }

    private static class TelemetryExecutionInterceptorProvider
            implements Provider<ExecutionInterceptor>
    {
        private final OpenTelemetry openTelemetry;

        @Inject
        public TelemetryExecutionInterceptorProvider(OpenTelemetry openTelemetry)
        {
            this.openTelemetry = requireNonNull(openTelemetry, "openTelemetry is null");
        }

        @Override
        public ExecutionInterceptor get()
        {
            return AwsSdkTelemetry.builder(openTelemetry)
                    .setCaptureExperimentalSpanAttributes(true)
                    .setRecordIndividualHttpError(true)
                    .build()
                    .newExecutionInterceptor();
        }
    }

    public static class GlueSecurityMappingModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            GlueSecurityMappingConfig config = buildConfigObject(GlueSecurityMappingConfig.class);
            newOptionalBinder(
                    binder,
                    new TypeLiteral<IAMSecurityMappingProvider<IAMSecurityMappings<IAMSecurityMapping>, IAMSecurityMapping, GlueSecurityMappingConfig>>() {})
                    .setBinding()
                    .to(GlueSecurityMappingProvider.class)
                    .in(Scopes.SINGLETON);

            var mappingsBinder = binder.bind(new Key<Supplier<IAMSecurityMappings<IAMSecurityMapping>>>()
            {
            });
            if (config.getConfigFile().isPresent()) {
                mappingsBinder.toInstance(new IAMSecurityMappingsFileSource<>(
                        config, new TypeReference<>() {}));
            }
            else if (config.getConfigUri().isPresent()) {
                mappingsBinder.to(new TypeLiteral<IAMSecurityMappingsUriSource<IAMSecurityMappings<IAMSecurityMapping>, IAMSecurityMapping, GlueSecurityMappingConfig>>() {}).in(SINGLETON);
                httpClientBinder(binder).bindHttpClient("glue-security-mapping", GlueMetastoreModule.ForGlueSecurityMapping.class)
                        .withConfigDefaults(httpConfig -> httpConfig
                                .setRequestTimeout(new Duration(10, SECONDS))
                                .setSelectorCount(1)
                                .setMinThreads(1));
            }
            else {
                throw new VerifyException("No security mapping source configured");
            }
        }
    }

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    public @interface ForGlueSecurityMapping
    { }
}
