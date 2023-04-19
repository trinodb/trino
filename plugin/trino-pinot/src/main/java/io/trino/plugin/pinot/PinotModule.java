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
package io.trino.plugin.pinot;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.plugin.base.jmx.RebindSafeMBeanServer;
import io.trino.plugin.pinot.client.IdentityPinotHostMapper;
import io.trino.plugin.pinot.client.PinotClient;
import io.trino.plugin.pinot.client.PinotDataFetcher;
import io.trino.plugin.pinot.client.PinotGrpcDataFetcher;
import io.trino.plugin.pinot.client.PinotGrpcServerQueryClientConfig;
import io.trino.plugin.pinot.client.PinotGrpcServerQueryClientTlsConfig;
import io.trino.plugin.pinot.client.PinotHostMapper;
import io.trino.plugin.pinot.client.PinotLegacyDataFetcher;
import io.trino.plugin.pinot.client.PinotLegacyServerQueryClientConfig;
import io.trino.plugin.pinot.deepstore.DeepStore;
import io.trino.plugin.pinot.deepstore.PinotDeepStore;
import io.trino.plugin.pinot.deepstore.gcs.PinotGcsModule;
import io.trino.plugin.pinot.deepstore.s3.PinotS3Module;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import org.apache.pinot.common.utils.DataSchema;

import javax.management.MBeanServer;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class PinotModule
        extends AbstractConfigurationAwareModule
{
    private final String catalogName;
    private final NodeManager nodeManager;

    public PinotModule(String catalogName, NodeManager nodeManager)
    {
        this.catalogName = catalogName;
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    @Override
    public void setup(Binder binder)
    {
        configBinder(binder).bindConfig(PinotConfig.class);
        binder.bind(PinotConnector.class).in(Scopes.SINGLETON);
        binder.bind(PinotMetadata.class).in(Scopes.SINGLETON);
        binder.bind(PinotSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(PinotPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(PinotPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(PinotClient.class).in(Scopes.SINGLETON);
        binder.bind(PinotTypeConverter.class).in(Scopes.SINGLETON);
        binder.bind(ExecutorService.class).annotatedWith(ForPinot.class)
                .toInstance(newCachedThreadPool(threadsNamed("pinot-metadata-fetcher-" + catalogName)));

        binder.bind(PinotSessionProperties.class).in(Scopes.SINGLETON);
        httpClientBinder(binder).bindHttpClient("pinot", ForPinot.class)
                .withConfigDefaults(cfg -> {
                    cfg.setIdleTimeout(new Duration(300, SECONDS));
                    cfg.setConnectTimeout(new Duration(300, SECONDS));
                    cfg.setRequestTimeout(new Duration(300, SECONDS));
                    cfg.setMaxConnectionsPerServer(250);
                    cfg.setMaxContentLength(DataSize.of(32, MEGABYTE));
                    cfg.setSelectorCount(10);
                    cfg.setTimeoutThreads(8);
                    cfg.setTimeoutConcurrency(4);
                });

        jsonBinder(binder).addDeserializerBinding(DataSchema.class).to(DataSchemaDeserializer.class);
        PinotClient.addJsonBinders(jsonCodecBinder(binder));
        binder.bind(MBeanServer.class).toInstance(new RebindSafeMBeanServer(getPlatformMBeanServer()));
        binder.bind(NodeManager.class).toInstance(nodeManager);
        binder.bind(ConnectorNodePartitioningProvider.class).to(PinotNodePartitioningProvider.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, PinotHostMapper.class).setDefault().to(IdentityPinotHostMapper.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, DeepStore.class);
        bindDeepStore(PinotDeepStore.DeepStoreProvider.NONE, new EmptyModule());
        bindDeepStore(PinotDeepStore.DeepStoreProvider.GCS, new PinotGcsModule());
        bindDeepStore(PinotDeepStore.DeepStoreProvider.S3, new PinotS3Module());

        install(conditionalModule(
                PinotConfig.class,
                config -> config.isGrpcEnabled(),
                new PinotGrpcModule(),
                new LegacyClientModule()));
    }

    @Provides
    @Singleton
    @ForPinotInsert
    public static ExecutorService createCoreInsertExecutor()
    {
        return newCachedThreadPool(daemonThreadsNamed("pinot-insert-%s"));
    }

    @Provides
    @Singleton
    @ForPinotInsert
    public static BoundedExecutor craateSegmentBuilderExecutor(PinotConfig config)
    {
        // Block on submit if queue size is exceeded
        return new BoundedExecutor(new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60_000, MILLISECONDS, new PinotPageSinkProvider.TimedBlockingQueue<>(config.getSegmentBuilderQueueSize(), config.getSegmentBuilderQueueTimeout().toMillis()), daemonThreadsNamed("pinot-segment-builder-%s")), config.getSegmentBuilderParallelism());
    }

    @Provides
    @Singleton
    @ForPinotInsert
    public static ScheduledExecutorService createInsertTimeoutExecutor(PinotConfig config)
    {
        return newScheduledThreadPool(config.getInsertTimeoutThreads(), daemonThreadsNamed("pinot-insert-timeout-%s"));
    }

    public static final class DataSchemaDeserializer
            extends JsonDeserializer<DataSchema>
    {
        @Override
        public DataSchema deserialize(JsonParser p, DeserializationContext ctxt)
                throws IOException
        {
            JsonNode jsonNode = ctxt.readTree(p);
            ArrayNode columnDataTypes = (ArrayNode) jsonNode.get("columnDataTypes");
            DataSchema.ColumnDataType[] columnTypes = new DataSchema.ColumnDataType[columnDataTypes.size()];
            for (int i = 0; i < columnDataTypes.size(); i++) {
                columnTypes[i] = DataSchema.ColumnDataType.valueOf(columnDataTypes.get(i).asText().toUpperCase(ENGLISH));
            }
            ArrayNode columnNamesJson = (ArrayNode) jsonNode.get("columnNames");
            String[] columnNames = new String[columnNamesJson.size()];
            for (int i = 0; i < columnNamesJson.size(); i++) {
                columnNames[i] = columnNamesJson.get(i).asText();
            }
            return new DataSchema(columnNames, columnTypes);
        }
    }

    public static class PinotGrpcModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        public void setup(Binder binder)
        {
            configBinder(binder).bindConfig(PinotGrpcServerQueryClientConfig.class);
            binder.bind(PinotDataFetcher.Factory.class).to(PinotGrpcDataFetcher.Factory.class).in(Scopes.SINGLETON);
            install(conditionalModule(
                    PinotGrpcServerQueryClientConfig.class,
                    config -> config.isUsePlainText(),
                    plainTextBinder -> plainTextBinder.bind(PinotGrpcDataFetcher.GrpcQueryClientFactory.class).to(PinotGrpcDataFetcher.PlainTextGrpcQueryClientFactory.class).in(Scopes.SINGLETON),
                    tlsBinder -> {
                        configBinder(tlsBinder).bindConfig(PinotGrpcServerQueryClientTlsConfig.class);
                        tlsBinder.bind(PinotGrpcDataFetcher.GrpcQueryClientFactory.class).to(PinotGrpcDataFetcher.TlsGrpcQueryClientFactory.class).in(Scopes.SINGLETON);
                    }));
        }
    }

    public static class LegacyClientModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        public void setup(Binder binder)
        {
            configBinder(binder).bindConfig(PinotLegacyServerQueryClientConfig.class);
            binder.bind(PinotDataFetcher.Factory.class).to(PinotLegacyDataFetcher.Factory.class).in(Scopes.SINGLETON);
        }
    }

    public void bindDeepStore(PinotDeepStore.DeepStoreProvider deepStoreProvider, Module module)
    {
        install(conditionalModule(
                PinotConfig.class,
                pinotConfig -> deepStoreProvider == pinotConfig.getDeepStoreProvider(),
                module));
    }

    public static class EmptyModule
            implements Module
    {
        @Override
        public void configure(Binder binder) {}
    }
}
