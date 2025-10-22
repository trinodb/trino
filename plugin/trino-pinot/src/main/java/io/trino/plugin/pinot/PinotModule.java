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

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.plugin.pinot.client.IdentityPinotHostMapper;
import io.trino.plugin.pinot.client.PinotClient;
import io.trino.plugin.pinot.client.PinotDataFetcher;
import io.trino.plugin.pinot.client.PinotGrpcDataFetcher;
import io.trino.plugin.pinot.client.PinotGrpcServerQueryClientConfig;
import io.trino.plugin.pinot.client.PinotGrpcServerQueryClientTlsConfig;
import io.trino.plugin.pinot.client.PinotHostMapper;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.utils.DataSchema;
import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.node.ArrayNode;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ExecutorService;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;

public class PinotModule
        extends AbstractConfigurationAwareModule
{
    private final String catalogName;

    public PinotModule(String catalogName)
    {
        this.catalogName = catalogName;
    }

    @Override
    public void setup(Binder binder)
    {
        configBinder(binder).bindConfig(PinotConfig.class);
        binder.bind(PinotConnector.class).in(Scopes.SINGLETON);
        binder.bind(PinotMetadata.class).in(Scopes.SINGLETON);
        binder.bind(PinotSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(PinotPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(PinotClient.class).in(Scopes.SINGLETON);
        binder.bind(PinotTypeConverter.class).in(Scopes.SINGLETON);
        binder.bind(ExecutorService.class).annotatedWith(ForPinot.class)
                .toInstance(newCachedThreadPool(threadsNamed("pinot-metadata-fetcher-" + catalogName)));

        binder.bind(PinotSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(PinotNodePartitioningProvider.class).in(Scopes.SINGLETON);
        httpClientBinder(binder).bindHttpClient("pinot", ForPinot.class)
                .withConfigDefaults(cfg -> {
                    cfg.setIdleTimeout(new Duration(300, SECONDS));
                    cfg.setConnectTimeout(new Duration(300, SECONDS));
                    cfg.setRequestTimeout(new Duration(300, SECONDS));
                    cfg.setMaxConnectionsPerServer(250);
                    cfg.setMaxResponseContentLength(DataSize.of(32, MEGABYTE));
                    cfg.setSelectorCount(10);
                    cfg.setTimeoutThreads(8);
                    cfg.setTimeoutConcurrency(4);
                });

        jsonBinder(binder).addDeserializerBinding(DataSchema.class).to(DataSchemaDeserializer.class);
        jsonBinder(binder).addDeserializerBinding(BrokerResponseNative.class).to(BrokerResponseNativeDeserializer.class);

        PinotClient.addJsonBinders(jsonCodecBinder(binder));
        binder.bind(ConnectorNodePartitioningProvider.class).to(PinotNodePartitioningProvider.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, PinotHostMapper.class).setDefault().to(IdentityPinotHostMapper.class).in(Scopes.SINGLETON);

        install(new PinotGrpcModule());
    }

    public static final class DataSchemaDeserializer
            extends ValueDeserializer<DataSchema>
    {
        @Override
        public DataSchema deserialize(JsonParser p, DeserializationContext ctxt)
        {
            JsonNode jsonNode = ctxt.readTree(p);
            ArrayNode columnDataTypes = (ArrayNode) jsonNode.get("columnDataTypes");
            DataSchema.ColumnDataType[] columnTypes = new DataSchema.ColumnDataType[columnDataTypes.size()];
            for (int i = 0; i < columnDataTypes.size(); i++) {
                columnTypes[i] = DataSchema.ColumnDataType.valueOf(columnDataTypes.get(i).asString().toUpperCase(ENGLISH));
            }
            ArrayNode columnNamesJson = (ArrayNode) jsonNode.get("columnNames");
            String[] columnNames = new String[columnNamesJson.size()];
            for (int i = 0; i < columnNamesJson.size(); i++) {
                columnNames[i] = columnNamesJson.get(i).asString();
            }
            return new DataSchema(columnNames, columnTypes);
        }
    }

    public static class BrokerResponseNativeDeserializer
            extends ValueDeserializer<BrokerResponseNative>
    {
        @Override
        public BrokerResponseNative deserialize(JsonParser p, DeserializationContext ctxt)
        {
            JsonNode jsonNode = ctxt.readTree(p);
            String value = jsonNode.toString();
            try {
                return BrokerResponseNative.fromJsonString(value);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
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

            if (buildConfigObject(PinotGrpcServerQueryClientConfig.class).isUsePlainText()) {
                binder.bind(PinotGrpcDataFetcher.GrpcQueryClientFactory.class).to(PinotGrpcDataFetcher.PlainTextGrpcQueryClientFactory.class).in(Scopes.SINGLETON);
            }
            else {
                configBinder(binder).bindConfig(PinotGrpcServerQueryClientTlsConfig.class);
                binder.bind(PinotGrpcDataFetcher.GrpcQueryClientFactory.class).to(PinotGrpcDataFetcher.TlsGrpcQueryClientFactory.class).in(Scopes.SINGLETON);
            }
        }
    }
}
