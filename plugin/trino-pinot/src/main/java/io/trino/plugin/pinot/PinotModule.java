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
import com.google.inject.Scopes;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.plugin.base.jmx.RebindSafeMBeanServer;
import io.trino.plugin.pinot.client.IdentityPinotHostMapper;
import io.trino.plugin.pinot.client.PinotClient;
import io.trino.plugin.pinot.client.PinotHostMapper;
import io.trino.plugin.pinot.client.PinotQueryClient;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import org.apache.pinot.common.utils.DataSchema;

import javax.management.MBeanServer;

import java.io.IOException;
import java.util.concurrent.Executor;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;

public class PinotModule
        implements Module
{
    private final String catalogName;
    private final NodeManager nodeManager;

    public PinotModule(String catalogName, NodeManager nodeManager)
    {
        this.catalogName = catalogName;
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(PinotConfig.class);
        binder.bind(PinotConnector.class).in(Scopes.SINGLETON);
        binder.bind(PinotMetadata.class).in(Scopes.SINGLETON);
        binder.bind(PinotSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(PinotPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(PinotClient.class).in(Scopes.SINGLETON);
        binder.bind(PinotQueryClient.class).in(Scopes.SINGLETON);
        binder.bind(Executor.class).annotatedWith(ForPinot.class)
                .toInstance(newCachedThreadPool(threadsNamed("pinot-metadata-fetcher-" + catalogName)));

        binder.bind(PinotSessionProperties.class).in(Scopes.SINGLETON);
        binder.bind(PinotNodePartitioningProvider.class).in(Scopes.SINGLETON);
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
}
