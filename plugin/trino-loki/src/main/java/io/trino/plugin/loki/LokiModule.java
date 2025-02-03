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
package io.trino.plugin.loki;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.github.jeschkies.loki.client.LokiClient;
import io.github.jeschkies.loki.client.LokiClientConfig;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.function.table.ConnectorTableFunction;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class LokiModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(LokiConnector.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorMetadata.class).to(LokiMetadata.class).in(Scopes.SINGLETON);
        binder.bind(LokiMetadata.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorSplitManager.class).to(LokiSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorRecordSetProvider.class).to(LokiRecordSetProvider.class).in(Scopes.SINGLETON);
        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(LokiTableFunctionProvider.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(LokiConfig.class);
    }

    @Provides
    @Singleton
    public static LokiClient getLokiClient(LokiConfig config)
    {
        return new LokiClient(new LokiClientConfig(config.getUri(), config.getQueryTimeout().toJavaTime()));
    }
}
