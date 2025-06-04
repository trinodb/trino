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
package io.trino.plugin.tpch;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorNodePartitioningProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class TpchModule
        extends AbstractConfigurationAwareModule
{
    private final NodeManager nodeManager;
    private final int defaultSplitsPerNode;
    private final boolean predicatePushdownEnabled;

    public TpchModule(NodeManager nodeManager, int defaultSplitsPerNode, boolean predicatePushdownEnabled)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.defaultSplitsPerNode = defaultSplitsPerNode;
        this.predicatePushdownEnabled = predicatePushdownEnabled;
    }

    @Override
    protected void setup(Binder binder)
    {
        binder.bind(NodeManager.class).toInstance(nodeManager);
        binder.bind(Connector.class).to(TpchConnector.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorMetadata.class).to(TpchMetadata.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorPageSourceProvider.class).to(TpchPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorSplitManager.class).to(TpchSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorNodePartitioningProvider.class).to(TpchNodePartitioningProvider.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(TpchConfig.class);
        configBinder(binder).bindConfigDefaults(TpchConfig.class, config -> {
            config.setSplitsPerNode(defaultSplitsPerNode);
            config.setPredicatePushdownEnabled(predicatePushdownEnabled);
        });
    }
}
