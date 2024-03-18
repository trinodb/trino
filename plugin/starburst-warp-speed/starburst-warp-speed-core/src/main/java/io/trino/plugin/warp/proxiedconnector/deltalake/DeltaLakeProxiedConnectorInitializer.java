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
package io.trino.plugin.warp.proxiedconnector.deltalake;

import com.google.inject.Module;
import io.trino.plugin.base.jmx.ConnectorObjectNameGeneratorModule;
import io.trino.plugin.deltalake.DeltaLakeConnectorFactory;
import io.trino.plugin.varada.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.varada.dispatcher.ProxiedConnectorInitializer;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.warp.proxiedconnector.utils.ConfigurationUtils.getDeltaLakeFilteredConfig;

public class DeltaLakeProxiedConnectorInitializer
        implements ProxiedConnectorInitializer
{
    @Override
    public List<Module> getModules(ConnectorContext context)
    {
        return List.of(
                new ConnectorObjectNameGeneratorModule(
                        "io.trino.plugin.deltalake",
                        "trino.plugin.deltalake"),
                binder -> binder.bind(DispatcherProxiedConnectorTransformer.class).to(DeltaLakeProxiedConnectorTransformer.class));
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        try {
            Map<String, String> deltaLakeConfig = getDeltaLakeFilteredConfig(config);
            Module moduleInstance = binder -> {};
            return DeltaLakeConnectorFactory.createConnector(catalogName,
                    deltaLakeConfig,
                    context,
                    Optional.empty(),
                    moduleInstance);
        }
        catch (Exception e) {
            throw new RuntimeException("cant create delta-lake connector", e);
        }
    }
}
