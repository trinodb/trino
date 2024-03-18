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
package io.trino.plugin.varada.di;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import io.trino.plugin.varada.annotations.ForWarp;
import io.trino.plugin.varada.metrics.MetricsModule;
import io.trino.spi.connector.ConnectorContext;
import io.varada.cloudvendors.CloudVendorModule;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class VaradaModules
        extends AbstractModule
        implements VaradaBaseModule
{
    private final String connectorId;
    private final Map<String, String> config;
    private final ConnectorContext context;
    private Optional<Module> storageEngineModule = Optional.empty();
    private Optional<Module> cloudVendorModule = Optional.empty();
    private Optional<List<ExtraModule>> extraModules = Optional.empty();

    public VaradaModules(String connectorId, Map<String, String> config, ConnectorContext context)
    {
        this.connectorId = connectorId;
        this.config = requireNonNull(config);
        this.context = requireNonNull(context);
    }

    @Override
    protected void configure()
    {
        install(new MetricsModule(connectorId));
        install(storageEngineModule.orElseGet(() -> new VaradaNativeStorageEngineModule(context, config)));
        install(cloudVendorModule.orElse(CloudVendorModule.getModule(context, ForWarp.class, connectorId, config)));
        install(new VaradaMainModule(context, config));

        extraModules.ifPresent(modules -> modules.stream()
                .map(externalModule -> externalModule.withConfig(config).withContext(context))
                .filter(VaradaBaseModule::shouldInstall)
                .forEach(this::install));
    }

    public VaradaModules withStorageEngineModule(Module module)
    {
        this.storageEngineModule = Optional.ofNullable(module);
        return this;
    }

    public VaradaModules withExtraModules(List<ExtraModule> extraModules)
    {
        this.extraModules = Optional.ofNullable(extraModules);
        return this;
    }

    public VaradaModules withCloudVendorModule(Module module)
    {
        this.cloudVendorModule = Optional.ofNullable(module);
        return this;
    }
}
