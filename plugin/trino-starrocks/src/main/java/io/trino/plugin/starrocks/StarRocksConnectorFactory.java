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
package io.trino.plugin.starrocks;

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.trino.plugin.base.ConnectorContextModule;
import io.trino.plugin.base.TypeDeserializerModule;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;
import static java.util.Objects.requireNonNull;

public class StarRocksConnectorFactory
        implements ConnectorFactory
{
    private final Optional<Module> extension;

    public StarRocksConnectorFactory()
    {
        this(Optional.empty());
    }

    StarRocksConnectorFactory(Optional<Module> extension)
    {
        this.extension = requireNonNull(extension, "extension is null");
    }

    @Override
    public String getName()
    {
        return "starrocks";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> requiredConfig, ConnectorContext context)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(requiredConfig, "requiredConfig is null");
        checkStrictSpiVersionMatch(context, this);

        Module starRocksModule = extension
                .map(module -> Modules.override(new StarRocksModule()).with(module))
                .orElseGet(StarRocksModule::new);

        List<Module> modules = ImmutableList.of(
                new JsonModule(),
                new TypeDeserializerModule(),
                new ConnectorContextModule(catalogName, context),
                starRocksModule);

        Bootstrap app = new Bootstrap(
                "io.trino.bootstrap.catalog." + catalogName,
                modules);

        Injector injector = app
                .doNotInitializeLogging()
                .disableSystemProperties()
                .setRequiredConfigurationProperties(requiredConfig)
                .initialize();

        return injector.getInstance(StarRocksConnector.class);
    }
}
