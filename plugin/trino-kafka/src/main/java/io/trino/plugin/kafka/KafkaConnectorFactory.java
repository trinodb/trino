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
package io.trino.plugin.kafka;

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.trino.plugin.base.CatalogNameModule;
import io.trino.plugin.base.TypeDeserializerModule;
import io.trino.plugin.kafka.security.KafkaSecurityModule;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.type.TypeManager;

import java.util.List;
import java.util.Map;

import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;
import static java.util.Objects.requireNonNull;

public class KafkaConnectorFactory
        implements ConnectorFactory
{
    private final List<Module> extensions;

    public KafkaConnectorFactory(List<Module> extensions)
    {
        this.extensions = ImmutableList.copyOf(extensions);
    }

    @Override
    public String getName()
    {
        return "kafka";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(config, "config is null");
        checkStrictSpiVersionMatch(context, this);

        Bootstrap app = new Bootstrap(
                ImmutableList.<Module>builder()
                        .add(new CatalogNameModule(catalogName))
                        .add(new JsonModule())
                        .add(new TypeDeserializerModule(context.getTypeManager()))
                        .add(new KafkaConnectorModule(context.getTypeManager()))
                        .add(new KafkaClientsModule())
                        .add(new KafkaSecurityModule())
                        .add(binder -> {
                            binder.bind(ClassLoader.class).toInstance(KafkaConnectorFactory.class.getClassLoader());
                            binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                        })
                        .addAll(extensions)
                        .build());

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();

        return injector.getInstance(KafkaConnector.class);
    }
}
