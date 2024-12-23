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
package io.trino.plugin.jdbc;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.BootstrapConfig;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.base.config.ConfigPropertyMetadata;
import io.trino.plugin.base.mapping.MappingConfig;
import io.trino.plugin.jdbc.credential.CredentialConfig;
import io.trino.plugin.jdbc.credential.CredentialProviderTypeConfig;
import io.trino.plugin.jdbc.credential.ExtraCredentialConfig;
import io.trino.plugin.jdbc.credential.file.ConfigFileBasedCredentialProviderConfig;
import io.trino.plugin.jdbc.credential.keystore.KeyStoreBasedCredentialProviderConfig;
import io.trino.plugin.jdbc.logging.FormatBasedRemoteQueryModifierConfig;
import io.trino.spi.NodeManager;
import io.trino.spi.VersionEmbedder;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.type.TypeManager;

import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;
import static io.trino.plugin.base.config.ConfigPropertyMetadata.getConfigProperties;
import static java.util.Objects.requireNonNull;

public class JdbcConnectorFactory
        implements ConnectorFactory
{
    private final String name;
    private final Supplier<Module> module;
    private final Set<String> nonRedactablePropertyNames;

    public JdbcConnectorFactory(String name, Supplier<Module> module, Set<ConfigPropertyMetadata> additionalProperties)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
        this.module = requireNonNull(module, "module is null");
        Set<Class<?>> configClasses = ImmutableSet.<Class<?>>builder()
                .add(BaseJdbcConfig.class)
                .add(CredentialConfig.class)
                .add(JdbcStatisticsConfig.class)
                .add(JdbcWriteConfig.class)
                .add(QueryConfig.class)
                .add(RemoteQueryCancellationConfig.class)
                .add(TypeHandlingJdbcConfig.class)
                .add(JdbcMetadataConfig.class)
                .add(JdbcJoinPushdownConfig.class)
                .add(DecimalConfig.class)
                .add(JdbcDynamicFilteringConfig.class)
                .add(KeyStoreBasedCredentialProviderConfig.class)
                .add(FormatBasedRemoteQueryModifierConfig.class)
                .add(ConfigFileBasedCredentialProviderConfig.class)
                .add(CredentialProviderTypeConfig.class)
                .add(ExtraCredentialConfig.class)
                .add(MappingConfig.class)
                .add(BootstrapConfig.class)
                .build();
        this.nonRedactablePropertyNames = Stream.concat(
                        configClasses.stream().flatMap(clazz -> getConfigProperties(clazz).stream()),
                        requireNonNull(additionalProperties, "additionalProperties is null").stream())
                .filter(property -> !property.sensitive())
                .map(ConfigPropertyMetadata::name)
                .collect(toImmutableSet());
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public Connector create(String catalogName, Map<String, String> requiredConfig, ConnectorContext context)
    {
        requireNonNull(requiredConfig, "requiredConfig is null");
        checkStrictSpiVersionMatch(context, this);

        Bootstrap app = new Bootstrap(
                binder -> binder.bind(TypeManager.class).toInstance(context.getTypeManager()),
                binder -> binder.bind(NodeManager.class).toInstance(context.getNodeManager()),
                binder -> binder.bind(VersionEmbedder.class).toInstance(context.getVersionEmbedder()),
                binder -> binder.bind(OpenTelemetry.class).toInstance(context.getOpenTelemetry()),
                binder -> binder.bind(CatalogName.class).toInstance(new CatalogName(catalogName)),
                new JdbcModule(),
                module.get());

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(requiredConfig)
                .initialize();

        return injector.getInstance(JdbcConnector.class);
    }

    @Override
    public Set<String> getRedactablePropertyNames(Set<String> propertyNames)
    {
        return Sets.difference(propertyNames, nonRedactablePropertyNames);
    }
}
