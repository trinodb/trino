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
package io.trino.plugin.redshift;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.jdbc.ExtraCredentialsBasedIdentityCacheMappingModule;
import io.trino.plugin.jdbc.JdbcConnector;
import io.trino.plugin.jdbc.JdbcModule;
import io.trino.plugin.jdbc.credential.CredentialProviderModule;
import io.trino.spi.NodeManager;
import io.trino.spi.VersionEmbedder;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.type.TypeManager;

import java.util.Map;

import static io.airlift.configuration.ConfigurationAwareModule.combine;
import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;
import static java.util.Objects.requireNonNull;

public class RedshiftConnectorFactory
        implements ConnectorFactory
{
    @Override
    public String getName()
    {
        return "redshift";
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
                new CredentialProviderModule(),
                new ExtraCredentialsBasedIdentityCacheMappingModule(),
                new RedshiftClientModule());

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(requiredConfig)
                .initialize();

        return injector.getInstance(RedshiftConnector.class);
    }
}
