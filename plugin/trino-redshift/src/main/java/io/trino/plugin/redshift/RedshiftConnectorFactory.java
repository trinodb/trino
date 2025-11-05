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
import io.trino.plugin.base.ConnectorContextModule;
import io.trino.plugin.jdbc.ExtraCredentialsBasedIdentityCacheMappingModule;
import io.trino.plugin.jdbc.JdbcModule;
import io.trino.plugin.jdbc.credential.CredentialProviderModule;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;

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
                "io.trino.bootstrap.catalog." + catalogName,
                new ConnectorContextModule(catalogName, context),
                new JdbcModule(),
                new CredentialProviderModule(),
                new ExtraCredentialsBasedIdentityCacheMappingModule(),
                new RedshiftClientModule());

        Injector injector = app
                .doNotInitializeLogging()
                .disableSystemProperties()
                .setRequiredConfigurationProperties(requiredConfig)
                .initialize();

        return injector.getInstance(Connector.class);
    }
}
