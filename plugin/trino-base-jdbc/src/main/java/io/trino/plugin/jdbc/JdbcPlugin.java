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

import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import io.trino.plugin.jdbc.credential.CredentialProviderModule;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.airlift.configuration.ConfigurationAwareModule.combine;
import static java.util.Objects.requireNonNull;

public class JdbcPlugin
        implements Plugin
{
    private final String name;
    private final Supplier<Module> module;

    public JdbcPlugin(String name, Supplier<Module> module)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
        this.module = requireNonNull(module, "module is null");
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new JdbcConnectorFactory(
                name,
                () -> combine(
                        new CredentialProviderModule(),
                        new ExtraCredentialsBasedIdentityCacheMappingModule(),
                        module.get())));
    }
}
