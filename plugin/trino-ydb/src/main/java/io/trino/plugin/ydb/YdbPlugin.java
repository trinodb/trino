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
package io.trino.plugin.ydb;

import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import io.trino.plugin.jdbc.credential.CredentialProviderModule;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

import static io.airlift.configuration.ConfigurationAwareModule.combine;

public record YdbPlugin(Module module)
        implements Plugin
{
    private static final String NAME = "ydb";

    public YdbPlugin()
    {
        this(new YdbClientModule());
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new YdbConnectorFactory(
                NAME,
                () -> combine(
                        new CredentialProviderModule(),
                        module)));
    }
}
