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
package io.trino.plugin.deltalake;

import com.google.inject.Module;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;
import java.util.Optional;

import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;
import static io.trino.plugin.deltalake.InternalDeltaLakeConnectorFactory.createConnector;
import static java.util.Objects.requireNonNull;

public class DeltaLakeConnectorFactory
        implements ConnectorFactory
{
    public static final String CONNECTOR_NAME = "delta_lake";

    private final Module module;

    public DeltaLakeConnectorFactory()
    {
        this(EMPTY_MODULE);
    }

    public DeltaLakeConnectorFactory(Module module)
    {
        this.module = requireNonNull(module, "module is null");
    }

    @Override
    public String getName()
    {
        return CONNECTOR_NAME;
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        checkStrictSpiVersionMatch(context, this);
        return createConnector(catalogName, config, context, Optional.empty(), Optional.empty(), module);
    }
}
