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
package io.trino.plugin.iceberg.gpu;

import io.trino.plugin.iceberg.IcebergConnectorFactory;
import io.trino.plugin.iceberg.gpu.engine.GpuEngine;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;
import java.util.Optional;

import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.trino.plugin.base.Versions.checkStrictSpiVersionMatch;

/**
 * {@code connector.name=iceberg_gpu}: the Iceberg connector, configured as one, with filtered
 * aggregations answered on a GPU through TornadoVM. Without TornadoVM on the server, shared with
 * plugins through {@code plugin.shared-packages}, it is the Iceberg connector.
 */
public class IcebergGpuConnectorFactory
        implements ConnectorFactory
{
    @Override
    public String getName()
    {
        return "iceberg_gpu";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        checkStrictSpiVersionMatch(context, this);
        Connector iceberg = IcebergConnectorFactory.createConnector(catalogName, config, context, EMPTY_MODULE, Optional.empty());
        return IcebergGpuConnector.wrap(iceberg, GpuEngine.isAvailable());
    }
}
