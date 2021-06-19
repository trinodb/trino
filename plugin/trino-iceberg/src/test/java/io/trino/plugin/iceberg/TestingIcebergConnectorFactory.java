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
package io.trino.plugin.iceberg;

import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorHandleResolver;

import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.iceberg.InternalIcebergConnectorFactory.createConnector;
import static java.util.Objects.requireNonNull;

public class TestingIcebergConnectorFactory
        implements ConnectorFactory
{
    private final Optional<HiveMetastore> metastore;
    private final boolean trackMetadataIo;

    public TestingIcebergConnectorFactory(Optional<HiveMetastore> metastore, boolean trackMetadataIo)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.trackMetadataIo = trackMetadataIo;
    }

    @Override
    public String getName()
    {
        return "iceberg";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new IcebergHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        return createConnector(catalogName, config, context, metastore, trackMetadataIo);
    }
}
