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
package io.prestosql.plugin.hive;

import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.ConnectorHandleResolver;

import java.util.Map;
import java.util.Optional;

import static io.prestosql.plugin.hive.InternalHiveConnectorFactory.createConnector;
import static java.util.Objects.requireNonNull;

public class TestingHiveConnectorFactory
        implements ConnectorFactory
{
    private final Optional<HiveMetastore> metastore;

    public TestingHiveConnectorFactory()
    {
        this.metastore = Optional.empty();
    }

    public TestingHiveConnectorFactory(HiveMetastore metastore)
    {
        this.metastore = Optional.of(requireNonNull(metastore, "metastore is null"));
    }

    @Override
    public String getName()
    {
        return "hive";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new HiveHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        return createConnector(catalogName, config, context, metastore);
    }
}
