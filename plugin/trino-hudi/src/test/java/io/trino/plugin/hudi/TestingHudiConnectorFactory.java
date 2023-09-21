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
package io.trino.plugin.hudi;

import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.hudi.InternalHudiConnectorFactory.createConnector;
import static java.util.Objects.requireNonNull;

public class TestingHudiConnectorFactory
        implements ConnectorFactory
{
    private final Optional<HiveMetastore> metastore;

    public TestingHudiConnectorFactory(Optional<HiveMetastore> metastore)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
    }

    @Override
    public String getName()
    {
        return "hudi";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        return createConnector(catalogName, config, context, metastore, Optional.empty());
    }
}
