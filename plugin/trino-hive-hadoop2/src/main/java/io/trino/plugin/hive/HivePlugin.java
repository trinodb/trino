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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.trino.spi.Plugin;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;
import java.util.Set;

public class HivePlugin
        implements Plugin
{
    private static final Logger log = Logger.get(HivePlugin.class);

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new HiveConnectorFactory("hive"), new LegacyHiveConnectorFactory());
    }

    @Override
    public Set<Class<?>> getFunctions()
    {
        return ImmutableSet.of(CanonicalizeHiveTimezoneId.class);
    }

    private static class LegacyHiveConnectorFactory
            extends HiveConnectorFactory
    {
        public LegacyHiveConnectorFactory()
        {
            super("hive-hadoop2");
        }

        @Override
        public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
        {
            log.warn("Connector name 'hive-hadoop2' is deprecated. Use 'hive' instead.");
            return super.create(catalogName, config, context);
        }
    }
}
