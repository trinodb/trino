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
package io.prestosql.iceberg;

import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.hive.metastore.ExtendedHiveMetastore;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class IcebergPlugin
        implements Plugin
{
    private final Optional<ExtendedHiveMetastore> metastore;

    public IcebergPlugin()
    {
        this(Optional.empty());
    }

    public IcebergPlugin(Optional<ExtendedHiveMetastore> metastore)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
    }

    private static ClassLoader getClassLoader()
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = IcebergPlugin.class.getClassLoader();
        }
        return classLoader;
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new IcebergConnectorFactory("iceberg", getClassLoader(), metastore));
    }
}
