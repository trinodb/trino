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
import com.google.inject.Module;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.hive.fs.DirectoryLister;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Optional;

import static com.google.inject.util.Modules.EMPTY_MODULE;
import static java.util.Objects.requireNonNull;

public class TestingHivePlugin
        implements Plugin
{
    private final Optional<HiveMetastore> metastore;
    private final Optional<OpenTelemetry> openTelemetry;
    private final Module module;
    private final Optional<DirectoryLister> directoryLister;

    public TestingHivePlugin()
    {
        this(Optional.empty(), Optional.empty(), EMPTY_MODULE, Optional.empty());
    }

    public TestingHivePlugin(HiveMetastore metastore)
    {
        this(Optional.of(metastore), Optional.empty(), EMPTY_MODULE, Optional.empty());
    }

    public TestingHivePlugin(Optional<HiveMetastore> metastore, Optional<OpenTelemetry> openTelemetry, Module module, Optional<DirectoryLister> directoryLister)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.openTelemetry = requireNonNull(openTelemetry, "openTelemetry is null");
        this.module = requireNonNull(module, "module is null");
        this.directoryLister = requireNonNull(directoryLister, "directoryLister is null");
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new TestingHiveConnectorFactory(metastore, openTelemetry, module, directoryLister));
    }
}
