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

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class TestingIcebergPlugin
        implements Plugin
{
    private final Optional<HiveMetastore> metastore;
    private final boolean trackMetadataIo;

    public TestingIcebergPlugin(HiveMetastore metastore, boolean trackMetadataIo)
    {
        this.metastore = Optional.of(requireNonNull(metastore, "metastore is null"));
        this.trackMetadataIo = trackMetadataIo;
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new TestingIcebergConnectorFactory(metastore, trackMetadataIo));
    }
}
