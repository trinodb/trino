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
package io.trino.plugin.hive.metastore.glue;

import com.google.common.collect.ImmutableSet;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.hive.FlociS3AndGlue;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastore.TableKind;
import io.trino.spi.catalog.CatalogName;
import software.amazon.awssdk.services.glue.GlueClient;

import java.net.URI;
import java.util.EnumSet;
import java.util.function.Consumer;

import static io.trino.plugin.hive.metastore.glue.GlueMetastoreModule.createGlueClient;

public final class TestingGlueHiveMetastore
{
    private TestingGlueHiveMetastore() {}

    public static GlueHiveMetastore createTestingGlueHiveMetastore(
            URI warehouseUri,
            Consumer<AutoCloseable> registerResource,
            boolean assumeCanonicalPartitionKeys,
            Consumer<GlueHiveMetastoreConfig> configureGlueConfig,
            TrinoFileSystemFactory fileSystemFactory)
    {
        GlueHiveMetastoreConfig glueConfig = new GlueHiveMetastoreConfig()
                .setDefaultWarehouseDir(warehouseUri.toString())
                .setAssumeCanonicalPartitionKeys(assumeCanonicalPartitionKeys);
        configureGlueConfig.accept(glueConfig);
        GlueClient glueClient = createGlueClient(glueConfig, ImmutableSet.of());
        registerResource.accept(glueClient);
        return new GlueHiveMetastore(
                glueClient,
                GlueCache.NOOP,
                new GlueMetastoreStats(),
                fileSystemFactory,
                glueConfig,
                new CatalogName("test"),
                EnumSet.allOf(TableKind.class));
    }

    public static GlueHiveMetastore createTestingGlueHiveMetastore(URI warehouseUri, Consumer<AutoCloseable> registerResource, FlociS3AndGlue floci, boolean assumeCanonicalPartitionKeys)
    {
        return createTestingGlueHiveMetastore(
                warehouseUri,
                registerResource,
                assumeCanonicalPartitionKeys,
                floci::configureGlueHiveMetastore,
                floci.createFileSystemFactory());
    }
}
