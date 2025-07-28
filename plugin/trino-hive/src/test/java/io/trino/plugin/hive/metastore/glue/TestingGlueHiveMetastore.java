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
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastore.TableKind;
import io.trino.spi.block.TestingSession;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
import software.amazon.awssdk.services.glue.GlueClient;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.Optional;
import java.util.function.Consumer;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.exists;
import static java.nio.file.Files.isDirectory;

public final class TestingGlueHiveMetastore
{
    private TestingGlueHiveMetastore() {}

    public static GlueHiveMetastore createTestingGlueHiveMetastore(Path defaultWarehouseDir, Consumer<AutoCloseable> registerResource)
    {
        if (!exists(defaultWarehouseDir)) {
            try {
                createDirectories(defaultWarehouseDir);
            }
            catch (IOException e) {
                throw new RuntimeException("Could not create directory: %s".formatted(defaultWarehouseDir), e);
            }
        }
        verify(isDirectory(defaultWarehouseDir), "%s is not a directory", defaultWarehouseDir);
        return createTestingGlueHiveMetastore(defaultWarehouseDir.toUri(), registerResource);
    }

    public static GlueHiveMetastore createTestingGlueHiveMetastore(URI warehouseUri, Consumer<AutoCloseable> registerResource)
    {
        GlueHiveMetastoreConfig glueConfig = new GlueHiveMetastoreConfig()
                .setDefaultWarehouseDir(warehouseUri.toString());
        GlueClientFactory glueClientFactory = new GlueClientFactory(
                glueConfig,
                Optional.empty(),
                ImmutableSet.of());
        ConnectorSession session = TestingSession.SESSION;
        ConnectorIdentity identity = session.getIdentity();
        GlueClient glueClient = glueClientFactory.create(identity);
        registerResource.accept(glueClient);
        return new GlueHiveMetastore(
                glueClient,
                GlueCache.NOOP,
                new GlueMetastoreStats(),
                HDFS_FILE_SYSTEM_FACTORY,
                glueConfig,
                new CatalogName("test"),
                EnumSet.allOf(TableKind.class));
    }
}
