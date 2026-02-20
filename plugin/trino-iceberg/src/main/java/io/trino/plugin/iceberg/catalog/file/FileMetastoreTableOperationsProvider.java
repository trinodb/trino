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
package io.trino.plugin.iceberg.catalog.file;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.iceberg.catalog.IcebergTableOperations;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.hms.TrinoHiveCatalog;
import io.trino.plugin.iceberg.fileio.ForwardingFileIoFactory;
import io.trino.spi.connector.ConnectorSession;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.encryption.KeyManagementClient;

import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.iceberg.IcebergSessionProperties.isUseFileSizeFromMetadata;
import static java.util.Objects.requireNonNull;

public class FileMetastoreTableOperationsProvider
        implements IcebergTableOperationsProvider
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final ForwardingFileIoFactory fileIoFactory;
    private final Optional<KeyManagementClient> keyManagementClient;

    @Inject
    public FileMetastoreTableOperationsProvider(
            TrinoFileSystemFactory fileSystemFactory,
            ForwardingFileIoFactory fileIoFactory,
            IcebergFileMetastoreEncryptionConfig encryptionConfig)
    {
        this(fileSystemFactory, fileIoFactory, createKeyManagementClient(encryptionConfig));
    }

    public FileMetastoreTableOperationsProvider(
            TrinoFileSystemFactory fileSystemFactory,
            ForwardingFileIoFactory fileIoFactory)
    {
        this(fileSystemFactory, fileIoFactory, Optional.empty());
    }

    public FileMetastoreTableOperationsProvider(
            TrinoFileSystemFactory fileSystemFactory,
            ForwardingFileIoFactory fileIoFactory,
            Optional<KeyManagementClient> keyManagementClient)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.fileIoFactory = requireNonNull(fileIoFactory, "fileIoFactory is null");
        this.keyManagementClient = requireNonNull(keyManagementClient, "keyManagementClient is null");
    }

    @Override
    public IcebergTableOperations createTableOperations(
            TrinoCatalog catalog,
            ConnectorSession session,
            String database,
            String table,
            Optional<String> owner,
            Optional<String> location)
    {
        return new FileMetastoreTableOperations(
                fileIoFactory.create(fileSystemFactory.create(session), isUseFileSizeFromMetadata(session)),
                ((TrinoHiveCatalog) catalog).getMetastore(),
                session,
                database,
                table,
                owner,
                location,
                keyManagementClient);
    }

    private static Optional<KeyManagementClient> createKeyManagementClient(IcebergFileMetastoreEncryptionConfig encryptionConfig)
    {
        requireNonNull(encryptionConfig, "encryptionConfig is null");

        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        encryptionConfig.getKmsType().ifPresent(value -> properties.put(CatalogProperties.ENCRYPTION_KMS_TYPE, value));
        encryptionConfig.getKmsImpl().ifPresent(value -> properties.put(CatalogProperties.ENCRYPTION_KMS_IMPL, value));

        Map<String, String> catalogProperties = properties.buildOrThrow();
        if (catalogProperties.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(EncryptionUtil.createKmsClient(catalogProperties));
    }
}
