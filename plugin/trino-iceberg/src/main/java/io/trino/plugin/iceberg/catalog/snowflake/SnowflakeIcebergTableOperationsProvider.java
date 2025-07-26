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
package io.trino.plugin.iceberg.catalog.snowflake;

import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.iceberg.catalog.IcebergTableOperations;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.fileio.ForwardingFileIoFactory;
import io.trino.spi.connector.ConnectorSession;
import org.apache.iceberg.snowflake.SnowflakeIcebergTableOperations;

import java.util.Optional;

import static io.trino.plugin.iceberg.IcebergSessionProperties.isUseFileSizeFromMetadata;
import static java.util.Objects.requireNonNull;

public class SnowflakeIcebergTableOperationsProvider
        implements IcebergTableOperationsProvider
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final ForwardingFileIoFactory fileIoFactory;
    private final String snowflakeDatabase;

    @Inject
    public SnowflakeIcebergTableOperationsProvider(
            TrinoFileSystemFactory fileSystemFactory,
            ForwardingFileIoFactory fileIoFactory,
            IcebergSnowflakeCatalogConfig icebergSnowflakeCatalogConfig)
    {
        this.snowflakeDatabase = requireNonNull(icebergSnowflakeCatalogConfig.getDatabase(), "database is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.fileIoFactory = requireNonNull(fileIoFactory, "fileIoFactory is null");
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
        return new SnowflakeIcebergTableOperations(
                (TrinoSnowflakeCatalog) catalog,
                fileIoFactory.create(fileSystemFactory.create(session), isUseFileSizeFromMetadata(session)),
                session,
                snowflakeDatabase,
                database,
                table,
                owner,
                location);
    }
}
