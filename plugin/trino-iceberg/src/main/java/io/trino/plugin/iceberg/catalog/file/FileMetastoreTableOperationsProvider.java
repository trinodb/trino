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

import io.trino.plugin.hive.HdfsEnvironment.HdfsContext;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.iceberg.FileIoProvider;
import io.trino.plugin.iceberg.catalog.IcebergTableOperations;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.spi.connector.ConnectorSession;

import javax.inject.Inject;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class FileMetastoreTableOperationsProvider
        implements IcebergTableOperationsProvider
{
    private final FileIoProvider fileIoProvider;

    @Inject
    public FileMetastoreTableOperationsProvider(FileIoProvider fileIoProvider)
    {
        this.fileIoProvider = requireNonNull(fileIoProvider, "fileIoProvider is null");
    }

    @Override
    public IcebergTableOperations createTableOperations(
            HiveMetastore hiveMetastore,
            ConnectorSession session,
            String database,
            String table,
            Optional<String> owner,
            Optional<String> location)
    {
        return new FileMetastoreTableOperations(
                fileIoProvider.createFileIo(new HdfsContext(session), session.getQueryId()),
                hiveMetastore,
                session,
                database,
                table,
                owner,
                location);
    }
}
