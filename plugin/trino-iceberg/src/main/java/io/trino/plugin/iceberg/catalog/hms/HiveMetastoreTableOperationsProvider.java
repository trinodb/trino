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
package io.trino.plugin.iceberg.catalog.hms;

import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreFactory;
import io.trino.plugin.iceberg.catalog.IcebergTableOperations;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.fileio.ForwardingFileIo;
import io.trino.spi.connector.ConnectorSession;

import javax.inject.Inject;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class HiveMetastoreTableOperationsProvider
        implements IcebergTableOperationsProvider
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final ThriftMetastoreFactory thriftMetastoreFactory;

    @Inject
    public HiveMetastoreTableOperationsProvider(TrinoFileSystemFactory fileSystemFactory, ThriftMetastoreFactory thriftMetastoreFactory)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.thriftMetastoreFactory = requireNonNull(thriftMetastoreFactory, "thriftMetastoreFactory is null");
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
        return new HiveMetastoreTableOperations(
                new ForwardingFileIo(fileSystemFactory.create(session)),
                ((TrinoHiveCatalog) catalog).getMetastore(),
                thriftMetastoreFactory.createMetastore(Optional.of(session.getIdentity())),
                session,
                database,
                table,
                owner,
                location);
    }
}
