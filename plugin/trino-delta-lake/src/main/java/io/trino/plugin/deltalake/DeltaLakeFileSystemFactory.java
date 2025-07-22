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
package io.trino.plugin.deltalake;

import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.deltalake.metastore.DeltaMetastoreTable;
import io.trino.plugin.deltalake.metastore.VendedCredentialsHandle;
import io.trino.spi.connector.ConnectorSession;

public interface DeltaLakeFileSystemFactory
        extends TrinoFileSystemFactory
{
    default TrinoFileSystem create(ConnectorSession session, DeltaLakeTableHandle table)
    {
        return create(session, table.toCredentialsHandle());
    }

    default TrinoFileSystem create(ConnectorSession session, DeltaMetastoreTable table)
    {
        return create(session, new VendedCredentialsHandle(table.catalogOwned(), table.managed(), table.location(), table.vendedCredentials()));
    }

    TrinoFileSystem create(ConnectorSession session, VendedCredentialsHandle table);

    /**
     * For external table create/write using location
     */
    TrinoFileSystem create(ConnectorSession session, String tableLocation);

    @Override
    default TrinoFileSystem create(ConnectorSession session)
    {
        throw new UnsupportedOperationException();
    }
}
