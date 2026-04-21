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
import io.trino.spi.security.ConnectorIdentity;

public interface DeltaLakeFileSystemFactory
        extends TrinoFileSystemFactory
{
    default TrinoFileSystem create(ConnectorSession session, DeltaLakeTableHandle table)
    {
        return create(session, table.toCredentialsHandle());
    }

    default TrinoFileSystem create(ConnectorSession session, DeltaMetastoreTable table)
    {
        return create(session, VendedCredentialsHandle.of(table));
    }

    TrinoFileSystem create(ConnectorSession session, VendedCredentialsHandle table);

    /**
     * For external table create/write using location
     */
    TrinoFileSystem create(ConnectorSession session, String tableLocation);

    /**
     * @deprecated Use {@link #create(ConnectorSession, VendedCredentialsHandle)} or {@link #create(ConnectorSession, String)}
     * instead. The new methods can potentially support vending credentials and may pass more information
     * when creating {@link TrinoFileSystem} in the future.
     */
    @Deprecated
    @Override
    default TrinoFileSystem create(ConnectorIdentity identity)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * @deprecated Use {@link #create(ConnectorSession, VendedCredentialsHandle)} or {@link #create(ConnectorSession, String)}
     * instead. The new methods can potentially support vending credentials and may pass more information
     * when creating {@link TrinoFileSystem} in the future.
     */
    @Deprecated
    @Override
    default TrinoFileSystem create(ConnectorSession session)
    {
        throw new UnsupportedOperationException();
    }
}
