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
package io.trino.plugin.deltalake.metastore.polaris;

import com.google.inject.Inject;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.plugin.deltalake.metastore.DeltaLakeTableOperations;
import io.trino.plugin.deltalake.metastore.DeltaLakeTableOperationsProvider;
import io.trino.plugin.deltalake.metastore.file.DeltaLakeFileMetastoreTableOperations;
import io.trino.spi.connector.ConnectorSession;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DeltaLakePolarisMetastoreTableOperationsProvider
        implements DeltaLakeTableOperationsProvider
{
    private final HiveMetastoreFactory hiveMetastoreFactory;

    @Inject
    public DeltaLakePolarisMetastoreTableOperationsProvider(HiveMetastoreFactory hiveMetastoreFactory)
    {
        this.hiveMetastoreFactory = requireNonNull(hiveMetastoreFactory, "hiveMetastoreFactory is null");
    }

    @Override
    public DeltaLakeTableOperations createTableOperations(ConnectorSession session)
    {
        // Polaris returns standard HiveMetastore interface, so we can reuse the file-based operations
        return new DeltaLakeFileMetastoreTableOperations(hiveMetastoreFactory.createMetastore(Optional.of(session.getIdentity())));
    }
}
