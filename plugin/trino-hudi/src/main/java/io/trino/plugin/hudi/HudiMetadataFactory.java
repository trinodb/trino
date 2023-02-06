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
package io.trino.plugin.hudi;

import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.cache.CachingHiveMetastore;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;

import java.util.Optional;

import static io.trino.plugin.hive.metastore.cache.CachingHiveMetastore.memoizeMetastore;
import static java.util.Objects.requireNonNull;

public class HudiMetadataFactory
{
    private final HiveMetastoreFactory metastoreFactory;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final TypeManager typeManager;
    private final long perTransactionMetastoreCacheMaximumSize;

    @Inject
    public HudiMetadataFactory(HiveMetastoreFactory metastoreFactory, TrinoFileSystemFactory fileSystemFactory, TypeManager typeManager, HudiConfig hudiConfig)
    {
        this.metastoreFactory = requireNonNull(metastoreFactory, "metastore is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.perTransactionMetastoreCacheMaximumSize = hudiConfig.getPerTransactionMetastoreCacheMaximumSize();
    }

    public HudiMetadata create(ConnectorIdentity identity)
    {
        // create per-transaction cache over hive metastore interface
        CachingHiveMetastore cachingHiveMetastore = memoizeMetastore(
                metastoreFactory.createMetastore(Optional.of(identity)),
                perTransactionMetastoreCacheMaximumSize);
        return new HudiMetadata(cachingHiveMetastore, fileSystemFactory, typeManager);
    }
}
