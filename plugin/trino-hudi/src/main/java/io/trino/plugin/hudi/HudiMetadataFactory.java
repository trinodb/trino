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
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.hive.TrinoViewHiveMetastore;
import io.trino.spi.NodeVersion;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;

import java.util.Optional;

import static io.trino.metastore.cache.CachingHiveMetastore.createPerTransactionCache;
import static java.util.Objects.requireNonNull;

public class HudiMetadataFactory
{
    private final HiveMetastoreFactory metastoreFactory;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final TypeManager typeManager;
    private final long perTransactionMetastoreCacheMaximumSize;
    private final String trinoVersion;

    @Inject
    public HudiMetadataFactory(HiveMetastoreFactory metastoreFactory,
                               TrinoFileSystemFactory fileSystemFactory,
                               TypeManager typeManager,
                               HudiConfig hudiConfig,
                               NodeVersion nodeVersion)
    {
        this.metastoreFactory = requireNonNull(metastoreFactory, "metastore is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.perTransactionMetastoreCacheMaximumSize = hudiConfig.getPerTransactionMetastoreCacheMaximumSize();
        this.trinoVersion = requireNonNull(nodeVersion, "nodeVersion is null").toString();
    }

    public HudiMetadata create(ConnectorIdentity identity)
    {
        CachingHiveMetastore cachingHiveMetastore = createPerTransactionCache(metastoreFactory.createMetastore(Optional.of(identity)), perTransactionMetastoreCacheMaximumSize);
        TrinoViewHiveMetastore trinoViewHiveMetastore = new TrinoViewHiveMetastore(
                cachingHiveMetastore,
                false, // todo add SystemSecurity awareness to the Hudi connector
                trinoVersion,
                "Trino Hudi connector");
        return new HudiMetadata(cachingHiveMetastore, trinoViewHiveMetastore, fileSystemFactory, typeManager);
    }
}
