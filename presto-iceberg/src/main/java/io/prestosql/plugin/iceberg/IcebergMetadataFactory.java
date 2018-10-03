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
package io.prestosql.plugin.iceberg;

import io.airlift.json.JsonCodec;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.spi.type.TypeManager;

import javax.inject.Inject;

import static io.prestosql.plugin.hive.metastore.cache.CachingHiveMetastore.memoizeMetastore;
import static java.util.Objects.requireNonNull;

public class IcebergMetadataFactory
{
    private final HiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;
    private final JsonCodec<CommitTaskData> commitTaskCodec;
    private final long perTransactionCacheMaximumSize;

    @Inject
    public IcebergMetadataFactory(
            HiveConfig config,
            HiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            JsonCodec<CommitTaskData> commitTaskDataJsonCodec)
    {
        this(metastore,
                hdfsEnvironment,
                typeManager,
                commitTaskDataJsonCodec,
                config.getPerTransactionMetastoreCacheMaximumSize());
    }

    public IcebergMetadataFactory(
            HiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            JsonCodec<CommitTaskData> commitTaskCodec,
            long perTransactionCacheMaximumSize)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.commitTaskCodec = requireNonNull(commitTaskCodec, "commitTaskCodec is null");
        this.perTransactionCacheMaximumSize = perTransactionCacheMaximumSize;
    }

    public IcebergMetadata create()
    {
        return new IcebergMetadata(
                memoizeMetastore(metastore, perTransactionCacheMaximumSize),
                hdfsEnvironment,
                typeManager,
                commitTaskCodec);
    }
}
