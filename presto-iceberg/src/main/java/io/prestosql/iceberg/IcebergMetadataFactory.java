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
package io.prestosql.iceberg;

import io.airlift.concurrent.BoundedExecutor;
import io.airlift.json.JsonCodec;
import io.prestosql.plugin.hive.ForHiveClient;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveClientConfig;
import io.prestosql.plugin.hive.LocationService;
import io.prestosql.plugin.hive.TransactionalMetadata;
import io.prestosql.plugin.hive.metastore.CachingHiveMetastore;
import io.prestosql.plugin.hive.metastore.ExtendedHiveMetastore;
import io.prestosql.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.prestosql.spi.type.TypeManager;

import javax.inject.Inject;

import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class IcebergMetadataFactory
        implements Supplier<TransactionalMetadata>
{
    @Override
    public TransactionalMetadata get()
    {
        SemiTransactionalHiveMetastore metastore = new SemiTransactionalHiveMetastore(
                hdfsEnvironment,
                CachingHiveMetastore.memoizeMetastore(this.metastore, perTransactionCacheMaximumSize), // per-transaction cache
                renameExecution,
                skipDeletionForAlter,
                true);

        return new IcebergMetadata(
                metastore,
                hdfsEnvironment,
                typeManager,
                taskCommitCodec,
                icebergUtil,
                locationService);
    }

    private final boolean skipDeletionForAlter;
    private final long perTransactionCacheMaximumSize;
    private final ExtendedHiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;
    private final JsonCodec<CommitTaskData> taskCommitCodec;
    private final BoundedExecutor renameExecution;
    private final IcebergUtil icebergUtil;
    private final LocationService locationService;

    @Inject
    @SuppressWarnings("deprecation")
    public IcebergMetadataFactory(
            HiveClientConfig hiveClientConfig,
            ExtendedHiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            @ForHiveClient ExecutorService executorService,
            TypeManager typeManager,
            JsonCodec<CommitTaskData> commitTaskDataJsonCodec,
            IcebergUtil icebergUtil,
            LocationService locationService)
    {
        this(
                metastore,
                hdfsEnvironment,
                hiveClientConfig.getMaxConcurrentFileRenames(),
                hiveClientConfig.isSkipDeletionForAlter(),
                hiveClientConfig.getPerTransactionMetastoreCacheMaximumSize(),
                typeManager,
                executorService,
                commitTaskDataJsonCodec,
                icebergUtil,
                locationService);
    }

    public IcebergMetadataFactory(
            ExtendedHiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            int maxConcurrentFileRenames,
            boolean skipDeletionForAlter,
            long perTransactionCacheMaximumSize,
            TypeManager typeManager,
            ExecutorService executorService,
            JsonCodec<CommitTaskData> commitTaskDataJsonCodec,
            IcebergUtil icebergUtil,
            LocationService locationService)
    {
        this.skipDeletionForAlter = skipDeletionForAlter;
        this.perTransactionCacheMaximumSize = perTransactionCacheMaximumSize;

        this.metastore = requireNonNull(metastore, "metastore is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.icebergUtil = icebergUtil;
        this.locationService = locationService;

        renameExecution = new BoundedExecutor(executorService, maxConcurrentFileRenames);
        this.taskCommitCodec = commitTaskDataJsonCodec;
    }
}
