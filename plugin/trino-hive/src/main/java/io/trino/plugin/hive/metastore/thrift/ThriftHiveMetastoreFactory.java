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
package io.trino.plugin.hive.metastore.thrift;

import io.airlift.units.Duration;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.plugin.hive.HideDeltaLakeTables;
import io.trino.spi.security.ConnectorIdentity;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import javax.inject.Inject;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ThriftHiveMetastoreFactory
        implements ThriftMetastoreFactory
{
    private final HdfsEnvironment hdfsEnvironment;
    private final IdentityAwareMetastoreClientFactory metastoreClientFactory;
    private final double backoffScaleFactor;
    private final Duration minBackoffDelay;
    private final Duration maxBackoffDelay;
    private final Duration maxRetryTime;
    private final Duration maxWaitForLock;
    private final int maxRetries;
    private final boolean impersonationEnabled;
    private final boolean deleteFilesOnDrop;
    private final boolean translateHiveViews;
    private final boolean assumeCanonicalPartitionKeys;
    private final boolean useSparkTableStatisticsFallback;
    private final ExecutorService writeStatisticsExecutor;
    private final ThriftMetastoreStats stats = new ThriftMetastoreStats();

    @Inject
    public ThriftHiveMetastoreFactory(
            IdentityAwareMetastoreClientFactory metastoreClientFactory,
            @HideDeltaLakeTables boolean hideDeltaLakeTables,
            @TranslateHiveViews boolean translateHiveViews,
            ThriftMetastoreConfig thriftConfig,
            HdfsEnvironment hdfsEnvironment,
            @ThriftHiveWriteStatisticsExecutor ExecutorService writeStatisticsExecutor)
    {
        this.metastoreClientFactory = requireNonNull(metastoreClientFactory, "metastoreClientFactory is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.backoffScaleFactor = thriftConfig.getBackoffScaleFactor();
        this.minBackoffDelay = thriftConfig.getMinBackoffDelay();
        this.maxBackoffDelay = thriftConfig.getMaxBackoffDelay();
        this.maxRetryTime = thriftConfig.getMaxRetryTime();
        this.maxRetries = thriftConfig.getMaxRetries();
        this.impersonationEnabled = thriftConfig.isImpersonationEnabled();
        this.deleteFilesOnDrop = thriftConfig.isDeleteFilesOnDrop();
        this.translateHiveViews = translateHiveViews;
        checkArgument(!hideDeltaLakeTables, "Hiding Delta Lake tables is not supported"); // TODO
        this.maxWaitForLock = thriftConfig.getMaxWaitForTransactionLock();

        this.assumeCanonicalPartitionKeys = thriftConfig.isAssumeCanonicalPartitionKeys();
        this.useSparkTableStatisticsFallback = thriftConfig.isUseSparkTableStatisticsFallback();
        this.writeStatisticsExecutor = requireNonNull(writeStatisticsExecutor, "writeStatisticsExecutor is null");
    }

    @Managed
    @Flatten
    public ThriftMetastoreStats getStats()
    {
        return stats;
    }

    @Override
    public boolean isImpersonationEnabled()
    {
        return impersonationEnabled;
    }

    @Override
    public ThriftMetastore createMetastore(Optional<ConnectorIdentity> identity)
    {
        return new ThriftHiveMetastore(
                identity,
                hdfsEnvironment,
                metastoreClientFactory,
                backoffScaleFactor,
                minBackoffDelay,
                maxBackoffDelay,
                maxRetryTime,
                maxWaitForLock,
                maxRetries,
                deleteFilesOnDrop,
                translateHiveViews,
                assumeCanonicalPartitionKeys,
                useSparkTableStatisticsFallback,
                stats,
                writeStatisticsExecutor);
    }
}
