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

import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.hive.util.RetryDriver;
import io.trino.spi.security.ConnectorIdentity;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;

public class ThriftHttpMetastoreFactory
        implements ThriftMetastoreFactory
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final IdentityAwareMetastoreClientFactory metastoreClientFactory;
    private final ExecutorService writeStatisticsExecutor;
    private final ThriftMetastoreStats stats = new ThriftMetastoreStats();

    @Inject
    public ThriftHttpMetastoreFactory(
            TrinoFileSystemFactory fileSystemFactory,
            IdentityAwareMetastoreClientFactory metastoreClientFactory,
            @ThriftHiveWriteStatisticsExecutor ExecutorService writeStatisticsExecutor)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.metastoreClientFactory = requireNonNull(metastoreClientFactory, "metastoreClientFactory is null");
        this.writeStatisticsExecutor = requireNonNull(writeStatisticsExecutor, "writeStatisticsExecutor is null");
    }

    @Override
    public boolean isImpersonationEnabled()
    {
        return false;
    }

    @Override
    public ThriftMetastore createMetastore(Optional<ConnectorIdentity> identity)
    {
        // we pass the default values from the ThriftMetastoreConfig for most of these
        // parameters, as they are not used in the in HttpMetastore implementation.
        // if needed we can make them configurable for these parameters in the future.
        return new ThriftHiveMetastore(
                identity,
                fileSystemFactory,
                metastoreClientFactory,
                RetryDriver.DEFAULT_SCALE_FACTOR,
                RetryDriver.DEFAULT_SLEEP_TIME,
                RetryDriver.DEFAULT_SLEEP_TIME,
                RetryDriver.DEFAULT_MAX_RETRY_TIME,
                new Duration(10, MINUTES),
                RetryDriver.DEFAULT_MAX_ATTEMPTS,
                false,
                false,
                false,
                stats,
                writeStatisticsExecutor);
    }

    @Managed
    @Flatten
    public ThriftMetastoreStats getStats()
    {
        return stats;
    }
}
