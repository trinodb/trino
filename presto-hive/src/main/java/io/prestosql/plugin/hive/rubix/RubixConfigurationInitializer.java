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
package io.prestosql.plugin.hive.rubix;

import com.google.common.annotations.VisibleForTesting;
import com.qubole.rubix.prestosql.CachingPrestoGoogleHadoopFileSystem;
import com.qubole.rubix.prestosql.CachingPrestoNativeAzureFileSystem;
import com.qubole.rubix.prestosql.CachingPrestoS3FileSystem;
import com.qubole.rubix.prestosql.PrestoClusterManager;
import io.prestosql.plugin.hive.DynamicConfigurationProvider;
import io.prestosql.plugin.hive.HdfsEnvironment.HdfsContext;
import io.prestosql.spi.HostAddress;
import org.apache.hadoop.conf.Configuration;

import javax.inject.Inject;

import java.net.URI;

import static com.google.common.base.Preconditions.checkState;
import static com.qubole.rubix.spi.CacheConfig.enableHeartbeat;
import static com.qubole.rubix.spi.CacheConfig.setBookKeeperServerPort;
import static com.qubole.rubix.spi.CacheConfig.setCacheDataDirPrefix;
import static com.qubole.rubix.spi.CacheConfig.setCacheDataEnabled;
import static com.qubole.rubix.spi.CacheConfig.setClusterNodeRefreshTime;
import static com.qubole.rubix.spi.CacheConfig.setClusterNodesFetchRetryCount;
import static com.qubole.rubix.spi.CacheConfig.setCoordinatorHostName;
import static com.qubole.rubix.spi.CacheConfig.setCurrentNodeHostName;
import static com.qubole.rubix.spi.CacheConfig.setDataTransferServerPort;
import static com.qubole.rubix.spi.CacheConfig.setEmbeddedMode;
import static com.qubole.rubix.spi.CacheConfig.setIsParallelWarmupEnabled;
import static com.qubole.rubix.spi.CacheConfig.setOnMaster;
import static com.qubole.rubix.spi.CacheConfig.setRubixClusterType;
import static com.qubole.rubix.spi.CacheConfig.setWorkerNodeInfoExpiryPeriod;
import static com.qubole.rubix.spi.ClusterType.PRESTOSQL_CLUSTER_MANAGER;
import static io.prestosql.plugin.hive.DynamicConfigurationProvider.setCacheKey;

public class RubixConfigurationInitializer
        implements DynamicConfigurationProvider
{
    private static final String RUBIX_S3_FS_CLASS_NAME = CachingPrestoS3FileSystem.class.getName();
    private static final String RUBIX_AZURE_FS_CLASS_NAME = CachingPrestoNativeAzureFileSystem.class.getName();
    private static final String RUBIX_GS_FS_CLASS_NAME = CachingPrestoGoogleHadoopFileSystem.class.getName();

    private final boolean parallelWarmupEnabled;
    private final String cacheLocation;
    private final int bookKeeperServerPort;
    private final int dataTransferServerPort;

    // Configs below are dependent on node joining the cluster
    private volatile boolean cacheReady;
    private boolean isMaster;
    private HostAddress masterAddress;
    private String nodeAddress;

    @Inject
    public RubixConfigurationInitializer(RubixConfig config)
    {
        this.parallelWarmupEnabled = config.getReadMode().isParallelWarmupEnabled();
        this.cacheLocation = config.getCacheLocation();
        this.bookKeeperServerPort = config.getBookKeeperServerPort();
        this.dataTransferServerPort = config.getDataTransferServerPort();
    }

    @Override
    public void updateConfiguration(Configuration config, HdfsContext context, URI uri)
    {
        if (!cacheReady) {
            setCacheDataEnabled(config, false);
            setCacheKey(config, "rubix_disabled");
            return;
        }

        updateConfiguration(config);
    }

    void updateConfiguration(Configuration config)
    {
        checkState(masterAddress != null, "masterAddress is not set");
        setCacheDataEnabled(config, true);
        setOnMaster(config, isMaster);
        setCoordinatorHostName(config, masterAddress.getHostText());
        PrestoClusterManager.setPrestoServerPort(config, masterAddress.getPort());
        setCurrentNodeHostName(config, nodeAddress);

        setIsParallelWarmupEnabled(config, parallelWarmupEnabled);
        setCacheDataDirPrefix(config, cacheLocation);
        setBookKeeperServerPort(config, bookKeeperServerPort);
        setDataTransferServerPort(config, dataTransferServerPort);

        setEmbeddedMode(config, true);
        setRubixClusterType(config, PRESTOSQL_CLUSTER_MANAGER);
        enableHeartbeat(config, false);
        setClusterNodeRefreshTime(config, 10);
        setClusterNodesFetchRetryCount(config, Integer.MAX_VALUE);
        setWorkerNodeInfoExpiryPeriod(config, 1);

        config.set("fs.s3.impl", RUBIX_S3_FS_CLASS_NAME);
        config.set("fs.s3a.impl", RUBIX_S3_FS_CLASS_NAME);
        config.set("fs.s3n.impl", RUBIX_S3_FS_CLASS_NAME);
        config.set("fs.wasb.impl", RUBIX_AZURE_FS_CLASS_NAME);
        config.set("fs.gs.impl", RUBIX_GS_FS_CLASS_NAME);

        setCacheKey(config, "rubix_enabled");
    }

    public void setMaster(boolean master)
    {
        isMaster = master;
    }

    public void setMasterAddress(HostAddress masterAddress)
    {
        this.masterAddress = masterAddress;
    }

    public void setCurrentNodeAddress(String nodeAddress)
    {
        this.nodeAddress = nodeAddress;
    }

    public void initializationDone()
    {
        checkState(masterAddress != null, "masterAddress is not set");
        cacheReady = true;
    }

    @VisibleForTesting
    boolean isCacheReady()
    {
        return cacheReady;
    }
}
