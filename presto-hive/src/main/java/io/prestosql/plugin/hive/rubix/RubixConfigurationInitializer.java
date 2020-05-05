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

import com.qubole.rubix.hadoop2.CachingNativeAzureFileSystem;
import com.qubole.rubix.prestosql.CachingPrestoAzureBlobFileSystem;
import com.qubole.rubix.prestosql.CachingPrestoDistributedFileSystem;
import com.qubole.rubix.prestosql.CachingPrestoGoogleHadoopFileSystem;
import com.qubole.rubix.prestosql.CachingPrestoNativeAzureFileSystem;
import com.qubole.rubix.prestosql.CachingPrestoS3FileSystem;
import com.qubole.rubix.prestosql.CachingPrestoSecureAzureBlobFileSystem;
import com.qubole.rubix.prestosql.CachingPrestoSecureNativeAzureFileSystem;
import com.qubole.rubix.prestosql.PrestoClusterManager;
import io.prestosql.plugin.hive.ConfigurationInitializer;
import io.prestosql.spi.HostAddress;
import org.apache.hadoop.conf.Configuration;

import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkState;
import static com.qubole.rubix.spi.CacheConfig.enableHeartbeat;
import static com.qubole.rubix.spi.CacheConfig.setBookKeeperServerPort;
import static com.qubole.rubix.spi.CacheConfig.setCacheDataDirPrefix;
import static com.qubole.rubix.spi.CacheConfig.setCacheDataEnabled;
import static com.qubole.rubix.spi.CacheConfig.setClusterNodeRefreshTime;
import static com.qubole.rubix.spi.CacheConfig.setCoordinatorHostName;
import static com.qubole.rubix.spi.CacheConfig.setCurrentNodeHostName;
import static com.qubole.rubix.spi.CacheConfig.setDataTransferServerPort;
import static com.qubole.rubix.spi.CacheConfig.setEmbeddedMode;
import static com.qubole.rubix.spi.CacheConfig.setIsParallelWarmupEnabled;
import static com.qubole.rubix.spi.CacheConfig.setOnMaster;

public class RubixConfigurationInitializer
        implements ConfigurationInitializer
{
    private static final String RUBIX_S3_FS_CLASS_NAME = CachingPrestoS3FileSystem.class.getName();

    private static final String RUBIX_NATIVE_AZURE_FS_CLASS_NAME = CachingPrestoNativeAzureFileSystem.class.getName();
    private static final String RUBIX_SECURE_NATIVE_AZURE_FS_CLASS_NAME = CachingPrestoSecureNativeAzureFileSystem.class.getName();
    private static final String RUBIX_AZURE_BLOB_FS_CLASS_NAME = CachingPrestoAzureBlobFileSystem.class.getName();
    private static final String RUBIX_SECURE_AZURE_BLOB_FS_CLASS_NAME = CachingPrestoSecureAzureBlobFileSystem.class.getName();

    private static final String RUBIX_GS_FS_CLASS_NAME = CachingPrestoGoogleHadoopFileSystem.class.getName();

    private static final String RUBIX_DISTRIBUTED_FS_CLASS_NAME = CachingPrestoDistributedFileSystem.class.getName();

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
    public void initializeConfiguration(Configuration config)
    {
        if (!cacheReady) {
            setCacheDataEnabled(config, false);
            return;
        }

        updateConfiguration(config);
    }

    public Configuration updateConfiguration(Configuration config)
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
        enableHeartbeat(config, false);
        setClusterNodeRefreshTime(config, 10);

        config.set("fs.s3.impl", RUBIX_S3_FS_CLASS_NAME);
        config.set("fs.s3a.impl", RUBIX_S3_FS_CLASS_NAME);
        config.set("fs.s3n.impl", RUBIX_S3_FS_CLASS_NAME);

        config.set("fs.wasb.impl", RUBIX_NATIVE_AZURE_FS_CLASS_NAME);
        config.set("fs.wasbs.impl", RUBIX_SECURE_NATIVE_AZURE_FS_CLASS_NAME);
        config.set("fs.abfs.impl", RUBIX_AZURE_BLOB_FS_CLASS_NAME);
        config.set("fs.abfss.impl", RUBIX_SECURE_AZURE_BLOB_FS_CLASS_NAME);

        config.set("fs.gs.impl", RUBIX_GS_FS_CLASS_NAME);

        config.set("fs.hdfs.impl", RUBIX_DISTRIBUTED_FS_CLASS_NAME);

        return config;
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
}
