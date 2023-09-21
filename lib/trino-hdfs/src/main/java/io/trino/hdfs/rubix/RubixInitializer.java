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
package io.trino.hdfs.rubix;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Closer;
import com.google.inject.Inject;
import com.qubole.rubix.bookkeeper.BookKeeper;
import com.qubole.rubix.bookkeeper.BookKeeperServer;
import com.qubole.rubix.bookkeeper.LocalDataTransferServer;
import com.qubole.rubix.common.metrics.MetricsReporterType;
import com.qubole.rubix.core.CachingFileSystem;
import com.qubole.rubix.prestosql.CachingPrestoAdlFileSystem;
import com.qubole.rubix.prestosql.CachingPrestoAzureBlobFileSystem;
import com.qubole.rubix.prestosql.CachingPrestoGoogleHadoopFileSystem;
import com.qubole.rubix.prestosql.CachingPrestoNativeAzureFileSystem;
import com.qubole.rubix.prestosql.CachingPrestoSecureAzureBlobFileSystem;
import com.qubole.rubix.prestosql.CachingPrestoSecureNativeAzureFileSystem;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeExecutor;
import dev.failsafe.RetryPolicy;
import io.airlift.log.Logger;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.plugin.base.CatalogName;
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.TrinoException;
import jakarta.annotation.Nullable;
import jakarta.annotation.PreDestroy;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.qubole.rubix.spi.CacheConfig.enableHeartbeat;
import static com.qubole.rubix.spi.CacheConfig.setBookKeeperServerPort;
import static com.qubole.rubix.spi.CacheConfig.setCacheDataDirPrefix;
import static com.qubole.rubix.spi.CacheConfig.setCacheDataEnabled;
import static com.qubole.rubix.spi.CacheConfig.setCacheDataExpirationAfterWrite;
import static com.qubole.rubix.spi.CacheConfig.setCacheDataFullnessPercentage;
import static com.qubole.rubix.spi.CacheConfig.setCacheDataOnMasterEnabled;
import static com.qubole.rubix.spi.CacheConfig.setClusterNodeRefreshTime;
import static com.qubole.rubix.spi.CacheConfig.setCoordinatorHostName;
import static com.qubole.rubix.spi.CacheConfig.setDataTransferServerPort;
import static com.qubole.rubix.spi.CacheConfig.setEmbeddedMode;
import static com.qubole.rubix.spi.CacheConfig.setIsParallelWarmupEnabled;
import static com.qubole.rubix.spi.CacheConfig.setMetricsReporters;
import static com.qubole.rubix.spi.CacheConfig.setOnMaster;
import static com.qubole.rubix.spi.CacheConfig.setPrestoClusterManager;
import static io.trino.hdfs.ConfigurationUtils.getInitialConfiguration;
import static io.trino.hdfs.DynamicConfigurationProvider.setCacheKey;
import static io.trino.hdfs.rubix.RubixInitializer.Owner.PRESTO;
import static io.trino.hdfs.rubix.RubixInitializer.Owner.RUBIX;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

/*
 * Responsibilities of this initializer:
 * 1. Wait for master and setup RubixConfigurationInitializer with information about master when it becomes available
 * 2. Start Rubix Servers.
 * 3. Inject BookKeeper object into CachingFileSystem class
 * 4. Update HDFS configuration
 */
public class RubixInitializer
{
    private static final String RUBIX_S3_FS_CLASS_NAME = CachingTrinoS3FileSystem.class.getName();

    private static final String RUBIX_NATIVE_AZURE_FS_CLASS_NAME = CachingPrestoNativeAzureFileSystem.class.getName();
    private static final String RUBIX_SECURE_NATIVE_AZURE_FS_CLASS_NAME = CachingPrestoSecureNativeAzureFileSystem.class.getName();
    private static final String RUBIX_AZURE_BLOB_FS_CLASS_NAME = CachingPrestoAzureBlobFileSystem.class.getName();
    private static final String RUBIX_SECURE_AZURE_BLOB_FS_CLASS_NAME = CachingPrestoSecureAzureBlobFileSystem.class.getName();
    private static final String RUBIX_SECURE_ADL_CLASS_NAME = CachingPrestoAdlFileSystem.class.getName();

    private static final String RUBIX_GS_FS_CLASS_NAME = CachingPrestoGoogleHadoopFileSystem.class.getName();
    private static final String FILESYSTEM_OWNED_BY_RUBIX_CONFIG_PROPETY = "presto.fs.owned.by.rubix";

    private static final FailsafeExecutor<?> DEFAULT_COORDINATOR_FAILSAFE_EXECUTOR = Failsafe.with(RetryPolicy.builder()
            .handle(TrinoException.class)
            .withMaxAttempts(-1)
            .withMaxDuration(Duration.ofMinutes(10))
            .withDelay(Duration.ofSeconds(1))
            .build());

    private static final Logger log = Logger.get(RubixInitializer.class);

    private final FailsafeExecutor<?> coordinatorFailsafeExecutor;
    private final boolean startServerOnCoordinator;
    private final boolean parallelWarmupEnabled;
    private final Optional<String> cacheLocation;
    private final long cacheTtlMillis;
    private final int diskUsagePercentage;
    private final int bookKeeperServerPort;
    private final int dataTransferServerPort;
    private final NodeManager nodeManager;
    private final CatalogName catalogName;
    private final HdfsConfigurationInitializer hdfsConfigurationInitializer;
    private final RubixHdfsInitializer rubixHdfsInitializer;

    private volatile boolean cacheReady;
    @Nullable
    private HostAddress masterAddress;
    @Nullable
    private BookKeeperServer bookKeeperServer;

    @Inject
    public RubixInitializer(
            RubixConfig rubixConfig,
            NodeManager nodeManager,
            CatalogName catalogName,
            HdfsConfigurationInitializer hdfsConfigurationInitializer,
            RubixHdfsInitializer rubixHdfsInitializer)
    {
        this(DEFAULT_COORDINATOR_FAILSAFE_EXECUTOR, rubixConfig, nodeManager, catalogName, hdfsConfigurationInitializer, rubixHdfsInitializer);
    }

    @VisibleForTesting
    RubixInitializer(
            FailsafeExecutor<?> coordinatorFailsafeExecutor,
            RubixConfig rubixConfig,
            NodeManager nodeManager,
            CatalogName catalogName,
            HdfsConfigurationInitializer hdfsConfigurationInitializer,
            RubixHdfsInitializer rubixHdfsInitializer)
    {
        this.coordinatorFailsafeExecutor = coordinatorFailsafeExecutor;
        this.startServerOnCoordinator = rubixConfig.isStartServerOnCoordinator();
        this.parallelWarmupEnabled = rubixConfig.getReadMode().isParallelWarmupEnabled();
        this.cacheLocation = rubixConfig.getCacheLocation();
        this.cacheTtlMillis = rubixConfig.getCacheTtl().toMillis();
        this.diskUsagePercentage = rubixConfig.getDiskUsagePercentage();
        this.bookKeeperServerPort = rubixConfig.getBookKeeperServerPort();
        this.dataTransferServerPort = rubixConfig.getDataTransferServerPort();
        this.nodeManager = nodeManager;
        this.catalogName = catalogName;
        this.hdfsConfigurationInitializer = hdfsConfigurationInitializer;
        this.rubixHdfsInitializer = rubixHdfsInitializer;
    }

    void initializeRubix()
    {
        if (nodeManager.getCurrentNode().isCoordinator() && !startServerOnCoordinator) {
            // setup JMX metrics on master (instead of starting server) so that JMX connector can be used
            // TODO: remove once https://github.com/trinodb/trino/issues/3821 is fixed
            setupRubixMetrics();

            // enable caching on coordinator so that cached block locations can be obtained
            cacheReady = true;
            return;
        }

        if (cacheLocation.isEmpty()) {
            throw new IllegalArgumentException("caching directories were not provided");
        }

        waitForCoordinator();
        startRubix();
    }

    @PreDestroy
    public void stopRubix()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            closer.register(() -> {
                if (bookKeeperServer != null) {
                    // This might throw NPE if Thrift server hasn't started yet (it's initialized
                    // asynchronously from BookKeeperServer thread).
                    // TODO: improve stopping of BookKeeperServer server in Rubix
                    bookKeeperServer.stopServer();
                    bookKeeperServer = null;
                }
            });
            closer.register(LocalDataTransferServer::stopServer);
        }
    }

    public void enableRubix(Configuration configuration)
    {
        if (!cacheReady) {
            disableRubix(configuration);
            return;
        }

        updateRubixConfiguration(configuration, PRESTO);
        setCacheKey(configuration, "rubix_enabled");
    }

    public void disableRubix(Configuration configuration)
    {
        setCacheDataEnabled(configuration, false);
        setCacheKey(configuration, "rubix_disabled");
    }

    public enum Owner
    {
        PRESTO,
        RUBIX,
    }

    public static Owner getConfigurationOwner(Configuration configuration)
    {
        if (configuration.get(FILESYSTEM_OWNED_BY_RUBIX_CONFIG_PROPETY, "").equals("true")) {
            return RUBIX;
        }
        return PRESTO;
    }

    @VisibleForTesting
    boolean isServerUp()
    {
        return LocalDataTransferServer.isServerUp() && bookKeeperServer != null && bookKeeperServer.isServerUp();
    }

    private void waitForCoordinator()
    {
        coordinatorFailsafeExecutor.run(() -> {
            if (nodeManager.getAllNodes().stream().noneMatch(Node::isCoordinator)) {
                // This exception will only be propagated when timeout is reached.
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "No coordinator node available");
            }
        });
    }

    private void startRubix()
    {
        Configuration configuration = getRubixServerConfiguration();

        MetricRegistry metricRegistry = new MetricRegistry();
        bookKeeperServer = new BookKeeperServer();
        BookKeeper bookKeeper = bookKeeperServer.startServer(configuration, metricRegistry);
        LocalDataTransferServer.startServer(configuration, metricRegistry, bookKeeper);

        CachingFileSystem.setLocalBookKeeper(configuration, bookKeeper, "catalog=" + catalogName);
        TrinoClusterManager.setNodeManager(nodeManager);
        log.info("Rubix initialized successfully");
        cacheReady = true;
    }

    private void setupRubixMetrics()
    {
        Configuration configuration = getRubixServerConfiguration();
        new BookKeeperServer().setupServer(configuration, new MetricRegistry());
        CachingFileSystem.setLocalBookKeeper(configuration, new DummyBookKeeper(), "catalog=" + catalogName);
        TrinoClusterManager.setNodeManager(nodeManager);
    }

    private Configuration getRubixServerConfiguration()
    {
        Node master = nodeManager.getAllNodes().stream()
                .filter(Node::isCoordinator)
                .collect(onlyElement());
        masterAddress = master.getHostAndPort();

        Configuration configuration = getInitialConfiguration();
        // Perform standard HDFS configuration initialization.
        hdfsConfigurationInitializer.initializeConfiguration(configuration);
        updateRubixConfiguration(configuration, RUBIX);
        setCacheKey(configuration, "rubix_internal");

        return configuration;
    }

    private void updateRubixConfiguration(Configuration config, Owner owner)
    {
        checkState(masterAddress != null, "masterAddress is not set");
        setCacheDataEnabled(config, true);
        setOnMaster(config, nodeManager.getCurrentNode().isCoordinator());
        setCoordinatorHostName(config, masterAddress.getHostText());

        setIsParallelWarmupEnabled(config, parallelWarmupEnabled);
        setCacheDataExpirationAfterWrite(config, cacheTtlMillis);
        setCacheDataFullnessPercentage(config, diskUsagePercentage);
        setBookKeeperServerPort(config, bookKeeperServerPort);
        setDataTransferServerPort(config, dataTransferServerPort);
        setMetricsReporters(config, MetricsReporterType.JMX.name());

        setEmbeddedMode(config, true);
        enableHeartbeat(config, false);
        setClusterNodeRefreshTime(config, 10);

        if (nodeManager.getCurrentNode().isCoordinator() && !startServerOnCoordinator) {
            // disable initialization of cache directories on master which hasn't got cache explicitly enabled
            setCacheDataOnMasterEnabled(config, false);
        }
        else {
            setCacheDataDirPrefix(config, cacheLocation.orElseThrow());
        }

        config.set("fs.s3.impl", RUBIX_S3_FS_CLASS_NAME);
        config.set("fs.s3a.impl", RUBIX_S3_FS_CLASS_NAME);
        config.set("fs.s3n.impl", RUBIX_S3_FS_CLASS_NAME);

        config.set("fs.wasb.impl", RUBIX_NATIVE_AZURE_FS_CLASS_NAME);
        config.set("fs.wasbs.impl", RUBIX_SECURE_NATIVE_AZURE_FS_CLASS_NAME);
        config.set("fs.abfs.impl", RUBIX_AZURE_BLOB_FS_CLASS_NAME);
        config.set("fs.abfss.impl", RUBIX_SECURE_AZURE_BLOB_FS_CLASS_NAME);
        config.set("fs.adl.impl", RUBIX_SECURE_ADL_CLASS_NAME);

        config.set("fs.gs.impl", RUBIX_GS_FS_CLASS_NAME);

        if (owner == RUBIX) {
            config.set(FILESYSTEM_OWNED_BY_RUBIX_CONFIG_PROPETY, "true");
        }

        setPrestoClusterManager(config, TrinoClusterManager.class.getName());

        rubixHdfsInitializer.initializeConfiguration(config);
    }
}
