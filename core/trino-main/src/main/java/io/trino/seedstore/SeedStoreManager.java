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
package io.trino.seedstore;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.filesystem.FileSystemClientManager;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.filesystem.HetuFileSystemClient;
import io.trino.spi.seedstore.Seed;
import io.trino.spi.seedstore.SeedStore;
import io.trino.spi.seedstore.SeedStoreFactory;
import io.trino.spi.seedstore.SeedStoreSubType;
import io.trino.statestore.StateStoreConstants;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static io.trino.spi.StandardErrorCode.SEED_STORE_FAILURE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.stream.Collectors.toCollection;

/**
 * SeedStoreManager manages the lifecycle of SeedStores
 *
 * @since 2020-03-06
 */
public class SeedStoreManager
{
    private static final Logger LOG = Logger.get(SeedStoreManager.class);

    // configuration files
    private static final File SEED_STORE_CONFIGURATION = new File("etc/seed-store.properties");
    private static final File STATE_STORE_CONFIGURATION = new File("etc/state-store.properties");
    // properties name
    private static final String SEED_STORE_ON_YARN_PROPERTY_NAME = "seed-store.on-yarn";
    private static final String SEED_STORE_TYPE_PROPERTY_NAME = "seed-store.type";
    private static final String SEED_STORE_CLUSTER_PROPERTY_NAME = "seed-store.cluster";
    private static final String SEED_STORE_SEED_HEARTBEAT_PROPERTY_NAME = "seed-store.seed.heartbeat";
    private static final String SEED_STORE_SEED_HEARTBEAT_TIMEOUT_PROPERTY_NAME = "seed-store.seed.heartbeat.timeout";
    private static final String SEED_STORE_FILESYSTEM_PROFILE = "seed-store.filesystem.profile";
    // properties default value
    private static final boolean SEED_STORE_ON_YARN_ENABLED_DEFAULT_VALUE = false;
    private static final String SEED_STORE_TYPE_DEFAULT_VALUE = "filebased";
    private static final String SEED_STORE_CLUSTER_DEFAULT_VALUE = "olk_default";
    private static final long SEED_STORE_SEED_HEARTBEAT_DEFAULT_VALUE = 10000; // 10 seconds
    private static final long SEED_STORE_SEED_HEARTBEAT_TIMEOUT_DEFAULT_VALUE = 60000; // 60 seconds
    private static final String SEED_STORE_FILESYSTEM_PROFILE_DEFAULT_VALUE = "hdfs-config-default";
    private static final int SEED_RETRY_TIMES = 5;
    private static final long SEED_RETRY_INTERVAL = 500L;

    private final Map<String, SeedStoreFactory> seedStoreFactories = new ConcurrentHashMap<>();
    private final FileSystemClientManager fileSystemClientManager;
    private HetuFileSystemClient fileSystemClient;
    private ScheduledExecutorService seedRefreshExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("SeedRefresher"));

    private String seedStoreType;
    private String clusterName;
    private String filesystemProfile;
    private boolean isSeedStoreOnYarnEnabled;
    private long seedHeartBeat;
    private long seedHeartBeatTimeout;
    private boolean isSeedStoreHazelcastEnabled;

    private SeedStore seedStoreOnYarn;
    private SeedStore seedStoreHazelcast;

    private ConcurrentHashMap<String, Seed> refreshableSeedsMap = new ConcurrentHashMap<>();

    @Inject
    public SeedStoreManager(FileSystemClientManager fileSystemClientManager)
    {
        this.fileSystemClientManager = requireNonNull(fileSystemClientManager);
    }

    /**
     * Register SeedStoreFactory to SeedStoreManager to create SeedStore
     *
     * @param factory SeedStoreFactory to create SeedStore
     */
    public void addSeedStoreFactory(SeedStoreFactory factory)
    {
        if (seedStoreFactories.putIfAbsent(factory.getName(), factory) != null) {
            throw new IllegalArgumentException(format("Seed Store '%s' is already registered", factory.getName()));
        }
    }

    /**
     * Use the registered SeedStoreFactory to load SeedStore
     *
     * @throws IOException exception when fail to create SeedStore
     */
    public void loadSeedStore()
            throws IOException
    {
        // initialize variables
        isSeedStoreOnYarnEnabled = SEED_STORE_ON_YARN_ENABLED_DEFAULT_VALUE;
        seedStoreType = SEED_STORE_TYPE_DEFAULT_VALUE;
        clusterName = SEED_STORE_CLUSTER_DEFAULT_VALUE;
        filesystemProfile = SEED_STORE_FILESYSTEM_PROFILE_DEFAULT_VALUE;
        seedHeartBeat = SEED_STORE_SEED_HEARTBEAT_DEFAULT_VALUE;
        seedHeartBeatTimeout = SEED_STORE_SEED_HEARTBEAT_TIMEOUT_DEFAULT_VALUE;

        Map<String, String> config = new HashMap<>();
        // load seed store configuration
        if (SEED_STORE_CONFIGURATION.exists()) {
            Map<String, String> seedStoreProperties = new HashMap<>(loadPropertiesFrom(SEED_STORE_CONFIGURATION.getPath()));
            String propertyValue = seedStoreProperties.get(SEED_STORE_ON_YARN_PROPERTY_NAME);
            if (propertyValue != null) {
                isSeedStoreOnYarnEnabled = Boolean.parseBoolean(propertyValue);
            }
            seedStoreType = seedStoreProperties.getOrDefault(SEED_STORE_TYPE_PROPERTY_NAME, seedStoreType);
            clusterName = seedStoreProperties.getOrDefault(SEED_STORE_CLUSTER_PROPERTY_NAME, clusterName);
            filesystemProfile = seedStoreProperties.getOrDefault(SEED_STORE_FILESYSTEM_PROFILE, filesystemProfile);
            propertyValue = seedStoreProperties.get(SEED_STORE_SEED_HEARTBEAT_PROPERTY_NAME);
            if (propertyValue != null) {
                seedHeartBeat = Long.parseLong(propertyValue);
            }
            propertyValue = seedStoreProperties.get(SEED_STORE_SEED_HEARTBEAT_TIMEOUT_PROPERTY_NAME);
            if (propertyValue != null) {
                seedHeartBeatTimeout = Long.parseLong(propertyValue);
            }
            if (seedHeartBeat > seedHeartBeatTimeout) {
                throw new InvalidParameterException(format("The value of %s cannot be greater than the value of %s in the property file",
                        SEED_STORE_SEED_HEARTBEAT_PROPERTY_NAME, SEED_STORE_SEED_HEARTBEAT_TIMEOUT_PROPERTY_NAME));
            }
            config.putAll(seedStoreProperties);
            if (isSeedStoreOnYarnEnabled) {
                // create seed store
                SeedStoreFactory seedStoreFactory = seedStoreFactories.get(seedStoreType);
                checkState(seedStoreFactory != null, "SeedStoreFactory %s is not registered", seedStoreFactory);
                if (fileSystemClient == null) {
                    fileSystemClient = fileSystemClientManager.getFileSystemClient(filesystemProfile, Paths.get("/"));
                }
                LOG.info("-- Loading seed store on-yarn --");
                try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(seedStoreFactory.getClass().getClassLoader())) {
                    seedStoreOnYarn = seedStoreFactory.create(clusterName,
                            SeedStoreSubType.ON_YARN,
                            fileSystemClient,
                            ImmutableMap.copyOf(config));
                }
            }
        }
        // load state store config if exist
        if (STATE_STORE_CONFIGURATION.exists()) {
            Map<String, String> stateStoreProperties = new HashMap<>(loadPropertiesFrom(STATE_STORE_CONFIGURATION.getPath()));
            // for now, seed store is started only if tcp-ip mode enabled and tcp-ip.seeds is not set
            String discoveryMode = stateStoreProperties.get(StateStoreConstants.DISCOVERY_MODE_PROPERTY_NAME);
            isSeedStoreHazelcastEnabled = discoveryMode != null && discoveryMode.equals(StateStoreConstants.DISCOVERY_MODE_TCPIP)
                    && stateStoreProperties.get(StateStoreConstants.HAZELCAST_DISCOVERY_TCPIP_SEEDS) == null;
            if (isSeedStoreHazelcastEnabled) {
                // create seed store
                SeedStoreFactory seedStoreFactory = seedStoreFactories.get(seedStoreType);
                checkState(seedStoreFactory != null, "SeedStoreFactory %s is not registered", seedStoreFactory);
                if (fileSystemClient == null) {
                    String stateStoreFilesystemProfile = stateStoreProperties.getOrDefault(StateStoreConstants.HAZELCAST_DISCOVERY_TCPIP_PROFILE, filesystemProfile);
                    fileSystemClient = fileSystemClientManager.getFileSystemClient(stateStoreFilesystemProfile, Paths.get("/"));
                }
                LOG.info("-- Loading seed store hazelcast --");
                try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(seedStoreFactory.getClass().getClassLoader())) {
                    seedStoreHazelcast = seedStoreFactory.create(clusterName,
                            SeedStoreSubType.HAZELCAST,
                            fileSystemClient,
                            ImmutableMap.copyOf(config));
                }
                try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(seedStoreHazelcast.getClass().getClassLoader())) {
                    // start seed refresher
                    seedRefreshExecutor.scheduleWithFixedDelay(() -> refreshSeeds(SeedStoreSubType.HAZELCAST), 0, seedHeartBeat, TimeUnit.MILLISECONDS);
                }
            }
        }
    }

    /**
     * Check if seed-store on-YARN is enabled
     */
    public boolean isSeedStoreOnYarnEnabled()
    {
        return isSeedStoreOnYarnEnabled;
    }

    /**
     * Get file system client
     */

    public HetuFileSystemClient getFileSystemClient()
    {
        return fileSystemClient;
    }

    /**
     * Get all seeds from seed store
     *
     * @return a collection of seeds in the seed store
     * @throws IOException if seed store is null
     */
    public Collection<Seed> getAllSeeds(SeedStoreSubType subType)
            throws IOException
    {
        SeedStore seedStore = getSeedStore(subType);
        if (seedStore == null) {
            throw new TrinoException(SEED_STORE_FAILURE, "Seed store is null");
        }

        Collection<Seed> seeds = seedStore.get();

        return seeds;
    }

    /**
     * Get all seeds from seed store
     *
     * @return the latest seed location in the seed store - https or http
     * @throws IOException if seed store is null
     */
    public String getLatestSeedLocation(SeedStoreSubType subType, boolean httpsRequired)
            throws IOException
    {
        SeedStore seedStore = getSeedStore(subType);
        if (seedStore == null) {
            throw new TrinoException(SEED_STORE_FAILURE, "Seed store is null");
        }

        Collection<Seed> seeds = seedStore.get();
        if (seeds.isEmpty()) {
            return null;
        }
        ArrayList<Seed> list = seeds.stream()
                .filter(seed -> {
                    if (httpsRequired) {
                        return seed.getLocation().contains("https:");
                    }
                    else {
                        return seed.getLocation().contains("http:");
                    }
                })
                .sorted(Comparator.comparing(Seed::getTimestamp))
                .collect(toCollection(ArrayList::new));
        if (list.isEmpty()) {
            return null;
        }
        else {
            return list.get(list.size() - 1).getLocation();
        }
    }

    /**
     * Add seed to seed store. If refreshable is enabled, seed will be refreshed periodically
     *
     * @param refreshable refreshable
     * @return a collection of seeds in the seed store
     * @throws IOException if seed store is null
     */

    public Collection<Seed> addSeed(SeedStoreSubType subType, String seedLocation, boolean refreshable)
            throws IOException
    {
        Collection<Seed> seeds = new HashSet<>();

        SeedStore seedStore = getSeedStore(subType);
        if (seedStore == null) {
            throw new TrinoException(SEED_STORE_FAILURE, "Seed store is null");
        }

        Seed seed = seedStore.create(ImmutableMap.of(
                Seed.LOCATION_PROPERTY_NAME, seedLocation,
                Seed.TIMESTAMP_PROPERTY_NAME, String.valueOf(System.currentTimeMillis())));
        seeds = addSeed(subType, seed);

        if (refreshable) {
            refreshableSeedsMap.put(seedLocation, seed);
        }

        LOG.debug("Seed=%s added to seed store", seedLocation);
        return seeds;
    }

    /**
     * remove seed from seed store
     *
     * @param seedLocation seedLocation
     * @return a collection of seeds in the seed store
     * @throws IOException if seed store is null
     */

    public Collection<Seed> removeSeed(SeedStoreSubType subType, String seedLocation)
            throws IOException
    {
        Collection<Seed> seeds = new HashSet<>();

        SeedStore seedStore = getSeedStore(subType);
        if (seedStore == null) {
            throw new TrinoException(SEED_STORE_FAILURE, "Seed store is null");
        }

        refreshableSeedsMap.remove(seedLocation);
        Optional<Seed> seedOptional;
        if (subType == SeedStoreSubType.ON_YARN) {
            seedOptional = seedStore.get()
                    .stream()
                    .filter(s -> {
                        return s.getLocation().equals(seedLocation) || s.getUniqueInstanceId().equals(seedLocation);
                    })
                    .findFirst();
        }
        else {
            seedOptional = seedStore.get().stream().filter(s -> s.getLocation().equals(seedLocation)).findFirst();
        }

        if (seedOptional.isPresent()) {
            seeds = seedStore.remove(Lists.newArrayList(seedOptional.get()));
            LOG.debug("Seed=%s removed from seed store", seedLocation);
        }

        return seeds;
    }

    /**
     * Clear expired seed in the seed store
     *
     * @throws IOException if seed store is null
     */
    public void clearExpiredSeeds(SeedStoreSubType subType)
            throws IOException
    {
        SeedStore seedStore = getSeedStore(subType);
        if (seedStore == null) {
            throw new TrinoException(SEED_STORE_FAILURE, "Seed store is null");
        }
        try {
            Collection<Seed> expiredSeeds = seedStore.get()
                    .stream()
                    .filter(s -> (System.currentTimeMillis() - s.getTimestamp() > seedHeartBeatTimeout))
                    .collect(Collectors.toList());
            if (expiredSeeds.size() > 0) {
                LOG.info("Expired seeds=%s will be cleared", expiredSeeds);
                seedStore.remove(expiredSeeds);
            }
        }
        catch (RuntimeException e) {
            LOG.warn("clearExpiredSeed failed with following message: %s", e.getMessage());
        }
    }

    /**
     * Update the existing seed at seedLocation with new properties
     *
     * @param subType subType
     * @param seedLocation seedLocation
     * @param updatedProperties updatedProperties
     */
    public void updateSeed(SeedStoreSubType subType, String seedLocation, Map<String, String> updatedProperties)
    {
        SeedStore seedStore = getSeedStore(subType);
        if (seedStore == null) {
            throw new TrinoException(SEED_STORE_FAILURE, "Seed store is null");
        }

        try {
            Collection<Seed> existingSeeds = seedStore.get();
            List<Seed> toUpdate = existingSeeds.stream()
                    .filter(s -> {
                        return s.getLocation().equals(seedLocation);
                    })
                    .collect(Collectors.toList());
            LOG.debug("SeedStoreManager::updateSeed toUpdate.size() is %s", Integer.toString(toUpdate.size()));
            for (Seed s : toUpdate) {
                seedStore.remove(ImmutableList.of(s));
                Seed newSeed = seedStore.create(updatedProperties);
                addSeed(subType, newSeed);
            }
        }
        catch (IOException e) {
            LOG.warn("Update seed %s failed with error: %s", seedLocation, e.getMessage());
        }
    }

    private Collection<Seed> addSeed(SeedStoreSubType subType, Seed seed)
            throws IOException
    {
        int retryTimes = 0;
        long retryInterval = 0L;
        Collection<Seed> seeds = null;

        SeedStore seedStore = getSeedStore(subType);
        if (seedStore == null) {
            throw new TrinoException(SEED_STORE_FAILURE, "Seed store is null");
        }
        do {
            try {
                TimeUnit.MILLISECONDS.sleep(retryInterval);
                seeds = seedStore.add(Lists.newArrayList(seed));
            }
            catch (InterruptedException | RuntimeException e) {
                LOG.warn("add seed=%s failed: %s, will retry at times: %s", seed, e.getMessage(), retryTimes);
            }
            finally {
                retryTimes++;
                retryInterval += SEED_RETRY_INTERVAL;
            }
        }
        while (retryTimes <= SEED_RETRY_TIMES && (seeds == null || seeds.size() == 0));

        if (seeds == null || seeds.size() == 0) {
            throw new TrinoException(SEED_STORE_FAILURE, String.format(Locale.ROOT, "add seed=%s to seed store failed after retry:%d",
                    seed.getLocation(), SEED_RETRY_TIMES));
        }

        return seeds;
    }

    @VisibleForTesting
    /**
     * Set SeedStore
     *
     * Set the local SeedStore
     * */
    public void setSeedStore(SeedStoreSubType subType, SeedStore seedStore)
    {
        if (subType == SeedStoreSubType.ON_YARN) {
            seedStoreOnYarn = seedStore;
        }
        else {
            seedStoreHazelcast = seedStore;
        }
    }

    /**
     * Get loaded SeedStore
     *
     * @return loaded SeedStore or null if it's not loaded
     */
    public SeedStore getSeedStore(SeedStoreSubType subType)
    {
        SeedStore seedStore = (subType == SeedStoreSubType.ON_YARN) ? seedStoreOnYarn : seedStoreHazelcast;
        return seedStore;
    }

    private void refreshSeeds(SeedStoreSubType subType)
    {
        SeedStore seedStore = getSeedStore(subType);
        if (seedStore == null) {
            throw new TrinoException(SEED_STORE_FAILURE, "Seed store is null");
        }
        for (Map.Entry<String, Seed> entry : refreshableSeedsMap.entrySet()) {
            long newTime = System.currentTimeMillis();
            LOG.debug("seed=%s refresh with oldTimestamp=%s and newTimestamp=%s", entry.getKey(), entry.getValue().getTimestamp(), newTime);
            Seed newSeed = seedStore.create(ImmutableMap.of(
                    Seed.LOCATION_PROPERTY_NAME, entry.getKey(),
                    Seed.TIMESTAMP_PROPERTY_NAME, String.valueOf(newTime)));
            try {
                seedStore.add(Lists.newArrayList(newSeed));
                entry.setValue(newSeed);
            }
            catch (IOException | RuntimeException e) {
                LOG.warn("Error refresh seed=%s with error message: %s, will refresh in next %s milliseconds",
                        entry.getKey(), e.getMessage(), seedHeartBeat);
                continue;
            }
        }
    }
}
