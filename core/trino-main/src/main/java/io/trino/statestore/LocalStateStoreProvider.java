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
package io.trino.statestore;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.metastore.MetaStoreConstants;
import io.trino.seedstore.SeedStoreManager;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.seedstore.SeedStoreSubType;
import io.trino.spi.statestore.StateCollection;
import io.trino.spi.statestore.StateStore;
import io.trino.spi.statestore.StateStoreFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static io.trino.spi.StandardErrorCode.STATE_STORE_FAILURE;
import static io.trino.statestore.StateStoreConstants.STATE_STORE_CONFIGURATION_PATH;
import static io.trino.statestore.StateStoreConstants.STATE_STORE_NAME_PROPERTY_NAME;
import static io.trino.statestore.StateStoreConstants.STATE_STORE_TYPE_PROPERTY_NAME;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * LocalStateStoreProvider manages the lifecycle of local StateStore clients
 *
 * @since 2020-03-04
 */
public class LocalStateStoreProvider
        implements StateStoreProvider
{
    private static final Logger log = Logger.get(LocalStateStoreProvider.class);
    private static final File STATE_STORE_CONFIGURATION = new File(STATE_STORE_CONFIGURATION_PATH);
    private static final String DEFAULT_STATE_STORE_NAME = "default-state-store";
    private static final long SLEEP_INTERVAL = 2000L;

    private final Map<String, StateStoreFactory> stateStoreFactories = new ConcurrentHashMap<>();
    private StateStore stateStore;
    private final SeedStoreManager seedStoreManager;

    @Inject
    public LocalStateStoreProvider(SeedStoreManager seedStoreManager)
    {
        this.seedStoreManager = requireNonNull(seedStoreManager, "seedStoreManager is null");
    }

    @Override
    public void addStateStoreFactory(StateStoreFactory factory)
    {
        if (stateStoreFactories.putIfAbsent(factory.getName(), factory) != null) {
            throw new IllegalArgumentException(format("State Store '%s' is already registered", factory.getName()));
        }
    }

    @Override
    public void loadStateStore()
            throws Exception
    {
        if (STATE_STORE_CONFIGURATION.exists()) {
            Map<String, String> properties = new HashMap<>(loadPropertiesFrom(STATE_STORE_CONFIGURATION.getPath()));
            String stateStoreType = properties.remove(STATE_STORE_TYPE_PROPERTY_NAME);
            setStateStore(stateStoreType, properties);
            createStateCollections();
        }
        else {
            log.info("No configuration file found, skip loading state store client");
        }
    }

    public void setStateStore(String stateStoreType, Map<String, String> properties)
    {
        requireNonNull(stateStoreType, "stateStoreType is null");
        requireNonNull(properties, "properties is null");

        log.info("-- Loading state store --");
        StateStoreFactory stateStoreFactory = stateStoreFactories.get(stateStoreType);
        checkState(stateStoreFactory != null, "State store %s is not registered", stateStoreType);
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(stateStoreFactory.getClass().getClassLoader())) {
            String stateStoreName = properties.remove(STATE_STORE_NAME_PROPERTY_NAME);
            if (stateStoreName == null) {
                log.info("State store name not provided, using default state store name: %s", DEFAULT_STATE_STORE_NAME);
                stateStoreName = DEFAULT_STATE_STORE_NAME;
            }
            // Create state stores defined in config
            stateStore = stateStoreFactory.create(stateStoreName, seedStoreManager.getSeedStore(SeedStoreSubType.HAZELCAST), ImmutableMap.copyOf(properties));
            stateStore.registerClusterFailureHandler(this::handleClusterDisconnection);
            stateStore.init();
        }
        catch (Exception e) {
            throw new TrinoException(STATE_STORE_FAILURE, "Unable to create state store: " + e.getMessage());
        }
        log.info("-- Loaded state store %s --", stateStoreType);
    }

    @Override
    public StateStore getStateStore()
    {
        return stateStore;
    }

    public void createStateCollections()
    {
        // Create essential state collections
        stateStore.createStateCollection(StateStoreConstants.DISCOVERY_SERVICE_COLLECTION_NAME, StateCollection.Type.MAP);
        stateStore.createStateCollection(StateStoreConstants.QUERY_STATE_COLLECTION_NAME, StateCollection.Type.MAP);
        stateStore.createStateCollection(StateStoreConstants.FINISHED_QUERY_STATE_COLLECTION_NAME, StateCollection.Type.MAP);
        stateStore.createStateCollection(StateStoreConstants.OOM_QUERY_STATE_COLLECTION_NAME, StateCollection.Type.MAP);
        stateStore.createStateCollection(StateStoreConstants.CPU_USAGE_STATE_COLLECTION_NAME, StateCollection.Type.MAP);
        stateStore.createStateCollection(StateStoreConstants.TRANSACTION_STATE_COLLECTION_NAME, StateCollection.Type.MAP);

        stateStore.createStateCollection(MetaStoreConstants.HETU_META_STORE_CATALOGCACHE_NAME, StateCollection.Type.MAP);
        stateStore.createStateCollection(MetaStoreConstants.HETU_META_STORE_CATALOGSCACHE_NAME, StateCollection.Type.MAP);
        stateStore.createStateCollection(MetaStoreConstants.HETU_META_STORE_TABLECACHE_NAME, StateCollection.Type.MAP);
        stateStore.createStateCollection(MetaStoreConstants.HETU_META_STORE_TABLESCACHE_NAME, StateCollection.Type.MAP);
        stateStore.createStateCollection(MetaStoreConstants.HETU_META_STORE_DATABASECACHE_NAME, StateCollection.Type.MAP);
        stateStore.createStateCollection(MetaStoreConstants.HETU_META_STORE_DATABASESCACHE_NAME, StateCollection.Type.MAP);
    }

    void handleClusterDisconnection(Object obj)
    {
        log.info("Connection to Hazelcast state store has SHUTDOWN.");
        while (true) {
            try {
                Thread.sleep(SLEEP_INTERVAL);
                seedStoreManager.loadSeedStore();
                loadStateStore();
                break;
            }
            catch (Exception ex) {
                log.info("Failed to reload state store: %s", ex.getMessage());
            }
        }
    }
}
