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
package io.trino.metastore;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.filesystem.FileSystemClientManager;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.filesystem.HetuFileSystemClient;
import io.trino.spi.metastore.HetuMetaStoreFactory;
import io.trino.spi.metastore.HetuMetastore;
import io.trino.spi.statestore.StateStore;
import io.trino.statestore.StateStoreProvider;
import io.trino.util.CatalogStoreUtil;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static io.trino.spi.StandardErrorCode.CATALOG_NOT_AVAILABLE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * HetuMetaStoreManager manages the lifecycle of HetuMetaStores
 *
 * @since 2020-04-26
 */
public class HetuMetaStoreManager
{
    private static final Logger LOG = Logger.get(HetuMetaStoreManager.class);
    // properties type name
    private static final File HETUMETASTORE_CONFIG_FILE = new File("etc/trino-metastore.properties");
    private static final String HETU_METASTORE_TYPE_PROPERTY_NAME = "trino.metastore.type";
    private static final String HETU_METASTORE_TYPE_HETU_FILE_SYSTEM = "msfilesystem";
    private static final String HETU_METASTORE_HETU_FILE_SYSTEM_PROFILE_NAME = "trino.metastore.msfilesystem.profile-name";
    private static final String HETU_METASTORE_CACHE_TYPE = "trino.metastore.cache.type";
    private static final String HETU_METASTORE_CACHE_TYPE_DEFAULT = "local";

    // properties default type value
    private static final String HETU_METASTORE_TYPE_DEFAULT_VALUE = "jdbc";

    private final Map<String, HetuMetaStoreFactory> hetuMetastoreFactories = new ConcurrentHashMap<>();
    private HetuMetastore hetuMetastore;
    private String hetuMetastoreType;
    private String metaCacheType;
    private StateStoreProvider stateStoreProvider;
    private StateStore stateStore;

    public HetuMetaStoreManager() {}

    @Inject
    public HetuMetaStoreManager(StateStoreProvider stateStoreProvider)
    {
        this.stateStoreProvider = stateStoreProvider;
    }

    public void addHetuMetaStoreFactory(HetuMetaStoreFactory hetuMetaStoreFactory)
    {
        requireNonNull(hetuMetaStoreFactory, "trinoMetaStoreFactory is null");

        if (hetuMetastoreFactories.putIfAbsent(hetuMetaStoreFactory.getName(), hetuMetaStoreFactory) != null) {
            throw new IllegalArgumentException(format("TrinoMetastore '%s' is already registered", hetuMetaStoreFactory.getName()));
        }
    }

    public void loadHetuMetastore(FileSystemClientManager fileSystemClientManager, Map<String, String> config)
            throws IOException
    {
        // create hetu metastore
        hetuMetastoreType = config.getOrDefault(HETU_METASTORE_TYPE_PROPERTY_NAME, HETU_METASTORE_TYPE_DEFAULT_VALUE);
        metaCacheType = config.getOrDefault(HETU_METASTORE_CACHE_TYPE, HETU_METASTORE_CACHE_TYPE_DEFAULT);
        config.remove(HETU_METASTORE_TYPE_PROPERTY_NAME);
        config.remove(HETU_METASTORE_CACHE_TYPE);
        HetuMetaStoreFactory hetuMetaStoreFactory = hetuMetastoreFactories.get(hetuMetastoreType);
        checkState(hetuMetaStoreFactory != null, "trinoMetaStoreFactory %s is not registered", hetuMetaStoreFactory);
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(HetuMetaStoreFactory.class.getClassLoader())) {
            HetuFileSystemClient client = null;
            LOG.error("1");
            if (HETU_METASTORE_TYPE_DEFAULT_VALUE.equals(hetuMetastoreType)) {
                if (config.containsKey("encrypted-properties") && config.containsKey("encrypted-key")) {
                    LOG.error("2");
                    CatalogStoreUtil catalogStoreUtil = new CatalogStoreUtil();
                    catalogStoreUtil.decryptEncryptedProperties(config);
                    LOG.error("3");
                }
            }
            LOG.error("4");
            if (HETU_METASTORE_TYPE_HETU_FILE_SYSTEM.equals(hetuMetastoreType)) {
                String profileName = config.get(HETU_METASTORE_HETU_FILE_SYSTEM_PROFILE_NAME);
                client = fileSystemClientManager.getFileSystemClient(profileName, Paths.get("/"));
            }
            LOG.error("5");
            if (stateStoreProvider == null) {
                stateStore = null;
                LOG.info("-- stateStore is null --");
            }
            else {
                stateStore = stateStoreProvider.getStateStore();
            }

            LOG.error("6");
            hetuMetastore = hetuMetaStoreFactory.create(hetuMetastoreType, ImmutableMap.copyOf(config), client, stateStore, metaCacheType);
            LOG.error("7");
        }
        catch (Exception e) {
            LOG.error(e.getMessage());
            throw new TrinoException(CATALOG_NOT_AVAILABLE, "trino metastore configured with invalid encryted properties");
        }

        LOG.info("-- Loaded Trino Metastore %s --", hetuMetastoreType);
    }

    public void loadHetuMetastore(FileSystemClientManager fileSystemClientManager)
            throws IOException
    {
        LOG.info("-- Loading Trino Metastore --");
        if (HETUMETASTORE_CONFIG_FILE.exists()) {
            // load configuration
            Map<String, String> config = new HashMap<>(loadPropertiesFrom(HETUMETASTORE_CONFIG_FILE.getPath()));
            loadHetuMetastore(fileSystemClientManager, config);
        }
    }

    public HetuMetastore getHetuMetastore()
    {
        return hetuMetastore;
    }
}
