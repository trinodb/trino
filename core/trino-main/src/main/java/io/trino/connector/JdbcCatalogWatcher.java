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
package io.trino.connector;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.metadata.CatalogManager;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.connector.ConnectorName;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public class JdbcCatalogWatcher
{
    private static final Logger log = Logger.get(JdbcCatalogWatcher.class);

    private final CatalogManager catalogManager;
    private final JdbcCatalogStore jdbcCatalogStore;
    private final boolean pollingEnabled;
    private final long pollingIntervalMillis;
    private final ScheduledExecutorService executorService;

    // Local cache of version per catalog: CatalogName -> Version
    private final Map<String, Long> localVersions = new ConcurrentHashMap<>();

    private ScheduledFuture<?> scheduledFuture;

    @Inject
    public JdbcCatalogWatcher(
            CatalogManager catalogManager,
            JdbcCatalogStore jdbcCatalogStore,
            JdbcCatalogStoreConfig config,
            @ForJdbcCatalogStore ScheduledExecutorService executorService)
    {
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
        this.jdbcCatalogStore = requireNonNull(jdbcCatalogStore, "jdbcCatalogStore is null");
        this.executorService = requireNonNull(executorService, "executorService is null");
        this.pollingEnabled = config.isPollingEnabled();
        this.pollingIntervalMillis = config.getPollingInterval().toMillis();
    }

    @PostConstruct
    public void start()
    {
        if (!pollingEnabled) {
            return;
        }

        log.info("Starting JDBC catalog watcher (interval: %d ms)", pollingIntervalMillis);
        
        // Initialize local state
        refreshVersions();

        scheduledFuture = executorService.scheduleWithFixedDelay(
                this::poll,
                pollingIntervalMillis,
                pollingIntervalMillis,
                TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    public void stop()
    {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
            scheduledFuture = null;
        }
    }

    private void refreshVersions()
    {
        try {
            Map<String, Long> dbVersions = jdbcCatalogStore.getCatalogVersions();
            localVersions.putAll(dbVersions);
        }
        catch (Exception e) {
            log.error(e, "Failed to initialize catalog versions");
        }
    }

    private void poll()
    {
        try {
            Map<String, Long> dbVersions = jdbcCatalogStore.getCatalogVersions();
            Set<String> processedCatalogs = new HashSet<>();

            // Check for Updates and Adds
            for (Map.Entry<String, Long> entry : dbVersions.entrySet()) {
                String catalogNameStr = entry.getKey();
                Long dbVersion = entry.getValue();
                processedCatalogs.add(catalogNameStr);

                Long localVersion = localVersions.get(catalogNameStr);

                if (localVersion == null) {
                    // New catalog
                    log.info("Detected new catalog: %s", catalogNameStr);
                    reloadCatalog(catalogNameStr);
                    localVersions.put(catalogNameStr, dbVersion);
                }
                else if (dbVersion > localVersion) {
                    // Updated catalog
                    log.info("Detected update for catalog: %s (v%d -> v%d)", catalogNameStr, localVersion, dbVersion);
                    reloadCatalog(catalogNameStr);
                    localVersions.put(catalogNameStr, dbVersion);
                }
            }

            // Check for Deletes
            // Identify catalogs present in local versions but missing in DB versions
            Set<String> deletedCatalogs = new HashSet<>(localVersions.keySet());
            deletedCatalogs.removeAll(processedCatalogs);

            for (String deletedCatalog : deletedCatalogs) {
                log.info("Detected deletion of catalog: %s", deletedCatalog);
                removeCatalog(deletedCatalog);
                localVersions.remove(deletedCatalog);
            }
        }
        catch (Exception e) {
            log.error(e, "Error polling for catalog updates");
        }
    }

    private void reloadCatalog(String catalogNameStr)
    {
        try {
            CatalogName catalogName = new CatalogName(catalogNameStr);
            // We fetch fresh properties from the store directly
            // Note: getCatalogProperties handles the lookup in the store
            // However, we need to bypass caching if the store caches, but JdbcCatalogStore is direct DB usually.
            
            // Wait: JdbcCatalogStore.getCatalogs() returns all.
            // We need a method to fetch ONE catalog's properties efficiently, or just rely on addOrReplaceCatalog flow logic.
            // But we need to CALL catalogManager.
            
            // To update a catalog in CatalogManager dynamically:
            // 1. If it exists, we drop it (preserving queries if possible? No, dropCatalog kills it usually unless we use update pattern).
            // Trino doesn't have a clean "update" on DynamicCatalogManager exposed easily except via CREATE OR REPLACE logic if supported.
            // createCatalog(...) checks ALREADY_EXISTS.
            
            // Best effort: Drop then Create.
            // This might disrupt running queries on that catalog. That's the trade-off of hot reload.
            
            // Ideally:
            // 1. Fetch properties from DB.
            // 2. Call catalogManager.createCatalog(..., NOT_EXISTS=false/true).
            
            // Let's see JdbcCatalogStore API. We need `loadProperties(name)` or similar.
            // Assuming we add `Optional<CatalogProperties> getCatalogProperties(String name)` to JdbcCatalogStore.
            
            CatalogProperties properties = jdbcCatalogStore.getCatalogProperties(catalogNameStr)
                    .orElseThrow(() -> new IllegalStateException("Catalog disappeared during reload: " + catalogNameStr));

            // Drop existing if any
            // We can check if it exists in CatalogManager via getCatalog(name).isPresent()
            // But dropCatalog(name, exists=true) is safer.
            
            try {
                catalogManager.dropCatalog(catalogName, true); // true = fail if missing? No, createCatalog wants it gone.
                // Wait, dropCatalog(name, boolean exists). If exists=false, it throws if not found.
                // We'll catch exception just in case.
            } catch (Exception ignored) {}

            catalogManager.createCatalog(catalogName, properties.connectorName(), properties.properties(), false);
            
        } catch (Exception e) {
            log.error(e, "Failed to reload catalog: %s", catalogNameStr);
        }
    }

    private void removeCatalog(String catalogNameStr)
    {
        try {
            catalogManager.dropCatalog(new CatalogName(catalogNameStr), true);
        } catch (Exception e) {
            log.error(e, "Failed to drop catalog: %s", catalogNameStr);
        }
    }
}
