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

import com.google.common.hash.Hashing;
import io.airlift.concurrent.Threads;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.metadata.Catalog;
import io.trino.metadata.CatalogManager;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.catalog.CatalogStore;
import io.trino.spi.connector.CatalogVersion;
import io.trino.spi.connector.ConnectorName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestFilePollingCatalogStore
{
    @Test
    void testFileChangeTriggersRefresh(@TempDir Path catalogDirectory)
            throws IOException, InterruptedException
    {
        Path catalogFile = catalogDirectory.resolve("my_catalog.properties");
        Files.writeString(catalogFile, "connector.name=memory");

        TestCatalogManager manager = new TestCatalogManager();
        try (FilePollingCatalogStore store = new FilePollingCatalogStore(catalogDirectory, "100ms")) {
            manager.setCatalogStore(store);
            store.setCatalogManager(manager);
            manager.loadInitialCatalogs();

            // Verify initial state - my_catalog should be loaded
            assertThat(manager.getCatalogNames()).containsExactly(new CatalogName("my_catalog"));
            assertThat(manager.getCatalog(new CatalogName("my_catalog"))).isPresent();
            assertThat(manager.getCatalog(new CatalogName("my_catalog")).get().getConnectorName())
                    .isEqualTo(new ConnectorName("memory"));

            // Wait for initial poll - should not trigger a refresh since nothing changed
            Thread.sleep(150);
            assertThat(manager.getRefreshCount()).isEqualTo(0);

            // Test: Modify a file - external change triggers refresh
            Files.writeString(catalogFile, "connector.name=elasticsearch\nnew.prop=true");
            waitForRefreshCount(manager, 1);
            assertThat(manager.getRefreshCount()).isEqualTo(1);
            // Assert on activeCatalogs - should contain the updated catalog
            assertThat(manager.getCatalogNames()).containsExactly(new CatalogName("my_catalog"));
            assertThat(manager.getCatalog(new CatalogName("my_catalog"))).isPresent();
            assertThat(manager.getCatalog(new CatalogName("my_catalog")).get().getConnectorName())
                    .isEqualTo(new ConnectorName("elasticsearch"));

            // Test: Add a new file - external addition triggers refresh
            Path newCatalogFile = catalogDirectory.resolve("new_catalog.properties");
            Files.writeString(newCatalogFile, "connector.name=tpch");
            waitForRefreshCount(manager, 2);
            assertThat(manager.getRefreshCount()).isEqualTo(2);
            // Assert on activeCatalogs - should contain both catalogs
            assertThat(manager.getCatalogNames()).containsExactlyInAnyOrder(
                    new CatalogName("my_catalog"),
                    new CatalogName("new_catalog"));
            assertThat(manager.getCatalog(new CatalogName("my_catalog")).get().getConnectorName())
                    .isEqualTo(new ConnectorName("elasticsearch"));
            assertThat(manager.getCatalog(new CatalogName("new_catalog")).get().getConnectorName())
                    .isEqualTo(new ConnectorName("tpch"));

            // Test: Delete a file - external deletion triggers refresh
            Files.delete(catalogFile);
            waitForRefreshCount(manager, 3);
            assertThat(manager.getRefreshCount()).isEqualTo(3);
            // Assert on activeCatalogs - my_catalog should be removed, only new_catalog remains
            assertThat(manager.getCatalogNames()).containsExactly(new CatalogName("new_catalog"));
            assertThat(manager.getCatalog(new CatalogName("my_catalog"))).isEmpty();
            assertThat(manager.getCatalog(new CatalogName("new_catalog"))).isPresent();
            assertThat(manager.getCatalog(new CatalogName("new_catalog")).get().getConnectorName())
                    .isEqualTo(new ConnectorName("tpch"));
        }
    }

    private static void waitForRefreshCount(TestCatalogManager manager, int expectedCount)
            throws InterruptedException
    {
        long endTime = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < endTime) {
            if (manager.getRefreshCount() >= expectedCount) {
                return;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("Expected refresh count " + expectedCount + " but got " + manager.getRefreshCount());
    }

    private static class FilePollingCatalogStore
            implements CatalogStore, Closeable
    {
        private static final Logger log = Logger.get(FilePollingCatalogStore.class);
        private final Path directory;
        private final ScheduledExecutorService executor;
        private final Duration pollInterval;
        private final AtomicLong lastHash = new AtomicLong(0);

        private volatile CatalogManager catalogManager;

        public FilePollingCatalogStore(Path directory, String pollInterval)
        {
            this.directory = directory;
            this.pollInterval = Duration.valueOf(pollInterval);
            this.executor = Executors.newSingleThreadScheduledExecutor(Threads.daemonThreadsNamed("file-poller"));
        }

        @Override
        public void setCatalogManager(Object catalogManager)
        {
            if (!(catalogManager instanceof CatalogManager cm)) {
                throw new IllegalArgumentException("catalogManager must be an instance of CatalogManager");
            }
            this.catalogManager = cm;
            startPolling();
        }

        private void startPolling()
        {
            executor.scheduleWithFixedDelay(() -> {
                try {
                    pollForChanges();
                }
                catch (Throwable e) {
                    log.error(e, "Error during polling");
                }
            }, pollInterval.toMillis(), pollInterval.toMillis(), TimeUnit.MILLISECONDS);
        }

        private void pollForChanges()
        {
            long currentHash = computeStateHash();
            if (lastHash.get() != 0 && currentHash != lastHash.get()) {
                log.info("Detected change in catalog directory. Triggering refresh.");
                catalogManager.refreshCatalogsFromStore();
            }
            lastHash.set(currentHash);
        }

        private long computeStateHash()
        {
            try (Stream<Path> files = Files.list(directory)) {
                var hasher = Hashing.murmur3_128().newHasher();
                files.filter(path -> path.toString().endsWith(".properties"))
                        .sorted()
                        .forEach(path -> {
                            try {
                                hasher.putString(path.getFileName().toString(), UTF_8);
                                hasher.putBytes(Files.readAllBytes(path));
                            }
                            catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                        });
                return hasher.hash().asLong();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public Collection<StoredCatalog> getCatalogs()
        {
            try (Stream<Path> files = Files.list(directory)) {
                return files.filter(path -> path.toString().endsWith(".properties"))
                        .map(path -> {
                            String catalogName = path.getFileName().toString().replace(".properties", "");
                            return new StoredCatalog()
                            {
                                @Override
                                public CatalogName name()
                                {
                                    return new CatalogName(catalogName);
                                }

                                @Override
                                public CatalogProperties loadProperties()
                                {
                                    try {
                                        Properties props = new Properties();
                                        props.load(Files.newBufferedReader(path));
                                        Map<String, String> properties = new ConcurrentHashMap<>();
                                        props.forEach((key, value) -> properties.put(key.toString(), value.toString()));

                                        String connectorName = properties.get("connector.name");
                                        if (connectorName == null) {
                                            throw new IllegalStateException("connector.name is required");
                                        }
                                        return createCatalogProperties(
                                                new CatalogName(catalogName),
                                                new ConnectorName(connectorName),
                                                properties);
                                    }
                                    catch (IOException e) {
                                        throw new UncheckedIOException(e);
                                    }
                                }
                            };
                        })
                        .collect(java.util.stream.Collectors.toList());
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public CatalogProperties createCatalogProperties(CatalogName catalogName, ConnectorName connectorName, Map<String, String> properties)
        {
            return new CatalogProperties(catalogName, new CatalogVersion("1"), connectorName, properties);
        }

        @Override
        public void addOrReplaceCatalog(CatalogProperties catalogProperties)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void removeCatalog(CatalogName catalogName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close()
        {
            executor.shutdownNow();
        }
    }

    private static class TestCatalogManager
            implements CatalogManager
    {
        private final AtomicInteger refreshCount = new AtomicInteger(0);
        private volatile CatalogStore catalogStore;

        /**
         * Track active catalog properties instead of full Catalog objects.
         * This is simpler and more focused on what we're actually testing.
         */
        private final ConcurrentMap<CatalogName, CatalogProperties> activeCatalogs = new ConcurrentHashMap<>();

        public void setCatalogStore(CatalogStore catalogStore)
        {
            this.catalogStore = catalogStore;
        }

        @Override
        public Set<CatalogName> getCatalogNames()
        {
            return activeCatalogs.keySet();
        }

        @Override
        public Optional<Catalog> getCatalog(CatalogName catalogName)
        {
            // For testing, return a failed catalog if the name is tracked
            CatalogProperties props = activeCatalogs.get(catalogName);
            if (props == null) {
                return Optional.empty();
            }
            return Optional.of(Catalog.failedCatalog(props.name(), props.version(), props.connectorName()));
        }

        @Override
        public Optional<CatalogProperties> getCatalogProperties(CatalogHandle catalogHandle)
        {
            return Optional.empty();
        }

        @Override
        public Set<CatalogHandle> getActiveCatalogs()
        {
            return activeCatalogs.values().stream()
                    .map(props -> CatalogHandle.createRootCatalogHandle(props.name(), props.version()))
                    .collect(java.util.stream.Collectors.toSet());
        }

        @Override
        public void createCatalog(CatalogName catalogName, ConnectorName connectorName, Map<String, String> properties, boolean notExists)
        {
        }

        @Override
        public void dropCatalog(CatalogName catalogName, boolean exists)
        {
        }

        @Override
        public void refreshCatalogsFromStore()
        {
            if (catalogStore != null) {
                // Build snapshot of what should exist
                Set<CatalogName> desiredNames = new java.util.HashSet<>();

                for (CatalogStore.StoredCatalog storedCatalog : catalogStore.getCatalogs()) {
                    CatalogProperties props = storedCatalog.loadProperties();
                    CatalogName catalogName = storedCatalog.name();
                    desiredNames.add(catalogName);

                    // Store the catalog properties - always replace to catch property changes
                    activeCatalogs.put(catalogName, props);
                }

                // Remove catalogs that should no longer exist
                Set<CatalogName> currentNames = new java.util.HashSet<>(activeCatalogs.keySet());
                for (CatalogName catalogName : currentNames) {
                    if (!desiredNames.contains(catalogName)) {
                        activeCatalogs.remove(catalogName);
                    }
                }
            }
            refreshCount.incrementAndGet();
        }

        public void loadInitialCatalogs()
        {
            // Ensure the store is set
            if (catalogStore == null) {
                return;
            }

            // Clear any previous state for a clean test
            activeCatalogs.clear();

            // Get all catalogs from the store, just like the real manager does on startup
            for (CatalogStore.StoredCatalog storedCatalog : catalogStore.getCatalogs()) {
                CatalogProperties props = storedCatalog.loadProperties();
                CatalogName catalogName = storedCatalog.name();

                // Store the catalog properties
                activeCatalogs.put(catalogName, props);
            }
        }

        public int getRefreshCount()
        {
            return refreshCount.get();
        }
    }
}
