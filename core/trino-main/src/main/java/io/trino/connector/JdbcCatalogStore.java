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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.catalog.CatalogStore;
import io.trino.spi.catalog.StoredCatalog;
import io.trino.spi.connector.ConnectorName;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import static io.trino.connector.FileCatalogStore.computeCatalogVersion;
import static io.trino.spi.StandardErrorCode.CATALOG_STORE_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public final class JdbcCatalogStore
        implements CatalogStore {
    private static final Logger log = Logger.get(JdbcCatalogStore.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();
    private static final TypeReference<Map<String, String>> MAP_TYPE_REFERENCE = new TypeReference<>() {
    };

    private final JdbcCatalogStoreDao dao;
    private final boolean readOnly;

    @Inject
    public JdbcCatalogStore(JdbcCatalogStoreDao dao, JdbcCatalogStoreConfig config) {
        this.dao = requireNonNull(dao, "dao is null");
        this.readOnly = requireNonNull(config, "config is null").isReadOnly();
    }

    // New method for polling
    public Map<String, Long> getCatalogVersions() {
        return dao.getCatalogVersions();
    }
    
    // New method to fetch single catalog properties
    public Optional<CatalogProperties> getCatalogProperties(String catalogName) {
        return dao.getCatalog(catalogName).map(record -> 
             new CatalogProperties(
                new CatalogName(record.catalogName()),
                computeCatalogVersion(new CatalogName(record.catalogName()), new ConnectorName(record.connectorName()), deserializeProperties(record.properties())),
                new ConnectorName(record.connectorName()),
                deserializeProperties(record.properties())));
    }

    @Override
    public Collection<StoredCatalog> getCatalogs() {
        return dao.getCatalogs().stream()
                .map(record -> (StoredCatalog) new JdbcStoredCatalog(
                        new CatalogName(record.catalogName()),
                        new ConnectorName(record.connectorName()),
                        deserializeProperties(record.properties())))
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public CatalogProperties createCatalogProperties(CatalogName catalogName, ConnectorName connectorName,
            Map<String, String> properties) {
        checkModifiable();
        return new CatalogProperties(
                catalogName,
                computeCatalogVersion(catalogName, connectorName, properties),
                connectorName,
                ImmutableMap.copyOf(properties));
    }

    @Override
    public void addOrReplaceCatalog(CatalogProperties catalogProperties) {
        checkModifiable();
        CatalogName catalogName = catalogProperties.name();
        String catalogNameStr = catalogName.toString();
        String connectorNameStr = catalogProperties.connectorName().toString();
        String propertiesJson = serializeProperties(catalogProperties.properties());
        // Simple versioning: system current time as crude version or just currentTimeMillis
        // In a real system, we might want monotonic increasing ID from DB sequence
        long version = System.currentTimeMillis(); 

        try {
            // Use delete + insert pattern for upsert to avoid mapping complexities
            // Ideally should be a single transaction
            dao.deleteCatalog(catalogNameStr);
            dao.insertCatalog(catalogNameStr, connectorNameStr, propertiesJson, version);
            log.info("Stored catalog %s (v%d)", catalogName, version);
        } catch (RuntimeException e) {
            log.error(e, "Could not store catalog properties for %s", catalogName);
            throw new TrinoException(CATALOG_STORE_ERROR, "Could not store catalog properties", e);
        }
    }

    @Override
    public void removeCatalog(CatalogName catalogName) {
        checkModifiable();
        try {
            dao.deleteCatalog(catalogName.toString());
            log.info("Removed catalog %s", catalogName);
        } catch (RuntimeException e) {
            log.warn(e, "Could not remove catalog properties for %s", catalogName);
        }
    }

    private void checkModifiable() {
        if (readOnly) {
            throw new TrinoException(NOT_SUPPORTED, "Catalog store is read only");
        }
    }

    private static String serializeProperties(Map<String, String> properties) {
        try {
            return OBJECT_MAPPER.writeValueAsString(properties);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException("Failed to serialize catalog properties", e);
        }
    }

    private static Map<String, String> deserializeProperties(String json) {
        try {
            return OBJECT_MAPPER.readValue(json, MAP_TYPE_REFERENCE);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException("Failed to deserialize catalog properties", e);
        }
    }

    private record JdbcStoredCatalog(CatalogName catalogName, ConnectorName connectorName,
            Map<String, String> properties)
            implements StoredCatalog {
        private JdbcStoredCatalog(CatalogName catalogName, ConnectorName connectorName,
                Map<String, String> properties) {
            this.catalogName = requireNonNull(catalogName, "catalogName is null");
            this.connectorName = requireNonNull(connectorName, "connectorName is null");
            this.properties = ImmutableMap.copyOf(requireNonNull(properties, "properties is null"));
        }

        @Override
        public CatalogName name() {
            return catalogName;
        }

        @Override
        public CatalogProperties loadProperties() {
            return new CatalogProperties(
                    catalogName,
                    computeCatalogVersion(catalogName, connectorName, properties),
                    connectorName,
                    properties);
        }
    }
}
