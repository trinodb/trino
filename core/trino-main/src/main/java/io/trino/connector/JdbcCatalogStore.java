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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import io.airlift.log.Logger;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.CatalogHandle.CatalogVersion;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.mapper.reflect.ColumnName;

import javax.inject.Inject;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Maps.fromProperties;
import static io.trino.spi.StandardErrorCode.CATALOG_STORE_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.CatalogHandle.createRootCatalogHandle;
import static java.util.Objects.requireNonNull;

public final class JdbcCatalogStore
        implements CatalogStore
{
    private static final Logger log = Logger.get(JdbcCatalogStore.class);

    private final boolean readOnly;
    private final Jdbi catalogsJdbi;

    private final ConcurrentMap<String, StoredCatalog> catalogs = new ConcurrentHashMap<>();

    @Inject
    public JdbcCatalogStore(JdbcCatalogStoreConfig config)
    {
        requireNonNull(config, "config is null");
        readOnly = config.isReadOnly();
        String catalogsUrl = config.getCatalogConfigDbUrl();
        String catalogsUser = config.getCatalogConfigDbUser();
        String catalogsPassword = config.getCatalogConfigDbPassword();

        catalogsJdbi = Jdbi.create(catalogsUrl, catalogsUser, catalogsPassword);

        List<String> disabledCatalogs = firstNonNull(config.getDisabledCatalogs(), ImmutableList.of());

        List<JdbcStoredCatalog> dbCatalogs = catalogsJdbi.withHandle(handle -> {
            handle.execute("CREATE TABLE IF NOT EXISTS catalogs (name VARCHAR(255), properties TEXT, PRIMARY KEY (name))");

            return handle.createQuery("SELECT name, properties FROM catalogs")
                    .mapToBean(JdbcStoredCatalog.class)
                    .list();
        });

        for (JdbcStoredCatalog catalog : dbCatalogs) {
            String catalogName = catalog.getName();
            checkArgument(!catalogName.equals(GlobalSystemConnector.NAME), "Catalog name SYSTEM is reserved for internal usage");

            if (disabledCatalogs.contains(catalogName)) {
                log.info("Skipping disabled catalog %s", catalogName);
                continue;
            }
            catalogs.put(catalog.getName(), catalog);
        }
    }

    @Override
    public Collection<StoredCatalog> getCatalogs()
    {
        return ImmutableList.copyOf(catalogs.values());
    }

    @Override
    public CatalogProperties createCatalogProperties(String catalogName, ConnectorName connectorName, Map<String, String> properties)
    {
        checkModifiable();
        return new CatalogProperties(
                createRootCatalogHandle(catalogName, computeCatalogVersion(catalogName, connectorName, properties)),
                connectorName,
                ImmutableMap.copyOf(properties));
    }

    @Override
    public void addOrReplaceCatalog(CatalogProperties catalogProperties)
    {
        checkModifiable();
        String catalogName = catalogProperties.getCatalogHandle().getCatalogName();

        Properties properties = new Properties();
        properties.setProperty("connector.name", catalogProperties.getConnectorName().toString());
        properties.putAll(catalogProperties.getProperties());

        StringWriter writer = new StringWriter();
        try {
            properties.store(writer, null);
        }
        catch (IOException e) {
            log.error(e, "Could not store catalog properties for %s", catalogName);
            // don't expose exception to end user
            throw new TrinoException(CATALOG_STORE_ERROR, "Could not store catalog properties");
        }

        String stringProperties = writer.getBuffer().toString();

        JdbcStoredCatalog jdbcCatalog = new JdbcStoredCatalog(catalogName, stringProperties);

        catalogsJdbi.withHandle(handle -> {
            handle.createUpdate("INSERT INTO catalogs (name,properties) VALUES (:name, :properties)")
                    .bind("name", catalogName)
                    .bind("properties", stringProperties)
                    .execute();
            return null;
        });

        catalogs.put(catalogName, jdbcCatalog);
    }

    @Override
    public void removeCatalog(String catalogName)
    {
        checkModifiable();

        catalogsJdbi.withHandle(handle -> {
            handle.createUpdate("DELETE FROM catalogs WHERE name = :name")
                    .bind("name", catalogName)
                    .execute();
            return null;
        });

        catalogs.remove(catalogName);
    }

    private void checkModifiable()
    {
        if (readOnly) {
            throw new TrinoException(NOT_SUPPORTED, "Catalog store is read only");
        }
    }

    /**
     * This is not a generic, universal, or stable version computation, and can and will change from version to version without warning.
     * For places that need a long term stable version, do not use this code.
     */
    static CatalogVersion computeCatalogVersion(String catalogName, ConnectorName connectorName, Map<String, String> properties)
    {
        Hasher hasher = Hashing.sha256().newHasher();
        hasher.putUnencodedChars("catalog-hash");
        hashLengthPrefixedString(hasher, catalogName);
        hashLengthPrefixedString(hasher, connectorName.toString());
        hasher.putInt(properties.size());
        ImmutableSortedMap.copyOf(properties).forEach((key, value) -> {
            hashLengthPrefixedString(hasher, key);
            hashLengthPrefixedString(hasher, value);
        });
        return new CatalogVersion(hasher.hash().toString());
    }

    private static void hashLengthPrefixedString(Hasher hasher, String value)
    {
        hasher.putInt(value.length());
        hasher.putUnencodedChars(value);
    }

    public static class JdbcStoredCatalog
            implements StoredCatalog
    {
        private String name;
        private String properties;

        public JdbcStoredCatalog() {}

        public JdbcStoredCatalog(String name, String properties)
        {
            this.name = name;
            this.properties = properties;
        }

        @ColumnName("properties")
        public String getProperties()
        {
            return properties;
        }

        public void setProperties(String properties)
        {
            this.properties = properties;
        }

        @ColumnName("name")
        @Override
        public String getName()
        {
            return name;
        }

        public void setName(String name)
        {
            this.name = name;
        }

        @Override
        public CatalogProperties loadProperties()
        {
            final Properties properties = new Properties();

            try {
                properties.load(new StringReader(this.properties));
            }
            catch (IOException e) {
                throw new UncheckedIOException("Error reading catalog properties " + this.name, e);
            }

            Map<String, String> props = new HashMap<>(
                    fromProperties(properties)
                            .entrySet().stream()
                            .collect(toImmutableMap(Entry::getKey, entry -> entry.getValue().trim())));

            String connectorNameValue = props.remove("connector.name");
            checkState(connectorNameValue != null, "Catalog configuration %s does not contain 'connector.name'", this.name);

            if (connectorNameValue.indexOf('-') >= 0) {
                String deprecatedConnectorName = connectorNameValue;
                connectorNameValue = connectorNameValue.replace('-', '_');
                log.warn("Catalog '%s' is using the deprecated connector name '%s'. The correct connector name is '%s'", name, deprecatedConnectorName, connectorNameValue);
            }

            ConnectorName connectorName = new ConnectorName(connectorNameValue);
            CatalogHandle catalogHandle = createRootCatalogHandle(name, computeCatalogVersion(name, connectorName, props));

            return new CatalogProperties(catalogHandle, connectorName, ImmutableMap.copyOf(props));
        }
    }
}
