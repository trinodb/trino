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

import javax.inject.Inject;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.Files.getNameWithoutExtension;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static io.trino.spi.StandardErrorCode.CATALOG_STORE_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.CatalogHandle.createRootCatalogHandle;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.Objects.requireNonNull;

public final class FileCatalogStore
        implements CatalogStore
{
    private static final Logger log = Logger.get(FileCatalogStore.class);

    private final boolean readOnly;
    private final File catalogsDirectory;
    private final ConcurrentMap<String, StoredCatalog> catalogs = new ConcurrentHashMap<>();

    @Inject
    public FileCatalogStore(FileCatalogStoreConfig config)
    {
        requireNonNull(config, "config is null");
        readOnly = config.isReadOnly();
        catalogsDirectory = config.getCatalogConfigurationDir();
        List<String> disabledCatalogs = firstNonNull(config.getDisabledCatalogs(), ImmutableList.of());

        for (File file : listCatalogFiles(catalogsDirectory)) {
            String catalogName = getNameWithoutExtension(file.getName());
            checkArgument(!catalogName.equals(GlobalSystemConnector.NAME), "Catalog name SYSTEM is reserved for internal usage");
            if (disabledCatalogs.contains(catalogName)) {
                log.info("Skipping disabled catalog %s", catalogName);
                continue;
            }
            catalogs.put(catalogName, new FileStoredCatalog(catalogName, file));
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
        File file = toFile(catalogName);
        Properties properties = new Properties();
        properties.setProperty("connector.name", catalogProperties.getConnectorName().toString());
        properties.putAll(catalogProperties.getProperties());

        try {
            File temporary = new File(file.getPath() + ".tmp");
            try (FileOutputStream out = new FileOutputStream(temporary)) {
                properties.store(out, null);
                out.flush();
                out.getFD().sync();
            }
            Files.move(temporary.toPath(), file.toPath(), REPLACE_EXISTING);
        }
        catch (IOException e) {
            log.error(e, "Could not store catalog properties for %s", catalogName);
            // don't expose exception to end user
            throw new TrinoException(CATALOG_STORE_ERROR, "Could not store catalog properties");
        }
        catalogs.put(catalogName, new FileStoredCatalog(catalogName, file));
    }

    @Override
    public void removeCatalog(String catalogName)
    {
        checkModifiable();
        catalogs.remove(catalogName);
        toFile(catalogName).delete();
    }

    private void checkModifiable()
    {
        if (readOnly) {
            throw new TrinoException(NOT_SUPPORTED, "Catalog store is read only");
        }
    }

    private File toFile(String catalogName)
    {
        return new File(catalogsDirectory, catalogName + ".properties");
    }

    private static List<File> listCatalogFiles(File catalogsDirectory)
    {
        if (catalogsDirectory == null || !catalogsDirectory.isDirectory()) {
            return ImmutableList.of();
        }

        File[] files = catalogsDirectory.listFiles();
        if (files == null) {
            return ImmutableList.of();
        }
        return Arrays.stream(files)
                .filter(File::isFile)
                .filter(file -> file.getName().endsWith(".properties"))
                .collect(toImmutableList());
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

    private static class FileStoredCatalog
            implements StoredCatalog
    {
        private final String name;
        private final File file;

        public FileStoredCatalog(String name, File file)
        {
            this.name = requireNonNull(name, "name is null");
            this.file = requireNonNull(file, "file is null");
        }

        @Override
        public String getName()
        {
            return name;
        }

        @Override
        public CatalogProperties loadProperties()
        {
            Map<String, String> properties;
            try {
                properties = new HashMap<>(loadPropertiesFrom(file.getPath()));
            }
            catch (IOException e) {
                throw new UncheckedIOException("Error reading catalog property file " + file, e);
            }

            String connectorNameValue = properties.remove("connector.name");
            checkState(connectorNameValue != null, "Catalog configuration %s does not contain 'connector.name'", file.getAbsoluteFile());
            if (connectorNameValue.indexOf('-') >= 0) {
                String deprecatedConnectorName = connectorNameValue;
                connectorNameValue = connectorNameValue.replace('-', '_');
                log.warn("Catalog '%s' is using the deprecated connector name '%s'. The correct connector name is '%s'", name, deprecatedConnectorName, connectorNameValue);
            }
            ConnectorName connectorName = new ConnectorName(connectorNameValue);

            CatalogHandle catalogHandle = createRootCatalogHandle(name, computeCatalogVersion(name, connectorName, properties));
            return new CatalogProperties(catalogHandle, connectorName, ImmutableMap.copyOf(properties));
        }
    }
}
