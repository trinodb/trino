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
import com.google.common.io.Files;
import io.airlift.log.Logger;
import io.trino.connector.system.GlobalSystemConnector;
import io.trino.spi.connector.CatalogHandle;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static io.trino.connector.CoordinatorDynamicCatalogManager.computeCatalogVersion;
import static io.trino.spi.connector.CatalogHandle.createRootCatalogHandle;
import static java.util.Objects.requireNonNull;

public class FileCatalogStore
        implements CatalogStore
{
    private static final Logger log = Logger.get(FileCatalogStore.class);

    private final List<StoredCatalog> catalogs;

    @Inject
    public FileCatalogStore(StaticCatalogManagerConfig config)
    {
        List<String> disabledCatalogs = firstNonNull(config.getDisabledCatalogs(), ImmutableList.of());

        ImmutableList.Builder<StoredCatalog> storedCatalogs = ImmutableList.builder();
        for (File file : listCatalogFiles(config.getCatalogConfigurationDir())) {
            String catalogName = Files.getNameWithoutExtension(file.getName());
            checkArgument(!catalogName.equals(GlobalSystemConnector.NAME), "Catalog name SYSTEM is reserved for internal usage");
            if (disabledCatalogs.contains(catalogName)) {
                log.info("Skipping disabled catalog %s", catalogName);
                continue;
            }
            storedCatalogs.add(new FileStoredCatalog(catalogName, file));
        }
        this.catalogs = storedCatalogs.build();
    }

    @Override
    public Collection<StoredCatalog> getCatalogs()
    {
        return catalogs;
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
