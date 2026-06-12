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
package io.trino.plugin.iceberg.catalog.rest;

import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.jdbc.JdbcCatalog;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public final class RestCatalogTestUtils
{
    private RestCatalogTestUtils() {}

    public static Catalog backendCatalog(Path warehouseLocation)
            throws IOException
    {
        JdbcCatalog catalog = new JdbcCatalog();
        catalog.initialize("backend_jdbc", catalogProperties(warehouseLocation));

        return catalog;
    }

    /**
     * Creates a backend catalog backed by a file:// URI warehouse whose namespace metadata
     * never includes a {@code location} property (such REST catalogs are technically allowed).
     */
    public static JdbcCatalog backendCatalogWithoutNamespaceLocation(Path warehouseLocation)
            throws IOException
    {
        NoLocationJdbcCatalog catalog = new NoLocationJdbcCatalog();
        catalog.initialize("backend_jdbc", catalogProperties(warehouseLocation));

        return catalog;
    }

    private static ImmutableMap<String, String> catalogProperties(Path warehouseLocation)
            throws IOException
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        properties.put(CatalogProperties.URI, "jdbc:h2:file:" + Files.createTempFile(null, null).toAbsolutePath());
        properties.put(JdbcCatalog.PROPERTY_PREFIX + "username", "user");
        properties.put(JdbcCatalog.PROPERTY_PREFIX + "password", "password");
        properties.put(JdbcCatalog.PROPERTY_PREFIX + "schema-version", "V1");
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation.resolve("iceberg_data").toFile().getAbsolutePath());

        return properties.buildOrThrow();
    }

    /**
     * A JdbcCatalog subclass that omits the {@code location} property from namespace metadata.
     * JdbcCatalog 1.10+ always synthesises a warehouse-relative {@code location} even for
     * namespaces created without one; this subclass strips it, reproducing the behaviour of
     * REST catalog servers that do not assign namespace locations.
     */
    public static class NoLocationJdbcCatalog
            extends JdbcCatalog
    {
        @Override
        public Map<String, String> loadNamespaceMetadata(Namespace namespace)
        {
            Map<String, String> metadata = new HashMap<>(super.loadNamespaceMetadata(namespace));
            metadata.remove("location");
            return ImmutableMap.copyOf(metadata);
        }
    }
}
