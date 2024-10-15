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
import io.airlift.configuration.secrets.SecretsResolver;
import io.airlift.testing.TempFile;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.catalog.CatalogStore;
import io.trino.spi.catalog.CatalogStoreFactory;
import io.trino.spi.connector.ConnectorName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class TestCatalogStoreManager
{
    @Test
    void testCatalogStoreIsLoaded()
            throws IOException
    {
        try (TempFile tempFile = new TempFile()) {
            Files.writeString(tempFile.path(), "some-property=some-value");
            CatalogStoreConfig catalogStoreConfig = new CatalogStoreConfig().setCatalogStoreKind("test");
            CatalogStoreManager catalogStoreManager = new CatalogStoreManager(new SecretsResolver(ImmutableMap.of()), catalogStoreConfig);
            catalogStoreManager.addCatalogStoreFactory(new TestingCatalogStoreFactory());
            catalogStoreManager.loadConfiguredCatalogStore(catalogStoreConfig.getCatalogStoreKind(), tempFile.file());
            assertThat(catalogStoreManager.getCatalogs()).containsExactly(TestingCatalogStore.STORED_CATALOG);
        }
    }

    @Test
    void testCatalogStoreIsLoadedWithoutConfiguration()
            throws IOException
    {
        CatalogStoreManager catalogStoreManager = new CatalogStoreManager(new SecretsResolver(ImmutableMap.of()), new CatalogStoreConfig().setCatalogStoreKind("test"));
        catalogStoreManager.addCatalogStoreFactory(new TestingCatalogStoreFactory());
        catalogStoreManager.loadConfiguredCatalogStore();
        assertThat(catalogStoreManager.getCatalogs()).containsExactly(TestingCatalogStore.STORED_CATALOG);
    }

    private static class TestingCatalogStoreFactory
            implements CatalogStoreFactory
    {
        @Override
        public String getName()
        {
            return "test";
        }

        @Override
        public CatalogStore create(Map<String, String> config)
        {
            return new TestingCatalogStore();
        }
    }

    private static final class TestingCatalogStore
            implements CatalogStore
    {
        private static final CatalogStore.StoredCatalog STORED_CATALOG = new CatalogStore.StoredCatalog()
        {
            @Override
            public CatalogName name()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public CatalogProperties loadProperties()
            {
                throw new UnsupportedOperationException();
            }
        };

        @Override
        public Collection<StoredCatalog> getCatalogs()
        {
            return ImmutableList.of(STORED_CATALOG);
        }

        @Override
        public CatalogProperties createCatalogProperties(CatalogName catalogName, ConnectorName connectorName, Map<String, String> properties)
        {
            throw new UnsupportedOperationException();
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
    }
}
