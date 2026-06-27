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
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.catalog.CatalogStore;
import io.trino.spi.catalog.CatalogStoreFactory;
import io.trino.spi.connector.CatalogVersion;
import io.trino.spi.connector.ConnectorName;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Collection;
import java.util.Map;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestBrokenCatalogQueries
{
    private static final String BROKEN_CATALOG = "broken";
    private static final String HEALTHY_CATALOG = "tpch";

    private QueryRunner queryRunner;

    @BeforeAll
    public void setUp()
    {
        // The broken catalog is present in the catalog store at startup, but its connector cannot be loaded
        // (it has invalid properties), so loadInitialCatalogs marks it as FAILING.
        queryRunner = new StandaloneQueryRunner(
                TEST_SESSION,
                builder -> builder
                        .setAdditionalModule(new BrokenCatalogStoreModule())
                        .addProperty("catalog.store", "broken_at_startup"));
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog(HEALTHY_CATALOG, "tpch", Map.of());
    }

    @AfterAll
    public void tearDown()
    {
        if (queryRunner != null) {
            queryRunner.close();
            queryRunner = null;
        }
    }

    @Test
    public void testBrokenCatalogStaysVisible()
    {
        assertThat(queryRunner.execute("SELECT state FROM system.metadata.catalogs WHERE catalog_name = '%s'".formatted(BROKEN_CATALOG)).getOnlyValue())
                .isEqualTo("FAILING");
        assertThat(queryRunner.execute("SELECT count(*) FROM system.jdbc.catalogs WHERE table_cat = '%s'".formatted(BROKEN_CATALOG)).getOnlyValue())
                .isEqualTo(1L);
    }

    @Test
    public void testShowCatalogsIncludesBrokenCatalog()
    {
        assertThat(queryRunner.execute("SHOW CATALOGS").getMaterializedRows())
                .extracting(row -> row.getField(0))
                .contains(HEALTHY_CATALOG, BROKEN_CATALOG);
    }

    @Test
    public void testCatalogEnumeratingQueriesIgnoreBrokenCatalog()
    {
        // Broken catalog contributes no metadata; healthy catalog still does.
        assertThat(queryRunner.execute("SELECT count(*) FROM system.jdbc.tables WHERE table_cat = '%s'".formatted(BROKEN_CATALOG)).getOnlyValue())
                .isEqualTo(0L);
        assertThat((long) queryRunner.execute("SELECT count(*) FROM system.jdbc.tables WHERE table_cat = '%s'".formatted(HEALTHY_CATALOG)).getOnlyValue())
                .isGreaterThan(0L);

        assertThat(queryRunner.execute("SELECT count(*) FROM system.jdbc.schemas WHERE table_catalog = '%s'".formatted(BROKEN_CATALOG)).getOnlyValue())
                .isEqualTo(0L);
        assertThat((long) queryRunner.execute("SELECT count(*) FROM system.jdbc.schemas WHERE table_catalog = '%s'".formatted(HEALTHY_CATALOG)).getOnlyValue())
                .isGreaterThan(0L);

        assertThat(queryRunner.execute("SELECT count(*) FROM system.jdbc.columns WHERE table_cat = '%s'".formatted(BROKEN_CATALOG)).getOnlyValue())
                .isEqualTo(0L);
        assertThat((long) queryRunner.execute("SELECT count(*) FROM system.jdbc.columns WHERE table_schem = 'tiny'").getOnlyValue())
                .isGreaterThan(0L);

        assertThat(queryRunner.execute("SELECT count(*) FROM system.metadata.table_comments WHERE catalog_name = '%s'".formatted(BROKEN_CATALOG)).getOnlyValue())
                .isEqualTo(0L);

        assertThat(queryRunner.execute("SELECT count(*) FROM system.metadata.materialized_views WHERE catalog_name = '%s'".formatted(BROKEN_CATALOG)).getOnlyValue())
                .isEqualTo(0L);

        // Not catalog-scoped; just verify no exception.
        assertThatCode(() -> {
            queryRunner.execute("SHOW SESSION");
            queryRunner.execute("SHOW FUNCTIONS");
            queryRunner.execute("SELECT * FROM system.metadata.materialized_view_properties");
            queryRunner.execute("SELECT * FROM system.metadata.table_properties");
        }).doesNotThrowAnyException();
    }

    private static class BrokenCatalogStoreModule
            implements Module
    {
        @Override
        public void configure(Binder binder) {}

        @Provides
        @Singleton
        public CatalogStoreFactory createCatalogStoreFactory(CatalogStoreManager catalogStoreManager)
        {
            CatalogStoreFactory factory = new BrokenCatalogStoreFactory();
            catalogStoreManager.addCatalogStoreFactory(factory);
            return factory;
        }
    }

    private static class BrokenCatalogStoreFactory
            implements CatalogStoreFactory
    {
        @Override
        public String getName()
        {
            return "broken_at_startup";
        }

        @Override
        public CatalogStore create(Map<String, String> config)
        {
            return new BrokenCatalogStore();
        }
    }

    private static class BrokenCatalogStore
            implements CatalogStore
    {
        private static final CatalogProperties BROKEN_CATALOG_PROPERTIES = new CatalogProperties(
                new CatalogName(BROKEN_CATALOG),
                new CatalogVersion("broken"),
                new ConnectorName("tpch"),
                Map.of("invalid-property", "value"));

        private final CatalogStore delegate = new InMemoryCatalogStore();

        @Override
        public Collection<StoredCatalog> getCatalogs()
        {
            return ImmutableList.<StoredCatalog>builder()
                    .add(new StoredCatalog()
                    {
                        @Override
                        public CatalogName name()
                        {
                            return new CatalogName(BROKEN_CATALOG);
                        }

                        @Override
                        public CatalogProperties loadProperties()
                        {
                            return BROKEN_CATALOG_PROPERTIES;
                        }
                    })
                    .addAll(delegate.getCatalogs())
                    .build();
        }

        @Override
        public CatalogProperties createCatalogProperties(CatalogName catalogName, ConnectorName connectorName, Map<String, String> properties)
        {
            return delegate.createCatalogProperties(catalogName, connectorName, properties);
        }

        @Override
        public void addOrReplaceCatalog(CatalogProperties catalogProperties)
        {
            delegate.addOrReplaceCatalog(catalogProperties);
        }

        @Override
        public void removeCatalog(CatalogName catalogName)
        {
            delegate.removeCatalog(catalogName);
        }
    }
}
