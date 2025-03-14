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
package io.trino.plugin.jdbc;

import com.google.common.collect.ImmutableList;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.plugin.jdbc.H2QueryRunner.createH2QueryRunner;
import static io.trino.plugin.jdbc.TestingH2JdbcModule.createH2ConnectionUrl;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

// Single-threaded because H2 DDL operations can sometimes take a global lock, leading to apparent deadlocks
// like in https://github.com/trinodb/trino/issues/7209.
@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
final class TestDynamicCatalogsSql
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createH2QueryRunner(ImmutableList.of(), TestingH2JdbcModule.createProperties());
    }

    @Test
    void testCreateDropCatalog()
    {
        String catalog = "catalog_" + randomNameSuffix();
        assertThatThrownBy(() -> computeActual("CREATE CATALOG %s USING base_jdbc".formatted(catalog)))
                .hasMessageContaining("Invalid configuration property connection-url: must not be null");
        String connectionUrl = createH2ConnectionUrl();
        assertUpdate(
                """
                CREATE CATALOG %s USING base_jdbc
                WITH (
                   "connection-url" = '%s'
                )
                """.formatted(catalog, connectionUrl));
        assertUpdate("DROP CATALOG " + catalog);
    }

    @Test
    void testRenameCatalog()
    {
        String oldCatalog = "catalog_" + randomNameSuffix();
        String connectionUrl = createH2ConnectionUrl();
        assertUpdate(
                """
                CREATE CATALOG %s USING base_jdbc
                WITH (
                   "connection-url" = '%s'
                )
                """.formatted(oldCatalog, connectionUrl));

        String catalog = "catalog_" + randomNameSuffix();
        assertUpdate("ALTER CATALOG %s RENAME TO %s".formatted(oldCatalog, catalog));
        assertThatThrownBy(() -> computeActual("DROP CATALOG " + oldCatalog))
                .hasMessage("Catalog '%s' not found".formatted(oldCatalog));
        assertThat((String) computeActual("SHOW CATALOGS LIKE '%s'".formatted(catalog)).getOnlyValue())
                .isEqualTo(catalog);

        assertUpdate("DROP CATALOG " + catalog);
    }

    @Test
    void testRenameCatalogWithNonLowerCaseName()
    {
        String oldCatalog = "catalog_" + randomNameSuffix();
        String connectionUrl = createH2ConnectionUrl();
        assertUpdate(
                """
                CREATE CATALOG %s USING base_jdbc
                WITH (
                   "connection-url" = '%s'
                )
                """.formatted(oldCatalog, connectionUrl));

        String catalog = "CATALOG_" + randomNameSuffix();
        assertUpdate("ALTER CATALOG %s RENAME TO %s".formatted(oldCatalog, catalog));
        assertThatThrownBy(() -> computeActual("DROP CATALOG " + oldCatalog))
                .hasMessage("Catalog '%s' not found".formatted(oldCatalog));
        assertThat((String) computeActual("SHOW CATALOGS LIKE '%s'".formatted(catalog.toLowerCase(ENGLISH))).getOnlyValue())
                .isEqualTo(catalog.toLowerCase(ENGLISH));

        assertUpdate("DROP CATALOG " + catalog);
    }

    @Test
    void testSetCatalogProperties()
    {
        String catalog = "catalog_" + randomNameSuffix();
        String connectionUrl = createH2ConnectionUrl();
        assertUpdate(
                """
                CREATE CATALOG %s USING base_jdbc
                WITH (
                   "bootstrap.quiet" = 'true',
                   "connection-url" = '%s'
                )
                """.formatted(catalog, connectionUrl));

        String newJdbcUrl = createH2ConnectionUrl();
        assertUpdate(
                """
                ALTER CATALOG %s SET PROPERTIES
                   "connection-url" = '%s'
                """.formatted(catalog, newJdbcUrl));

        assertUpdate("DROP CATALOG " + catalog);
    }

    @Test
    void testSetCatalogPropertiesWithNonLowerCaseName()
    {
        String catalog = "CATALOG_" + randomNameSuffix();
        String connectionUrl = createH2ConnectionUrl();
        assertUpdate(
                """
                CREATE CATALOG %s USING base_jdbc
                WITH (
                   "bootstrap.quiet" = 'true',
                   "connection-url" = '%s'
                )
                """.formatted(catalog, connectionUrl));

        String newJdbcUrl = createH2ConnectionUrl();
        assertUpdate(
                """
                ALTER CATALOG %s SET PROPERTIES
                   "connection-url" = '%s'
                """.formatted(catalog, newJdbcUrl));

        assertUpdate("DROP CATALOG " + catalog);
    }

    @Test
    void testSetCatalogPropertiesWithFunctionCall()
    {
        String catalog = "CATALOG_" + randomNameSuffix();
        String connectionUrl = createH2ConnectionUrl();
        assertUpdate(
                """
                CREATE CATALOG %s USING base_jdbc
                WITH (
                   "bootstrap.quiet" = 'true',
                   "connection-url" = ltrim('         %s')
                )
                """.formatted(catalog, connectionUrl));

        String newJdbcUrl = createH2ConnectionUrl();
        assertUpdate(
                """
                ALTER CATALOG %s SET PROPERTIES
                   "connection-url" = '%s'
                """.formatted(catalog, newJdbcUrl));

        assertUpdate("DROP CATALOG " + catalog);
    }
}
