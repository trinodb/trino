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
package io.trino.plugin.router;

import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRouterMetadata
{
    @Test
    public void testRedirectPrefixMatch()
    {
        RouterMetadata metadata = metadataWithPrefix("prod_", "catalog_prod");

        Optional<CatalogSchemaTableName> result = metadata.redirectTable(SESSION, new SchemaTableName("prod_sales", "orders"));

        assertThat(result).contains(new CatalogSchemaTableName("catalog_prod", "sales", "orders"));
    }

    @Test
    public void testRedirectNoMatch()
    {
        RouterMetadata metadata = metadataWithPrefix("prod_", "catalog_prod");

        Optional<CatalogSchemaTableName> result = metadata.redirectTable(SESSION, new SchemaTableName("unknown_schema", "t"));

        assertThat(result).isEmpty();
    }

    @Test
    public void testPrefixMatchWithOnlyPrefix()
    {
        RouterMetadata metadata = metadataWithPrefix("prod_", "catalog_prod");

        // Schema name IS the prefix with nothing after it — no valid target schema, skip rule
        Optional<CatalogSchemaTableName> result = metadata.redirectTable(SESSION, new SchemaTableName("prod_", "t"));

        assertThat(result).isEmpty();
    }

    @Test
    public void testSchemaExistsPrefixRule()
    {
        RouterMetadata metadata = metadataWithPrefix("prod_", "catalog_prod");

        assertThat(metadata.schemaExists(SESSION, "prod_sales")).isTrue();
        assertThat(metadata.schemaExists(SESSION, "prod_")).isFalse();
        assertThat(metadata.schemaExists(SESSION, "other_sales")).isFalse();
    }

    @Test
    public void testGetTableHandleAlwaysNull()
    {
        RouterMetadata metadata = metadataWithPrefix("prod_", "catalog_prod");

        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName("prod_sales", "t"), Optional.empty(), Optional.empty()))
                .isNull();
    }

    @Test
    public void testListSchemaNamesIncludesPrefixMappedGlueDatabases()
    {
        RouterMetadata metadata = new RouterMetadata(
                new RouterConfig().setSchemaPrefixRules(List.of("prod_=catalog_prod")),
                stubbedGlueClient(List.of("sales", "marketing")));

        List<String> schemas = metadata.listSchemaNames(SESSION);

        assertThat(schemas).containsExactlyInAnyOrder("prod_sales", "prod_marketing");
    }

    @Test
    public void testListSchemaNamesQueriesPerPrefixGlueCatalogId()
    {
        // Each prefix hits a different Glue catalog; the stub returns different databases per catalog ID.
        GlueClient glueClient = catalogAwareGlueClient(Map.of(
                "111122223333", List.of("sales", "marketing"),
                "444455556666", List.of("reports")));

        RouterMetadata metadata = new RouterMetadata(
                new RouterConfig()
                        .setSchemaPrefixRules(List.of("prod_=catalog_prod", "staging_=catalog_staging"))
                        .setSchemaPrefixGlueCatalogIds(List.of("prod_=111122223333", "staging_=444455556666")),
                glueClient);

        List<String> schemas = metadata.listSchemaNames(SESSION);

        assertThat(schemas).containsExactlyInAnyOrder("prod_sales", "prod_marketing", "staging_reports");
    }

    @Test
    public void testListSchemaNamesUsesDefaultGlueCatalogWhenIdAbsent()
    {
        // Prefix has no catalog ID configured — request must have no catalogId set (null).
        GlueClient glueClient = catalogAwareGlueClient(Map.of("", List.of("db1")));

        RouterMetadata metadata = new RouterMetadata(
                new RouterConfig().setSchemaPrefixRules(List.of("prod_=catalog_prod")),
                glueClient);

        List<String> schemas = metadata.listSchemaNames(SESSION);

        assertThat(schemas).containsExactlyInAnyOrder("prod_db1");
    }

    @Test
    public void testMappedSchemasFiltersListSchemaNames()
    {
        RouterMetadata metadata = new RouterMetadata(
                new RouterConfig()
                        .setSchemaPrefixRules(List.of("prod_=catalog_prod"))
                        .setSchemaPrefixMappedSchemas(List.of("prod_=sales.*")),
                stubbedGlueClient(List.of("sales", "sales_eu", "marketing")));

        List<String> schemas = metadata.listSchemaNames(SESSION);

        assertThat(schemas).containsExactlyInAnyOrder("prod_sales", "prod_sales_eu");
    }

    @Test
    public void testMappedSchemasFiltersRedirect()
    {
        RouterMetadata metadata = new RouterMetadata(
                new RouterConfig()
                        .setSchemaPrefixRules(List.of("prod_=catalog_prod"))
                        .setSchemaPrefixMappedSchemas(List.of("prod_=sales.*")),
                stubbedGlueClient(List.of()));

        assertThat(metadata.redirectTable(SESSION, new SchemaTableName("prod_sales", "t")))
                .contains(new CatalogSchemaTableName("catalog_prod", "sales", "t"));
        assertThat(metadata.redirectTable(SESSION, new SchemaTableName("prod_marketing", "t")))
                .isEmpty();
    }

    @Test
    public void testMappedSchemasFiltersSchemaExists()
    {
        RouterMetadata metadata = new RouterMetadata(
                new RouterConfig()
                        .setSchemaPrefixRules(List.of("prod_=catalog_prod"))
                        .setSchemaPrefixMappedSchemas(List.of("prod_=sales.*")),
                stubbedGlueClient(List.of()));

        assertThat(metadata.schemaExists(SESSION, "prod_sales")).isTrue();
        assertThat(metadata.schemaExists(SESSION, "prod_marketing")).isFalse();
    }

    @Test
    public void testMappedSchemasMultiplePatternsPerPrefix()
    {
        RouterMetadata metadata = new RouterMetadata(
                new RouterConfig()
                        .setSchemaPrefixRules(List.of("prod_=catalog_prod"))
                        .setSchemaPrefixMappedSchemas(List.of("prod_=sales", "prod_=marketing")),
                stubbedGlueClient(List.of()));

        assertThat(metadata.redirectTable(SESSION, new SchemaTableName("prod_sales", "t")))
                .contains(new CatalogSchemaTableName("catalog_prod", "sales", "t"));
        assertThat(metadata.redirectTable(SESSION, new SchemaTableName("prod_marketing", "t")))
                .contains(new CatalogSchemaTableName("catalog_prod", "marketing", "t"));
        assertThat(metadata.redirectTable(SESSION, new SchemaTableName("prod_other", "t")))
                .isEmpty();
    }

    @Test
    public void testMappedSchemasAbsentPrefixAllowsAll()
    {
        // prod_ has a filter; staging_ has none — all staging schemas should pass
        RouterMetadata metadata = new RouterMetadata(
                new RouterConfig()
                        .setSchemaPrefixRules(List.of("prod_=catalog_prod", "staging_=catalog_staging"))
                        .setSchemaPrefixMappedSchemas(List.of("prod_=sales")),
                stubbedGlueClient(List.of()));

        assertThat(metadata.redirectTable(SESSION, new SchemaTableName("staging_anything", "t")))
                .contains(new CatalogSchemaTableName("catalog_staging", "anything", "t"));
        assertThat(metadata.redirectTable(SESSION, new SchemaTableName("prod_other", "t")))
                .isEmpty();
    }

    @Test
    public void testLocalGlueCatalogListsSchemas()
    {
        RouterMetadata metadata = new RouterMetadata(
                new RouterConfig().setDefaultCatalogTarget("catalog_default"),
                stubbedGlueClient(List.of("sales", "marketing")));

        assertThat(metadata.listSchemaNames(SESSION)).containsExactlyInAnyOrder("sales", "marketing");
    }

    @Test
    public void testLocalGlueCatalogRedirectsTable()
    {
        RouterMetadata metadata = new RouterMetadata(
                new RouterConfig().setDefaultCatalogTarget("catalog_default"),
                stubbedGlueClient(List.of()));

        assertThat(metadata.redirectTable(SESSION, new SchemaTableName("sales", "orders")))
                .contains(new CatalogSchemaTableName("catalog_default", "sales", "orders"));
    }

    @Test
    public void testLocalGlueCatalogMappedSchemasFilter()
    {
        RouterMetadata metadata = new RouterMetadata(
                new RouterConfig()
                        .setDefaultCatalogTarget("catalog_default")
                        .setDefaultMappedSchemas(List.of("sales.*")),
                stubbedGlueClient(List.of("sales", "sales_eu", "marketing")));

        assertThat(metadata.listSchemaNames(SESSION)).containsExactlyInAnyOrder("sales", "sales_eu");
        assertThat(metadata.redirectTable(SESSION, new SchemaTableName("sales", "t")))
                .contains(new CatalogSchemaTableName("catalog_default", "sales", "t"));
        assertThat(metadata.redirectTable(SESSION, new SchemaTableName("marketing", "t"))).isEmpty();
    }

    @Test
    public void testLocalGlueCatalogSchemaExists()
    {
        RouterMetadata metadata = new RouterMetadata(
                new RouterConfig()
                        .setDefaultCatalogTarget("catalog_default")
                        .setDefaultMappedSchemas(List.of("sales.*")),
                stubbedGlueClient(List.of()));

        assertThat(metadata.schemaExists(SESSION, "sales")).isTrue();
        assertThat(metadata.schemaExists(SESSION, "sales_eu")).isTrue();
        assertThat(metadata.schemaExists(SESSION, "marketing")).isFalse();
    }

    @Test
    public void testPrefixRuleTakesPrecedenceOverLocalGlue()
    {
        RouterMetadata metadata = new RouterMetadata(
                new RouterConfig()
                        .setSchemaPrefixRules(List.of("prod_=catalog_prod"))
                        .setDefaultCatalogTarget("catalog_default"),
                stubbedGlueClient(List.of()));

        // prod_sales matches the prefix rule — must NOT be redirected via local Glue
        assertThat(metadata.redirectTable(SESSION, new SchemaTableName("prod_sales", "t")))
                .contains(new CatalogSchemaTableName("catalog_prod", "sales", "t"));
        // sales has no prefix match — falls through to local Glue
        assertThat(metadata.redirectTable(SESSION, new SchemaTableName("sales", "t")))
                .contains(new CatalogSchemaTableName("catalog_default", "sales", "t"));
    }

    @Test
    public void testLocalGlueCatalogUsesConfiguredCatalogId()
    {
        GlueClient glueClient = catalogAwareGlueClient(Map.of("999900001111", List.of("sales")));

        RouterMetadata metadata = new RouterMetadata(
                new RouterConfig()
                        .setDefaultCatalogTarget("catalog_default")
                        .setDefaultCatalogId("999900001111"),
                glueClient);

        assertThat(metadata.listSchemaNames(SESSION)).containsExactlyInAnyOrder("sales");
    }

    private static RouterMetadata metadataWithPrefix(String prefix, String catalog)
    {
        return new RouterMetadata(
                new RouterConfig().setSchemaPrefixRules(List.of(prefix + "=" + catalog)),
                stubbedGlueClient(List.of()));
    }

    // Returns databases keyed by catalog ID; use empty string key for the no-catalog-id case.
    private static GlueClient catalogAwareGlueClient(Map<String, List<String>> dbsByCatalogId)
    {
        return new GlueClient()
        {
            @Override
            public String serviceName()
            {
                return "glue";
            }

            @Override
            public void close() {}

            @Override
            public GetDatabasesResponse getDatabases(GetDatabasesRequest request)
            {
                String key = request.catalogId() != null ? request.catalogId() : "";
                List<String> names = dbsByCatalogId.getOrDefault(key, List.of());
                List<Database> databases = names.stream()
                        .map(name -> Database.builder().name(name).build())
                        .toList();
                return GetDatabasesResponse.builder().databaseList(databases).build();
            }
        };
    }

    private static GlueClient stubbedGlueClient(List<String> databaseNames)
    {
        List<Database> databases = databaseNames.stream()
                .map(name -> Database.builder().name(name).build())
                .toList();
        GetDatabasesResponse response = GetDatabasesResponse.builder().databaseList(databases).build();

        return new GlueClient()
        {
            @Override
            public String serviceName()
            {
                return "glue";
            }

            @Override
            public void close() {}

            @Override
            public GetDatabasesResponse getDatabases(GetDatabasesRequest request)
            {
                return response;
            }
        };
    }
}
