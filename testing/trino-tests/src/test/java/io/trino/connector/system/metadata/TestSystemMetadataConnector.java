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
package io.trino.connector.system.metadata;

import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.CountingMockConnector;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tests.FailingMockConnectorPlugin;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableMultiset.toImmutableMultiset;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.stream.Collectors.joining;

public class TestSystemMetadataConnector
        extends AbstractTestQueryFramework
{
    private static final int MAX_PREFIXES_COUNT = 10;

    private CountingMockConnector countingMockConnector;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        countingMockConnector = closeAfterClass(new CountingMockConnector());
        closeAfterClass(() -> countingMockConnector = null);
        Session session = testSessionBuilder().build();
        QueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setNodeCount(1)
                .addCoordinatorProperty("optimizer.experimental-max-prefetched-information-schema-prefixes", Integer.toString(MAX_PREFIXES_COUNT))
                .build();
        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            queryRunner.installPlugin(countingMockConnector.getPlugin());
            queryRunner.createCatalog("test_catalog", "mock", Map.of());

            queryRunner.installPlugin(new FailingMockConnectorPlugin());
            queryRunner.createCatalog("broken_catalog", "failing_mock", Map.of());
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }

    @Test
    public void testTableCommentsMetadataCalls()
    {
        // Specific relation
        assertMetadataCalls(
                "SELECT comment FROM system.metadata.table_comments WHERE catalog_name = 'test_catalog' AND schema_name = 'test_schema1' AND table_name = 'test_table1'",
                "VALUES 'comment for test_schema1.test_table1'",
                ImmutableMultiset.<String>builder()
                        .addCopies("ConnectorMetadata.getSystemTable(schema=test_schema1, table=test_table1)", 4)
                        .add("ConnectorMetadata.getMaterializedView(schema=test_schema1, table=test_table1)")
                        .add("ConnectorMetadata.getView(schema=test_schema1, table=test_table1)")
                        .add("ConnectorMetadata.redirectTable(schema=test_schema1, table=test_table1)")
                        .add("ConnectorMetadata.getTableHandle(schema=test_schema1, table=test_table1)")
                        .add("ConnectorMetadata.getTableMetadata(handle=test_schema1.test_table1)")
                        .build());

        // Specific relation that does not exist
        assertMetadataCalls(
                "SELECT comment FROM system.metadata.table_comments WHERE catalog_name = 'test_catalog' AND schema_name = 'test_schema1' AND table_name = 'does_not_exist'",
                "SELECT '' WHERE false",
                ImmutableMultiset.<String>builder()
                        .addCopies("ConnectorMetadata.getSystemTable(schema=test_schema1, table=does_not_exist)", 4)
                        .add("ConnectorMetadata.getMaterializedView(schema=test_schema1, table=does_not_exist)")
                        .add("ConnectorMetadata.getView(schema=test_schema1, table=does_not_exist)")
                        .add("ConnectorMetadata.redirectTable(schema=test_schema1, table=does_not_exist)")
                        .add("ConnectorMetadata.getTableHandle(schema=test_schema1, table=does_not_exist)")
                        .build());

        // Specific relation in a schema that does not exist
        assertMetadataCalls(
                "SELECT comment FROM system.metadata.table_comments WHERE catalog_name = 'test_catalog' AND schema_name = 'wrong_schema1' AND table_name = 'test_table1'",
                "SELECT '' WHERE false",
                ImmutableMultiset.<String>builder()
                        .addCopies("ConnectorMetadata.getSystemTable(schema=wrong_schema1, table=test_table1)", 4)
                        .add("ConnectorMetadata.getMaterializedView(schema=wrong_schema1, table=test_table1)")
                        .add("ConnectorMetadata.getView(schema=wrong_schema1, table=test_table1)")
                        .add("ConnectorMetadata.redirectTable(schema=wrong_schema1, table=test_table1)")
                        .add("ConnectorMetadata.getTableHandle(schema=wrong_schema1, table=test_table1)")
                        .build());

        // Specific relation in a schema that does not exist across existing and non-existing catalogs
        assertMetadataCalls(
                "SELECT comment FROM system.metadata.table_comments WHERE catalog_name IN ('wrong', 'test_catalog') AND schema_name = 'wrong_schema1' AND table_name = 'test_table1'",
                "SELECT '' WHERE false",
                ImmutableMultiset.<String>builder()
                        .addCopies("ConnectorMetadata.getSystemTable(schema=wrong_schema1, table=test_table1)", 4)
                        .add("ConnectorMetadata.getMaterializedView(schema=wrong_schema1, table=test_table1)")
                        .add("ConnectorMetadata.getView(schema=wrong_schema1, table=test_table1)")
                        .add("ConnectorMetadata.redirectTable(schema=wrong_schema1, table=test_table1)")
                        .add("ConnectorMetadata.getTableHandle(schema=wrong_schema1, table=test_table1)")
                        .build());

        // Whole catalog
        assertMetadataCalls(
                "SELECT count(DISTINCT schema_name), count(DISTINCT table_name), count(comment), count(*) FROM system.metadata.table_comments WHERE catalog_name = 'test_catalog'",
                "VALUES (3, 2008, 3000, 3008)",
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.streamRelationComments")
                        .build());

        // Whole catalog except for information_schema (typical query run by BI tools)
        assertMetadataCalls(
                "SELECT count(DISTINCT schema_name), count(DISTINCT table_name), count(comment), count(*) FROM system.metadata.table_comments WHERE catalog_name = 'test_catalog' AND schema_name != 'information_schema'",
                "VALUES (2, 2000, 3000, 3000)",
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.streamRelationComments")
                        .build());

        // Two catalogs
        assertMetadataCalls(
                "SELECT count(DISTINCT schema_name), count(DISTINCT table_name), count(comment), count(*) FROM system.metadata.table_comments WHERE catalog_name IN ('test_catalog', 'tpch')",
                "VALUES (12, 2016, 3000, 3088)",
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.streamRelationComments")
                        .build());

        // Whole schema
        assertMetadataCalls(
                "SELECT count(table_name), count(comment) FROM system.metadata.table_comments WHERE catalog_name = 'test_catalog' AND schema_name = 'test_schema1'",
                "VALUES (1000, 1000)",
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.streamRelationComments(schema=test_schema1)")
                        .build());

        // Two schemas
        assertMetadataCalls(
                "SELECT count(DISTINCT schema_name), count(DISTINCT table_name), count(comment), count(*) FROM system.metadata.table_comments WHERE catalog_name = 'test_catalog' AND schema_name IN ('test_schema1', 'test_schema2')",
                "VALUES (2, 2000, 3000, 3000)",
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.streamRelationComments(schema=test_schema1)")
                        .add("ConnectorMetadata.streamRelationComments(schema=test_schema2)")
                        .build());

        // Multiple schemas
        assertMetadataCalls(
                "SELECT count(DISTINCT schema_name), count(DISTINCT table_name), count(comment), count(*) FROM system.metadata.table_comments WHERE catalog_name = 'test_catalog' AND schema_name IN " +
                        Stream.concat(
                                        Stream.of("test_schema1", "test_schema2"),
                                        IntStream.range(1, MAX_PREFIXES_COUNT + 1)
                                                .mapToObj(i -> "bogus_schema" + i))
                                .map("'%s'"::formatted)
                                .collect(joining(",", "(", ")")),
                "VALUES (2, 2000, 3000, 3000)",
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.streamRelationComments(schema=test_schema1)")
                        .add("ConnectorMetadata.streamRelationComments(schema=test_schema2)")
                        .addAll(IntStream.range(1, MAX_PREFIXES_COUNT + 1)
                                .mapToObj("ConnectorMetadata.streamRelationComments(schema=bogus_schema%s)"::formatted)
                                .toList())
                        .build());

        // Small LIMIT
        assertMetadataCalls(
                "SELECT count(*) FROM (SELECT * FROM system.metadata.table_comments WHERE catalog_name = 'test_catalog' LIMIT 1)",
                "VALUES 1",
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.streamRelationComments")
                        .build());

        // Big LIMIT
        assertMetadataCalls(
                "SELECT count(*) FROM (SELECT * FROM system.metadata.table_comments WHERE catalog_name = 'test_catalog' LIMIT 1000)",
                "VALUES 1000",
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.streamRelationComments")
                        .build());

        // Non-existent catalog
        assertMetadataCalls(
                "SELECT comment FROM system.metadata.table_comments WHERE catalog_name = 'wrong'",
                "SELECT '' WHERE false",
                ImmutableMultiset.of());

        // Empty catalog name
        assertMetadataCalls(
                "SELECT comment FROM system.metadata.table_comments WHERE catalog_name = ''",
                "SELECT '' WHERE false",
                ImmutableMultiset.of());

        // Empty table schema and table name
        assertMetadataCalls(
                "SELECT comment FROM system.metadata.table_comments WHERE schema_name = '' AND table_name = ''",
                "SELECT '' WHERE false",
                ImmutableMultiset.of());

        // Empty table schema
        assertMetadataCalls(
                "SELECT count(comment) FROM system.metadata.table_comments WHERE schema_name = ''",
                "VALUES 0",
                ImmutableMultiset.of());

        // Empty table name
        assertMetadataCalls(
                "SELECT count(comment) FROM system.metadata.table_comments WHERE table_name = ''",
                "VALUES 0",
                ImmutableMultiset.of());
    }

    @Test
    public void testMaterializedViewsMetadataCalls()
    {
        // Specific relation
        assertMetadataCalls(
                "SELECT comment FROM system.metadata.materialized_views WHERE catalog_name = 'test_catalog' AND schema_name = 'test_schema1' AND name = 'test_table1'",
                // TODO introduce materialized views in CountingMockConnector
                "SELECT '' WHERE false",
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.getMaterializedView(schema=test_schema1, table=test_table1)")
                        .build());

        // Specific relation that does not exist
        assertMetadataCalls(
                "SELECT comment FROM system.metadata.materialized_views WHERE catalog_name = 'test_catalog' AND schema_name = 'test_schema1' AND name = 'does_not_exist'",
                "SELECT '' WHERE false",
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.getMaterializedView(schema=test_schema1, table=does_not_exist)")
                        .build());

        // Specific relation in a schema that does not exist
        assertMetadataCalls(
                "SELECT comment FROM system.metadata.materialized_views WHERE catalog_name = 'test_catalog' AND schema_name = 'wrong_schema1' AND name = 'test_table1'",
                "SELECT '' WHERE false",
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.getMaterializedView(schema=wrong_schema1, table=test_table1)")
                        .build());

        // Specific relation in a schema that does not exist across existing and non-existing catalogs
        assertMetadataCalls(
                // TODO should succeed, the broken_catalog is not one of the selected catalogs
                "SELECT comment FROM system.metadata.materialized_views WHERE catalog_name IN ('wrong', 'test_catalog') AND schema_name = 'wrong_schema1' AND name = 'test_table1'",
                "SELECT '' WHERE false",
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.getMaterializedView(schema=wrong_schema1, table=test_table1)")
                        .build());

        // Whole catalog
        assertMetadataCalls(
                "SELECT count(DISTINCT schema_name), count(DISTINCT name), count(comment), count(*) FROM system.metadata.materialized_views WHERE catalog_name = 'test_catalog'",
                // TODO introduce materialized views in CountingMockConnector
                "VALUES (0, 0, 0, 0)",
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.getMaterializedViews")
                        .build());

        // Whole catalog except for information_schema (typical query run by BI tools)
        assertMetadataCalls(
                "SELECT count(DISTINCT schema_name), count(DISTINCT name), count(comment), count(*) FROM system.metadata.materialized_views WHERE catalog_name = 'test_catalog' AND schema_name != 'information_schema'",
                // TODO introduce materialized views in CountingMockConnector
                "VALUES (0, 0, 0, 0)",
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.getMaterializedViews")
                        .build());

        // Two catalogs
        assertMetadataCalls(
                "SELECT count(DISTINCT schema_name), count(DISTINCT name), count(comment), count(*) FROM system.metadata.materialized_views WHERE catalog_name IN ('test_catalog', 'tpch')",
                // TODO introduce materialized views in CountingMockConnector
                "VALUES (0, 0, 0, 0)",
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.getMaterializedViews")
                        .build());

        // Whole schema
        assertMetadataCalls(
                "SELECT count(name), count(comment) FROM system.metadata.materialized_views WHERE catalog_name = 'test_catalog' AND schema_name = 'test_schema1'",
                // TODO introduce materialized views in CountingMockConnector
                "VALUES (0, 0)",
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.getMaterializedViews(schema=test_schema1)")
                        .build());

        // Two schemas
        assertMetadataCalls(
                "SELECT count(DISTINCT schema_name), count(DISTINCT name), count(comment), count(*) FROM system.metadata.materialized_views WHERE catalog_name = 'test_catalog' AND schema_name IN ('test_schema1', 'test_schema2')",
                // TODO introduce materialized views in CountingMockConnector
                "VALUES (0, 0, 0, 0)",
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.getMaterializedViews(schema=test_schema1)")
                        .add("ConnectorMetadata.getMaterializedViews(schema=test_schema2)")
                        .build());

        // Multiple schemas
        assertMetadataCalls(
                "SELECT count(DISTINCT schema_name), count(DISTINCT name), count(comment), count(*) FROM system.metadata.materialized_views WHERE catalog_name = 'test_catalog' AND schema_name IN " +
                        Stream.concat(
                                        Stream.of("test_schema1", "test_schema2"),
                                        IntStream.range(1, MAX_PREFIXES_COUNT + 1)
                                                .mapToObj(i -> "bogus_schema" + i))
                                .map("'%s'"::formatted)
                                .collect(joining(",", "(", ")")),
                // TODO introduce materialized views in CountingMockConnector
                "VALUES (0, 0, 0, 0)",
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.getMaterializedViews(schema=test_schema1)")
                        .add("ConnectorMetadata.getMaterializedViews(schema=test_schema2)")
                        .addAll(IntStream.range(1, MAX_PREFIXES_COUNT + 1)
                                .mapToObj("ConnectorMetadata.getMaterializedViews(schema=bogus_schema%s)"::formatted)
                                .toList())
                        .build());

        // Small LIMIT
        assertMetadataCalls(
                "SELECT count(*) FROM (SELECT * FROM system.metadata.materialized_views WHERE catalog_name = 'test_catalog' LIMIT 1)",
                // TODO introduce materialized views in CountingMockConnector
                "VALUES 0",
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.getMaterializedViews")
                        .build());

        // Big LIMIT
        assertMetadataCalls(
                "SELECT count(*) FROM (SELECT * FROM system.metadata.materialized_views WHERE catalog_name = 'test_catalog' LIMIT 1000)",
                // TODO introduce thousands of materialized views in CountingMockConnector
                "VALUES 0",
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.getMaterializedViews")
                        .build());

        // Non-existent catalog
        assertMetadataCalls(
                "SELECT comment FROM system.metadata.materialized_views WHERE catalog_name = 'wrong'",
                "SELECT '' WHERE false",
                ImmutableMultiset.of());

        // Empty catalog name
        assertMetadataCalls(
                "SELECT comment FROM system.metadata.materialized_views WHERE catalog_name = ''",
                "SELECT '' WHERE false",
                ImmutableMultiset.of());

        // Empty table schema and table name
        assertMetadataCalls(
                "SELECT comment FROM system.metadata.materialized_views WHERE schema_name = '' AND name = ''",
                "SELECT '' WHERE false",
                ImmutableMultiset.of());

        // Empty table schema
        assertMetadataCalls(
                "SELECT count(comment) FROM system.metadata.materialized_views WHERE schema_name = ''",
                "VALUES 0",
                ImmutableMultiset.of());

        // Empty table name
        assertMetadataCalls(
                "SELECT count(comment) FROM system.metadata.materialized_views WHERE name = ''",
                "VALUES 0",
                ImmutableMultiset.of());
    }

    @Test
    public void testMetadataListingExceptionHandling()
    {
        // TODO this should probably gracefully continue when some catalog is "broken" (does not currently work, e.g. is offline)
        assertQueryFails(
                "SELECT * FROM system.metadata.table_comments",
                "Catalog is broken");

        // TODO this should probably gracefully continue when some catalog is "broken" (does not currently work, e.g. is offline)
        assertQueryFails(
                "SELECT * FROM system.metadata.materialized_views",
                "Error listing materialized views for catalog broken_catalog: Catalog is broken");
    }

    private void assertMetadataCalls(@Language("SQL") String actualSql, @Language("SQL") String expectedSql, Multiset<String> expectedMetadataCallsCount)
    {
        Multiset<String> actualMetadataCallsCount = countingMockConnector.runTracing(() -> {
            // expectedSql is run on H2, so does not affect counts.
            assertQuery(actualSql, expectedSql);
        });

        actualMetadataCallsCount = actualMetadataCallsCount.stream()
                // Every query involves beginQuery and cleanupQuery, so ignore them.
                .filter(method -> !"ConnectorMetadata.beginQuery".equals(method) && !"ConnectorMetadata.cleanupQuery".equals(method))
                .collect(toImmutableMultiset());

        assertMultisetsEqual(actualMetadataCallsCount, expectedMetadataCallsCount);
    }
}
