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
package io.trino.connector.informationschema;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.CountingMockConnector;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testng.services.ManageTestResources;
import io.trino.tests.FailingMockConnectorPlugin;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;

import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.stream.Collectors.joining;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestInformationSchemaConnector
        extends AbstractTestQueryFramework
{
    private static final int MAX_PREFIXES_COUNT = 10;

    @ManageTestResources.Suppress(because = "Not a TestNG test class")
    private CountingMockConnector countingMockConnector;

    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        countingMockConnector = closeAfterClass(new CountingMockConnector());
        Session session = testSessionBuilder().build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setNodeCount(1)
                .addCoordinatorProperty("optimizer.experimental-max-prefetched-information-schema-prefixes", Integer.toString(MAX_PREFIXES_COUNT))
                .build();
        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            queryRunner.installPlugin(countingMockConnector.getPlugin());
            queryRunner.createCatalog("test_catalog", "mock", ImmutableMap.of());

            queryRunner.installPlugin(new FailingMockConnectorPlugin());
            queryRunner.createCatalog("broken_catalog", "failing_mock", ImmutableMap.of());
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }

    @AfterAll
    public void cleanUp()
    {
        countingMockConnector = null; // closed by closeAfterClass
    }

    @Test
    public void testBasic()
    {
        assertQuery("SELECT count(*) FROM tpch.information_schema.schemata", "VALUES 10");
        assertQuery("SELECT count(*) FROM tpch.information_schema.tables", "VALUES 80");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns", "VALUES 583");
        assertQuery("SELECT * FROM tpch.information_schema.schemata ORDER BY 1 DESC, 2 DESC LIMIT 1", "VALUES ('tpch', 'tiny')");
        assertQuery("SELECT * FROM tpch.information_schema.tables ORDER BY 1 DESC, 2 DESC, 3 DESC, 4 DESC LIMIT 1", "VALUES ('tpch', 'tiny', 'supplier', 'BASE TABLE')");
        assertQuery("SELECT * FROM tpch.information_schema.columns ORDER BY 1 DESC, 2 DESC, 3 DESC, 4 DESC LIMIT 1", "VALUES ('tpch', 'tiny', 'supplier', 'suppkey', 1, NULL, 'NO', 'bigint')");
        assertQuery("SELECT * FROM test_catalog.information_schema.columns ORDER BY 1 DESC, 2 DESC, 3 DESC, 4 DESC LIMIT 1", "VALUES ('test_catalog', 'test_schema2', 'test_table999', 'column_99', 100, NULL, 'YES', 'varchar')");
        assertQuery("SELECT count(*) FROM test_catalog.information_schema.columns", "VALUES 300034");
    }

    @Test
    public void testSchemaNamePredicate()
    {
        assertQuery("SELECT count(*) FROM tpch.information_schema.schemata WHERE schema_name = 'sf1'", "VALUES 1");
        assertQuery("SELECT count(*) FROM tpch.information_schema.schemata WHERE schema_name IS NOT NULL", "VALUES 10");
        assertQuery("SELECT count(*) FROM tpch.information_schema.tables WHERE table_schema = 'sf1'", "VALUES 8");
        assertQuery("SELECT count(*) FROM tpch.information_schema.tables WHERE table_schema IS NOT NULL", "VALUES 80");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns WHERE table_schema = 'sf1'", "VALUES 61");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns WHERE table_schema = 'information_schema'", "VALUES 34");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns WHERE table_schema > 'sf100'", "VALUES 427");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns WHERE table_schema != 'sf100'", "VALUES 522");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns WHERE table_schema LIKE 'sf100'", "VALUES 61");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns WHERE table_schema LIKE 'sf%'", "VALUES 488");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns WHERE table_schema IS NOT NULL", "VALUES 583");
    }

    @Test
    public void testTableNamePredicate()
    {
        assertQuery("SELECT count(*) FROM tpch.information_schema.tables WHERE table_name = 'orders'", "VALUES 9");
        assertQuery("SELECT count(*) FROM tpch.information_schema.tables WHERE table_name = 'ORDERS'", "VALUES 0");
        assertQuery("SELECT count(*) FROM tpch.information_schema.tables WHERE table_name LIKE 'orders'", "VALUES 9");
        assertQuery("SELECT count(*) FROM tpch.information_schema.tables WHERE table_name < 'orders'", "VALUES 30");
        assertQuery("SELECT count(*) FROM tpch.information_schema.tables WHERE table_name LIKE 'part'", "VALUES 9");
        assertQuery("SELECT count(*) FROM tpch.information_schema.tables WHERE table_name LIKE 'part%'", "VALUES 18");
        assertQuery("SELECT count(*) FROM tpch.information_schema.tables WHERE table_name IS NOT NULL", "VALUES 80");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns WHERE table_name = 'orders'", "VALUES 81");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns WHERE table_name LIKE 'orders'", "VALUES 81");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns WHERE table_name < 'orders'", "VALUES 265");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns WHERE table_name LIKE 'part'", "VALUES 81");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns WHERE table_name LIKE 'part%'", "VALUES 126");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns WHERE table_name IS NOT NULL", "VALUES 583");
    }

    @Test
    public void testMixedPredicate()
    {
        assertQuery("SELECT * FROM tpch.information_schema.tables WHERE table_schema = 'sf1' and table_name = 'orders'", "VALUES ('tpch', 'sf1', 'orders', 'BASE TABLE')");
        assertQuery("SELECT table_schema FROM tpch.information_schema.tables WHERE table_schema IS NOT NULL and table_name = 'orders'", "VALUES 'tiny', 'sf1', 'sf100', 'sf1000', 'sf10000', 'sf100000', 'sf300', 'sf3000', 'sf30000'");
        assertQuery("SELECT table_name FROM tpch.information_schema.tables WHERE table_schema = 'sf1' and table_name IS NOT NULL", "VALUES 'customer', 'lineitem', 'orders', 'part', 'partsupp', 'supplier', 'nation', 'region'");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns WHERE table_schema = 'sf1' and table_name = 'orders'", "VALUES 9");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns WHERE table_schema IS NOT NULL and table_name = 'orders'", "VALUES 81");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns WHERE table_schema = 'sf1' and table_name IS NOT NULL", "VALUES 61");
        assertQuery("SELECT count(*) FROM tpch.information_schema.tables WHERE table_schema > 'sf1' and table_name < 'orders'", "VALUES 24");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns WHERE table_schema > 'sf1' and table_name < 'orders'", "VALUES 224");
    }

    @Test
    public void testProject()
    {
        assertQuery("SELECT schema_name FROM tpch.information_schema.schemata ORDER BY 1 DESC LIMIT 1", "VALUES 'tiny'");
        assertQuery("SELECT table_name, table_type FROM tpch.information_schema.tables ORDER BY 1 DESC, 2 DESC LIMIT 1", "VALUES ('views', 'BASE TABLE')");
        assertQuery("SELECT column_name, data_type FROM tpch.information_schema.columns ORDER BY 1 DESC, 2 DESC LIMIT 1", "VALUES ('with_hierarchy', 'varchar')");
    }

    @Test
    public void testLimit()
    {
        assertQuery("SELECT count(*) FROM (SELECT * from tpch.information_schema.columns LIMIT 1)", "VALUES 1");
        assertQuery("SELECT count(*) FROM (SELECT * FROM tpch.information_schema.columns LIMIT 100)", "VALUES 100");
        assertQuery("SELECT count(*) FROM (SELECT * FROM test_catalog.information_schema.tables LIMIT 1000)", "VALUES 1000");
    }

    @Test
    @Timeout(60)
    public void testMetadataCalls()
    {
        assertMetadataCalls(
                "SELECT count(*) from test_catalog.information_schema.schemata WHERE schema_name LIKE 'test_sch_ma1'",
                "VALUES 1",
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.listSchemaNames")
                        .build());
        assertMetadataCalls(
                "SELECT count(*) from test_catalog.information_schema.schemata WHERE schema_name LIKE 'test_sch_ma1' AND schema_name IN ('test_schema1', 'test_schema2')",
                "VALUES 1",
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.listSchemaNames")
                        .build());
        assertMetadataCalls(
                "SELECT count(table_name), count(table_type) from test_catalog.information_schema.tables",
                "VALUES (3008, 3008)",
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.getRelationTypes")
                        .build());
        assertMetadataCalls(
                "SELECT count(table_name), count(table_type) from test_catalog.information_schema.tables WHERE table_schema = 'test_schema1'",
                "VALUES (1000, 1000)",
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.getRelationTypes(schema=test_schema1)")
                        .build());
        assertMetadataCalls(
                "SELECT count(table_name), count(table_type) from test_catalog.information_schema.tables WHERE table_schema LIKE 'test_sch_ma1'",
                "VALUES (1000, 1000)",
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.listSchemaNames")
                        .add("ConnectorMetadata.getRelationTypes(schema=test_schema1)")
                        .build());
        assertMetadataCalls(
                "SELECT count(table_name), count(table_type) from test_catalog.information_schema.tables WHERE table_schema LIKE 'test_sch_ma1' AND table_schema IN ('test_schema1', 'test_schema2')",
                "VALUES (1000, 1000)",
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.getRelationTypes(schema=test_schema1)")
                        .add("ConnectorMetadata.getRelationTypes(schema=test_schema2)")
                        .build());
        assertMetadataCalls(
                "SELECT count(table_name), count(table_type) from test_catalog.information_schema.tables WHERE table_schema IN " +
                        Stream.concat(
                                        Stream.of("test_schema1", "test_schema2"),
                                        IntStream.range(1, MAX_PREFIXES_COUNT + 1)
                                                .mapToObj(i -> "bogus_schema" + i))
                                .map("'%s'"::formatted)
                                .collect(joining(",", "(", ")")),
                "VALUES (3000, 3000)",
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.getRelationTypes")
                        .build());
        assertMetadataCalls(
                "SELECT count(table_name), count(table_type) from test_catalog.information_schema.tables WHERE table_name = 'test_table1'",
                "VALUES (2, 2)",
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.listSchemaNames")
                        .addCopies("ConnectorMetadata.getSystemTable(schema=test_schema1, table=test_table1)", 4)
                        .addCopies("ConnectorMetadata.getSystemTable(schema=test_schema2, table=test_table1)", 4)
                        .addCopies("ConnectorMetadata.getSystemTable(schema=test_schema3_empty, table=test_table1)", 4)
                        .addCopies("ConnectorMetadata.getSystemTable(schema=test_schema4_empty, table=test_table1)", 4)
                        .add("ConnectorMetadata.getMaterializedView(schema=test_schema1, table=test_table1)")
                        .add("ConnectorMetadata.getMaterializedView(schema=test_schema2, table=test_table1)")
                        .add("ConnectorMetadata.getMaterializedView(schema=test_schema3_empty, table=test_table1)")
                        .add("ConnectorMetadata.getMaterializedView(schema=test_schema4_empty, table=test_table1)")
                        .add("ConnectorMetadata.getView(schema=test_schema1, table=test_table1)")
                        .add("ConnectorMetadata.getView(schema=test_schema2, table=test_table1)")
                        .add("ConnectorMetadata.getView(schema=test_schema3_empty, table=test_table1)")
                        .add("ConnectorMetadata.getView(schema=test_schema4_empty, table=test_table1)")
                        .add("ConnectorMetadata.redirectTable(schema=test_schema1, table=test_table1)")
                        .add("ConnectorMetadata.redirectTable(schema=test_schema2, table=test_table1)")
                        .add("ConnectorMetadata.redirectTable(schema=test_schema3_empty, table=test_table1)")
                        .add("ConnectorMetadata.redirectTable(schema=test_schema4_empty, table=test_table1)")
                        .add("ConnectorMetadata.getTableHandle(schema=test_schema1, table=test_table1)")
                        .add("ConnectorMetadata.getTableHandle(schema=test_schema2, table=test_table1)")
                        .add("ConnectorMetadata.getTableHandle(schema=test_schema3_empty, table=test_table1)")
                        .add("ConnectorMetadata.getTableHandle(schema=test_schema4_empty, table=test_table1)")
                        .build());
        assertMetadataCalls(
                "SELECT count(table_name), count(table_type) from test_catalog.information_schema.tables WHERE table_name LIKE 'test_t_ble1'",
                "VALUES (2, 2)",
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.listSchemaNames")
                        .add("ConnectorMetadata.getRelationTypes(schema=test_schema1)")
                        .add("ConnectorMetadata.getRelationTypes(schema=test_schema2)")
                        .add("ConnectorMetadata.getRelationTypes(schema=test_schema3_empty)")
                        .add("ConnectorMetadata.getRelationTypes(schema=test_schema4_empty)")
                        .build());
        assertMetadataCalls(
                "SELECT count(table_name), count(table_type) from test_catalog.information_schema.tables WHERE table_name LIKE 'test_t_ble1' AND table_name IN ('test_table1', 'test_table2')",
                "VALUES (2, 2)",
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.listSchemaNames")
                        .addCopies("ConnectorMetadata.getSystemTable(schema=test_schema1, table=test_table1)", 4)
                        .addCopies("ConnectorMetadata.getSystemTable(schema=test_schema1, table=test_table2)", 4)
                        .addCopies("ConnectorMetadata.getSystemTable(schema=test_schema2, table=test_table1)", 4)
                        .addCopies("ConnectorMetadata.getSystemTable(schema=test_schema2, table=test_table2)", 4)
                        .addCopies("ConnectorMetadata.getSystemTable(schema=test_schema3_empty, table=test_table1)", 4)
                        .addCopies("ConnectorMetadata.getSystemTable(schema=test_schema3_empty, table=test_table2)", 4)
                        .addCopies("ConnectorMetadata.getSystemTable(schema=test_schema4_empty, table=test_table1)", 4)
                        .addCopies("ConnectorMetadata.getSystemTable(schema=test_schema4_empty, table=test_table2)", 4)
                        .add("ConnectorMetadata.getMaterializedView(schema=test_schema1, table=test_table1)")
                        .add("ConnectorMetadata.getMaterializedView(schema=test_schema1, table=test_table2)")
                        .add("ConnectorMetadata.getMaterializedView(schema=test_schema2, table=test_table2)")
                        .add("ConnectorMetadata.getMaterializedView(schema=test_schema2, table=test_table1)")
                        .add("ConnectorMetadata.getMaterializedView(schema=test_schema3_empty, table=test_table2)")
                        .add("ConnectorMetadata.getMaterializedView(schema=test_schema3_empty, table=test_table1)")
                        .add("ConnectorMetadata.getMaterializedView(schema=test_schema4_empty, table=test_table2)")
                        .add("ConnectorMetadata.getMaterializedView(schema=test_schema4_empty, table=test_table1)")
                        .add("ConnectorMetadata.getView(schema=test_schema1, table=test_table1)")
                        .add("ConnectorMetadata.getView(schema=test_schema1, table=test_table2)")
                        .add("ConnectorMetadata.getView(schema=test_schema2, table=test_table1)")
                        .add("ConnectorMetadata.getView(schema=test_schema2, table=test_table2)")
                        .add("ConnectorMetadata.getView(schema=test_schema3_empty, table=test_table1)")
                        .add("ConnectorMetadata.getView(schema=test_schema3_empty, table=test_table2)")
                        .add("ConnectorMetadata.getView(schema=test_schema4_empty, table=test_table1)")
                        .add("ConnectorMetadata.getView(schema=test_schema4_empty, table=test_table2)")
                        .add("ConnectorMetadata.redirectTable(schema=test_schema1, table=test_table1)")
                        .add("ConnectorMetadata.redirectTable(schema=test_schema1, table=test_table2)")
                        .add("ConnectorMetadata.redirectTable(schema=test_schema2, table=test_table1)")
                        .add("ConnectorMetadata.redirectTable(schema=test_schema2, table=test_table2)")
                        .add("ConnectorMetadata.redirectTable(schema=test_schema3_empty, table=test_table1)")
                        .add("ConnectorMetadata.redirectTable(schema=test_schema3_empty, table=test_table2)")
                        .add("ConnectorMetadata.redirectTable(schema=test_schema4_empty, table=test_table1)")
                        .add("ConnectorMetadata.redirectTable(schema=test_schema4_empty, table=test_table2)")
                        .add("ConnectorMetadata.getTableHandle(schema=test_schema1, table=test_table1)")
                        .add("ConnectorMetadata.getTableHandle(schema=test_schema1, table=test_table2)")
                        .add("ConnectorMetadata.getTableHandle(schema=test_schema2, table=test_table1)")
                        .add("ConnectorMetadata.getTableHandle(schema=test_schema2, table=test_table2)")
                        .add("ConnectorMetadata.getTableHandle(schema=test_schema3_empty, table=test_table1)")
                        .add("ConnectorMetadata.getTableHandle(schema=test_schema3_empty, table=test_table2)")
                        .add("ConnectorMetadata.getTableHandle(schema=test_schema4_empty, table=test_table1)")
                        .add("ConnectorMetadata.getTableHandle(schema=test_schema4_empty, table=test_table2)")
                        .build());
        assertMetadataCalls(
                "SELECT count(*) from test_catalog.information_schema.columns WHERE table_schema = 'test_schema1' AND table_name = 'test_table1'",
                "VALUES 100",
                ImmutableMultiset.<String>builder()
                        .addCopies("ConnectorMetadata.getSystemTable(schema=test_schema1, table=test_table1)", 4)
                        .add("ConnectorMetadata.getMaterializedView(schema=test_schema1, table=test_table1)")
                        .add("ConnectorMetadata.getView(schema=test_schema1, table=test_table1)")
                        .add("ConnectorMetadata.redirectTable(schema=test_schema1, table=test_table1)")
                        .add("ConnectorMetadata.getTableHandle(schema=test_schema1, table=test_table1)")
                        .add("ConnectorMetadata.getTableMetadata(handle=test_schema1.test_table1)")
                        .build());
        assertMetadataCalls(
                "SELECT count(*) from test_catalog.information_schema.columns WHERE table_catalog = 'wrong'",
                "VALUES 0",
                ImmutableMultiset.of());
        assertMetadataCalls(
                "SELECT count(*) from test_catalog.information_schema.columns WHERE table_catalog = 'test_catalog' AND table_schema = 'wrong_schema1' AND table_name = 'test_table1'",
                "VALUES 0",
                ImmutableMultiset.<String>builder()
                        .addCopies("ConnectorMetadata.getSystemTable(schema=wrong_schema1, table=test_table1)", 4)
                        .add("ConnectorMetadata.getMaterializedView(schema=wrong_schema1, table=test_table1)")
                        .add("ConnectorMetadata.getView(schema=wrong_schema1, table=test_table1)")
                        .add("ConnectorMetadata.redirectTable(schema=wrong_schema1, table=test_table1)")
                        .add("ConnectorMetadata.getTableHandle(schema=wrong_schema1, table=test_table1)")
                        .build());
        assertMetadataCalls(
                "SELECT count(*) from test_catalog.information_schema.columns WHERE table_catalog IN ('wrong', 'test_catalog') AND table_schema = 'wrong_schema1' AND table_name = 'test_table1'",
                "VALUES 0",
                ImmutableMultiset.<String>builder()
                        .addCopies("ConnectorMetadata.getSystemTable(schema=wrong_schema1, table=test_table1)", 4)
                        .add("ConnectorMetadata.getMaterializedView(schema=wrong_schema1, table=test_table1)")
                        .add("ConnectorMetadata.getView(schema=wrong_schema1, table=test_table1)")
                        .add("ConnectorMetadata.redirectTable(schema=wrong_schema1, table=test_table1)")
                        .add("ConnectorMetadata.getTableHandle(schema=wrong_schema1, table=test_table1)")
                        .build());
        assertMetadataCalls(
                "SELECT count(*) FROM (SELECT * from test_catalog.information_schema.columns LIMIT 1)",
                "VALUES 1",
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.listSchemaNames")
                        .build());
        assertMetadataCalls(
                "SELECT count(*) FROM (SELECT * from test_catalog.information_schema.columns LIMIT 1000)",
                "VALUES 1000",
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.listSchemaNames")
                        .add("ConnectorMetadata.streamRelationColumns(schema=test_schema1)")
                        .build());

        // Empty table schema and table name
        assertMetadataCalls(
                "SELECT count(table_name), count(table_type) from test_catalog.information_schema.tables WHERE table_schema = '' AND table_name = ''",
                "VALUES (0, 0)",
                ImmutableMultiset.of());

        // Empty table schema
        assertMetadataCalls(
                "SELECT count(table_name), count(table_type) from test_catalog.information_schema.tables WHERE table_schema = ''",
                "VALUES (0, 0)",
                ImmutableMultiset.of());

        // Empty table name
        assertMetadataCalls(
                "SELECT count(table_name), count(table_type) from test_catalog.information_schema.tables WHERE table_name = ''",
                "VALUES (0, 0)",
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.listSchemaNames")
                        .build());

        // Subset of tables' columns: table_type not selected
        assertMetadataCalls(
                "SELECT count(table_name) from test_catalog.information_schema.tables WHERE table_schema LIKE 'test_sch_ma1'",
                "VALUES 1000",
                ImmutableMultiset.<String>builder()
                        .add("ConnectorMetadata.listSchemaNames")
                        .add("ConnectorMetadata.listTables(schema=test_schema1)")
                        // view-related methods such as listViews not being called
                        .build());
    }

    @Test
    public void testMetadataListingExceptionHandling()
    {
        assertQueryFails(
                "SELECT * FROM broken_catalog.information_schema.schemata",
                "Error listing schemas for catalog broken_catalog: Catalog is broken");

        assertQueryFails(
                "SELECT * FROM broken_catalog.information_schema.tables",
                "Error listing tables for catalog broken_catalog: Catalog is broken");

        assertQueryFails(
                "SELECT * FROM broken_catalog.information_schema.views",
                "Error listing views for catalog broken_catalog: Catalog is broken");

        assertQueryFails(
                "SELECT * FROM broken_catalog.information_schema.table_privileges",
                "Error listing table privileges for catalog broken_catalog: Catalog is broken");

        assertQueryFails(
                "SELECT * FROM broken_catalog.information_schema.columns",
                "Error listing table columns for catalog broken_catalog: Catalog is broken");
    }

    private void assertMetadataCalls(@Language("SQL") String actualSql, @Language("SQL") String expectedSql, Multiset<String> expectedMetadataCallsCount)
    {
        expectedMetadataCallsCount = ImmutableMultiset.<String>builder()
                // Every query involves beginQuery and cleanupQuery, so expect them implicitly.
                .add("ConnectorMetadata.beginQuery", "ConnectorMetadata.cleanupQuery")
                .addAll(expectedMetadataCallsCount)
                .build();

        Multiset<String> actualMetadataCallsCount = countingMockConnector.runTracing(() -> {
            // expectedSql is run on H2, so does not affect counts.
            assertQuery(actualSql, expectedSql);
        });

        assertMultisetsEqual(actualMetadataCallsCount, expectedMetadataCallsCount);
    }
}
