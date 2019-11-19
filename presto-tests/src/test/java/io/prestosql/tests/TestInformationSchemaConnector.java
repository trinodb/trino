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
package io.prestosql.tests;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.connector.MockConnectorFactory;
import io.prestosql.connector.informationschema.InformationSchemaTable;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.DistributedQueryRunner;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.connector.MockConnectorFactory.Builder.defaultGetColumns;
import static io.prestosql.connector.informationschema.InformationSchemaTable.INFORMATION_SCHEMA;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestInformationSchemaConnector
        extends AbstractTestQueryFramework
{
    private static final AtomicLong LIST_SCHEMAS_CALLS_COUNTER = new AtomicLong();
    private static final AtomicLong LIST_TABLES_CALLS_COUNTER = new AtomicLong();
    private static final AtomicLong GET_COLUMNS_CALLS_COUNTER = new AtomicLong();

    public TestInformationSchemaConnector()
    {
        super(TestInformationSchemaConnector::createQueryRunner);
    }

    @Test
    public void testBasic()
    {
        assertQuery("SELECT count(*) FROM tpch.information_schema.schemata", "VALUES 10");
        assertQuery("SELECT count(*) FROM tpch.information_schema.tables", "VALUES 80");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns", "VALUES 585");
        assertQuery("SELECT * FROM tpch.information_schema.schemata ORDER BY 1 DESC, 2 DESC LIMIT 1", "VALUES ('tpch', 'tiny')");
        assertQuery("SELECT * FROM tpch.information_schema.tables ORDER BY 1 DESC, 2 DESC, 3 DESC, 4 DESC LIMIT 1", "VALUES ('tpch', 'tiny', 'supplier', 'BASE TABLE')");
        assertQuery("SELECT * FROM tpch.information_schema.columns ORDER BY 1 DESC, 2 DESC, 3 DESC, 4 DESC LIMIT 1", "VALUES ('tpch', 'tiny', 'supplier', 'suppkey', 1, NULL, 'YES', 'bigint', NULL, NULL)");
        assertQuery("SELECT count(*) FROM test_catalog.information_schema.columns", "VALUES 3000800");
    }

    @Test
    public void testSchemaNamePredicate()
    {
        assertQuery("SELECT count(*) FROM tpch.information_schema.schemata WHERE schema_name = 'sf1'", "VALUES 1");
        assertQuery("SELECT count(*) FROM tpch.information_schema.tables WHERE table_schema = 'sf1'", "VALUES 8");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns WHERE table_schema = 'sf1'", "VALUES 61");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns WHERE table_schema = 'information_schema'", "VALUES 36");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns WHERE table_schema > 'sf100'", "VALUES 427");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns WHERE table_schema != 'sf100'", "VALUES 524");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns WHERE table_schema LIKE 'sf100'", "VALUES 61");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns WHERE table_schema LIKE 'sf%'", "VALUES 488");
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
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns WHERE table_name = 'orders'", "VALUES 81");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns WHERE table_name LIKE 'orders'", "VALUES 81");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns WHERE table_name < 'orders'", "VALUES 267");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns WHERE table_name LIKE 'part'", "VALUES 81");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns WHERE table_name LIKE 'part%'", "VALUES 126");
    }

    @Test
    public void testMixedPredicate()
    {
        assertQuery("SELECT * FROM tpch.information_schema.tables WHERE table_schema = 'sf1' and table_name = 'orders'", "VALUES ('tpch', 'sf1', 'orders', 'BASE TABLE')");
        assertQuery("SELECT count(*) FROM tpch.information_schema.columns WHERE table_schema = 'sf1' and table_name = 'orders'", "VALUES 9");
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
        assertQuery("SELECT count(*) FROM (SELECT * FROM test_catalog.information_schema.tables LIMIT 10000)", "VALUES 10000");
    }

    @Test(timeOut = 60_000)
    public void testMetadataCalls()
    {
        assertMetadataCalls(
                "SELECT count(*) from test_catalog.information_schema.schemata WHERE schema_name LIKE 'test_sch_ma1'",
                "VALUES 1",
                new MetadataCallsCount()
                        .withListSchemasCount(1));
        assertMetadataCalls(
                "SELECT count(*) from test_catalog.information_schema.schemata WHERE schema_name LIKE 'test_sch_ma1' AND schema_name IN ('test_schema1', 'test_schema2')",
                "VALUES 1",
                new MetadataCallsCount()
                        .withListSchemasCount(1));
        assertMetadataCalls(
                "SELECT count(*) from test_catalog.information_schema.tables",
                "VALUES 30008",
                new MetadataCallsCount()
                        .withListSchemasCount(1)
                        .withListTablesCount(2));
        assertMetadataCalls(
                "SELECT count(*) from test_catalog.information_schema.tables WHERE table_schema = 'test_schema1'",
                "VALUES 10000",
                new MetadataCallsCount()
                        .withListTablesCount(1));
        assertMetadataCalls(
                "SELECT count(*) from test_catalog.information_schema.tables WHERE table_schema LIKE 'test_sch_ma1'",
                "VALUES 10000",
                new MetadataCallsCount()
                        .withListSchemasCount(1)
                        .withListTablesCount(1));
        assertMetadataCalls(
                "SELECT count(*) from test_catalog.information_schema.tables WHERE table_schema LIKE 'test_sch_ma1' AND table_schema IN ('test_schema1', 'test_schema2')",
                "VALUES 10000",
                new MetadataCallsCount()
                        .withListTablesCount(2));
        assertMetadataCalls(
                "SELECT count(*) from test_catalog.information_schema.tables WHERE table_name = 'test_table1'",
                "VALUES 2",
                new MetadataCallsCount()
                        .withListSchemasCount(3));
        assertMetadataCalls(
                "SELECT count(*) from test_catalog.information_schema.tables WHERE table_name LIKE 'test_t_ble1'",
                "VALUES 2",
                new MetadataCallsCount()
                        .withListSchemasCount(1)
                        .withListTablesCount(2));
        assertMetadataCalls(
                "SELECT count(*) from test_catalog.information_schema.tables WHERE table_name LIKE 'test_t_ble1' AND table_name IN ('test_table1', 'test_table2')",
                "VALUES 2",
                new MetadataCallsCount()
                        .withListSchemasCount(1));
        assertMetadataCalls(
                "SELECT count(*) from test_catalog.information_schema.columns WHERE table_schema = 'test_schema1' AND table_name = 'test_table1'",
                "VALUES 100",
                new MetadataCallsCount()
                        .withListTablesCount(1)
                        .withGetColumnsCount(1));
        assertNoMetadataCalls("SELECT count(*) from test_catalog.information_schema.columns WHERE table_catalog = 'wrong'", "VALUES 0");
        assertMetadataCalls(
                "SELECT count(*) from test_catalog.information_schema.columns WHERE table_catalog = 'test_catalog' AND table_schema = 'table_schema1' AND table_name = 'test_table1'",
                "VALUES 0",
                new MetadataCallsCount()
                        .withListTablesCount(1));
        assertMetadataCalls(
                "SELECT count(*) from test_catalog.information_schema.columns WHERE table_catalog IN ('wrong', 'test_catalog') AND table_schema = 'table_schema1' AND table_name = 'test_table1'",
                "VALUES 0",
                new MetadataCallsCount()
                        .withListTablesCount(1));
        assertMetadataCalls(
                "SELECT count(*) FROM (SELECT * from test_catalog.information_schema.columns LIMIT 1)",
                "VALUES 1",
                new MetadataCallsCount()
                        .withListSchemasCount(1)
                        .withListTablesCount(0)
                        .withGetColumnsCount(8));
        assertMetadataCalls(
                "SELECT count(*) FROM (SELECT * from test_catalog.information_schema.columns LIMIT 10000)",
                "VALUES 10000",
                new MetadataCallsCount()
                        .withListSchemasCount(1)
                        .withListTablesCount(1)
                        .withGetColumnsCount(10008));
    }

    private static DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder().build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setNodeCount(1)
                .build();
        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            queryRunner.installPlugin(new Plugin()
            {
                @Override
                public Iterable<ConnectorFactory> getConnectorFactories()
                {
                    List<SchemaTableName> tablesTestSchema1 = IntStream.range(0, 10000)
                            .mapToObj(i -> new SchemaTableName("test_schema1", "test_table" + i))
                            .collect(toImmutableList());
                    List<SchemaTableName> tablesTestSchema2 = IntStream.range(0, 20000)
                            .mapToObj(i -> new SchemaTableName("test_schema2", "test_table" + i))
                            .collect(toImmutableList());
                    List<SchemaTableName> tablesInformationSchema = Arrays.stream(InformationSchemaTable.values())
                            .map(InformationSchemaTable::getSchemaTableName)
                            .collect(toImmutableList());
                    MockConnectorFactory mockConnectorFactory = MockConnectorFactory.builder()
                            .withListSchemaNames(connectorSession -> {
                                LIST_SCHEMAS_CALLS_COUNTER.incrementAndGet();
                                return ImmutableList.of("test_schema1", "test_schema2");
                            })
                            .withListTables((connectorSession, schemaName) -> {
                                if (schemaName.equals(INFORMATION_SCHEMA)) {
                                    return tablesInformationSchema;
                                }
                                LIST_TABLES_CALLS_COUNTER.incrementAndGet();
                                if (schemaName.equals("test_schema1")) {
                                    return tablesTestSchema1;
                                }
                                if (schemaName.equals("test_schema2")) {
                                    return tablesTestSchema2;
                                }
                                return ImmutableList.of();
                            })
                            .withGetColumns(schemaTableName -> {
                                GET_COLUMNS_CALLS_COUNTER.incrementAndGet();
                                return defaultGetColumns().apply(schemaTableName);
                            })
                            .build();
                    return ImmutableList.of(mockConnectorFactory);
                }
            });
            queryRunner.createCatalog("test_catalog", "mock", ImmutableMap.of());
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }

    private void assertNoMetadataCalls(String actualSql, String expectedSql)
    {
        assertMetadataCalls(actualSql, expectedSql, new MetadataCallsCount());
    }

    private void assertMetadataCalls(String actualSql, String expectedSql, MetadataCallsCount expectedMetadataCallsCount)
    {
        long listSchemasCallsCountBefore = LIST_SCHEMAS_CALLS_COUNTER.get();
        long listTablesCallsCountBefore = LIST_TABLES_CALLS_COUNTER.get();
        long getColumnsCallsCountBefore = GET_COLUMNS_CALLS_COUNTER.get();
        assertQuery(actualSql, expectedSql);
        MetadataCallsCount actualMetadataCallsCount = new MetadataCallsCount()
                .withListSchemasCount(LIST_SCHEMAS_CALLS_COUNTER.get() - listSchemasCallsCountBefore)
                .withListTablesCount(LIST_TABLES_CALLS_COUNTER.get() - listTablesCallsCountBefore)
                .withGetColumnsCount(GET_COLUMNS_CALLS_COUNTER.get() - getColumnsCallsCountBefore);

        assertEquals(actualMetadataCallsCount, expectedMetadataCallsCount);
    }

    private static class MetadataCallsCount
    {
        private final long listSchemasCount;
        private final long listTablesCount;
        private final long getColumnsCount;

        private MetadataCallsCount()
        {
            this(0, 0, 0);
        }

        private MetadataCallsCount(long listSchemasCount, long listTablesCount, long getColumnsCount)
        {
            this.listSchemasCount = listSchemasCount;
            this.listTablesCount = listTablesCount;
            this.getColumnsCount = getColumnsCount;
        }

        public MetadataCallsCount withListSchemasCount(long listSchemasCount)
        {
            return new MetadataCallsCount(listSchemasCount, listTablesCount, getColumnsCount);
        }

        public MetadataCallsCount withListTablesCount(long listTablesCount)
        {
            return new MetadataCallsCount(listSchemasCount, listTablesCount, getColumnsCount);
        }

        public MetadataCallsCount withGetColumnsCount(long getColumnsCount)
        {
            return new MetadataCallsCount(listSchemasCount, listTablesCount, getColumnsCount);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MetadataCallsCount that = (MetadataCallsCount) o;
            return listSchemasCount == that.listSchemasCount &&
                    listTablesCount == that.listTablesCount &&
                    getColumnsCount == that.getColumnsCount;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(listSchemasCount, listTablesCount, getColumnsCount);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("listSchemasCount", listSchemasCount)
                    .add("listTablesCount", listTablesCount)
                    .add("getColumnsCount", getColumnsCount)
                    .toString();
        }
    }
}
