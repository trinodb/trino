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
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.SchemaTableName;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.testing.TestingSession.testSessionBuilder;

public class TestInformationSchemaConnector
        extends AbstractTestQueryFramework
{
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
    public void testNestedPredicate()
    {
        assertQuery("SELECT count(*) FROM (SELECT * FROM tpch.information_schema.tables WHERE table_name IN ('supplier', 'part', 'customer'))u WHERE u.table_name IN ('supplier', 'part', 'orders')", "VALUES 18");
    }

    @Test
    public void testLargeData()
    {
        assertQuery("SELECT count(*) from test_catalog.information_schema.tables", "VALUES 300008");
        assertQuery("SELECT count(*) from test_catalog.information_schema.tables WHERE table_schema = 'test_schema1'", "VALUES 100000");
        assertQuery("SELECT count(*) from test_catalog.information_schema.tables WHERE table_name = 'test_table1'", "VALUES 2");
    }

    private static DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder().build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setNodeCount(4)
                .build();
        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            queryRunner.installPlugin(new Plugin() {
                @Override
                public Iterable<ConnectorFactory> getConnectorFactories()
                {
                    List<SchemaTableName> tablesTestSchema1 = IntStream.range(0, 100000)
                            .mapToObj(i -> new SchemaTableName("test_schema1", "test_table" + i))
                            .collect(toImmutableList());
                    List<SchemaTableName> tablesTestSchema2 = IntStream.range(0, 200000)
                            .mapToObj(i -> new SchemaTableName("test_schema2", "test_table" + i))
                            .collect(toImmutableList());
                    List<SchemaTableName> tables = Stream.concat(tablesTestSchema1.stream(), tablesTestSchema2.stream()).collect(toImmutableList());
                    MockConnectorFactory.Builder builder = MockConnectorFactory.builder();
                    MockConnectorFactory mockConnectorFactory = builder.withListSchemaNames(connectorSession -> ImmutableList.of("test_schema1", "test_schema2"))
                            .withListTables((connectorSession, schemaNameOrNull) -> {
                                if (schemaNameOrNull == null) {
                                    return tables;
                                }
                                else {
                                    return tables.stream().filter(schemaTableName -> schemaTableName.getSchemaName().equals(schemaNameOrNull)).collect(toImmutableList());
                                }
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
}
