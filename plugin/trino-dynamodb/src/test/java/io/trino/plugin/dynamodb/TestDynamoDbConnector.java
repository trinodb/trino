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
package io.trino.plugin.dynamodb;

import com.google.common.collect.ImmutableList;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDynamoDbConnector
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DynamoDbServer server = closeAfterClass(new DynamoDbServer());
        return DynamoDbQueryRunner.builder(server)
                .setInitialTables(ImmutableList.of(TpchTable.NATION, TpchTable.REGION))
                .build();
    }

    @Test
    public void testNationCount()
    {
        assertQuery("SELECT count(*) FROM nation", "SELECT count(*) FROM nation");
    }

    @Test
    public void testRegionCount()
    {
        assertQuery("SELECT count(*) FROM region", "SELECT count(*) FROM region");
    }

    @Test
    public void testSelectNationNames()
    {
        // String columns compare cleanly: DynamoDB returns VARCHAR, H2 returns VARCHAR(25)
        assertThat(computeActual("SELECT name FROM nation").getOnlyColumnAsSet())
                .hasSize(25)
                .contains("ALGERIA", "BRAZIL", "CANADA");
    }

    @Test
    public void testSelectRegionNames()
    {
        assertThat(computeActual("SELECT name FROM region").getOnlyColumnAsSet())
                .containsExactlyInAnyOrder("AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST");
    }

    @Test
    public void testFilterByName()
    {
        assertQuery("SELECT count(*) FROM nation WHERE name = 'FRANCE'",
                "SELECT count(*) FROM nation WHERE name = 'FRANCE'");
        assertQuery("SELECT count(*) FROM nation WHERE name = 'NONEXISTENT'",
                "SELECT count(*) FROM nation WHERE name = 'NONEXISTENT'");
    }

    @Test
    public void testNullHandling()
    {
        assertQuery(
                "SELECT count(*) FROM nation WHERE comment IS NOT NULL",
                "SELECT count(*) FROM nation");
    }

    @Test
    public void testShowSchemas()
    {
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet())
                .contains("default");
    }

    @Test
    public void testShowTables()
    {
        assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet())
                .containsExactlyInAnyOrder("nation", "region");
    }

    @Test
    public void testDescribeNation()
    {
        assertQuerySucceeds("DESCRIBE nation");
    }

    @Test
    public void testAggregation()
    {
        // nationkey is DOUBLE (DynamoDB N type); use explicit result builder for type accuracy
        assertThat(query("SELECT min(nationkey), max(nationkey) FROM nation"))
                .result()
                .matches(resultBuilder(getSession(), DOUBLE, DOUBLE)
                        .row(0.0, 24.0)
                        .build());
    }

    @Test
    public void testProjection()
    {
        // nationkey is DOUBLE; arithmetic on DOUBLE yields DOUBLE
        assertThat(query("SELECT name, nationkey + 100 FROM nation WHERE name = 'FRANCE'"))
                .result()
                .matches(resultBuilder(getSession(), VARCHAR, DOUBLE)
                        .row("FRANCE", 106.0)
                        .build());
    }
}
