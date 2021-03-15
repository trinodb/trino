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
package io.trino.plugin.druid;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.assertions.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

import static io.trino.plugin.druid.BaseDruidIntegrationSmokeTest.SELECT_FROM_ORDERS;
import static io.trino.plugin.druid.BaseDruidIntegrationSmokeTest.SELECT_FROM_REGION;
import static io.trino.plugin.druid.DruidQueryRunner.copyAndIngestTpchData;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestDruidCaseInsensitiveMatch
        extends AbstractTestQueryFramework
{
    private TestingDruidServer druidServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        druidServer = new TestingDruidServer();
        closeAfterClass(() -> {
            druidServer.close();
            druidServer = null;
        });
        DistributedQueryRunner queryRunner = DruidQueryRunner.createDruidQueryRunnerTpch(
                druidServer, ImmutableMap.of(), ImmutableMap.of("case-insensitive-name-matching", "true"));
        copyAndIngestTpchData(queryRunner.execute(SELECT_FROM_ORDERS + " LIMIT 10"), this.druidServer, "orders", "CamelCase");
        return queryRunner;
    }

    @Test
    public void testNonLowerCaseTableName()
    {
        MaterializedResult expectedColumns = MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("__time", "timestamp(3)", "", "")
                .row("clerk", "varchar", "", "") // String columns are reported only as varchar
                .row("comment", "varchar", "", "")
                .row("custkey", "bigint", "", "") // Long columns are reported as bigint
                .row("orderdate", "varchar", "", "")
                .row("orderkey", "bigint", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("shippriority", "bigint", "", "") // Druid doesn't support int type
                .row("totalprice", "double", "", "")
                .build();
        MaterializedResult actualColumns = computeActual("DESCRIBE " + "CamelCase");
        Assert.assertEquals(actualColumns, expectedColumns);
        MaterializedResult materializedRows = computeActual("SELECT * FROM druid.druid.CAMELCASE");
        Assert.assertEquals(materializedRows.getRowCount(), 10);
        MaterializedResult materializedRows1 = computeActual("SELECT * FROM druid.CamelCase");
        MaterializedResult materializedRows2 = computeActual("SELECT * FROM druid.camelcase");
        assertThat(materializedRows.equals(materializedRows1));
        assertThat(materializedRows.equals(materializedRows2));
    }

    @Test
    public void testTableNameClash()
            throws IOException, InterruptedException
    {
        try {
            //ingesting data with already existing table name in lowercase which should fail
            copyAndIngestTpchData(getQueryRunner().execute(SELECT_FROM_REGION + " LIMIT 10"), this.druidServer, "region", "camelcase");
        }
        catch (AssertionError e) {
            Assert.assertEquals(e.getMessage(), "Datasource camelcase not loaded expected [true] but found [false]");
        }
    }
}
