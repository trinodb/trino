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
package io.prestosql.plugin.accumulo;

import com.google.common.collect.ImmutableMap;
import io.prestosql.testing.AbstractTestIntegrationSmokeTest;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.QueryRunner;
import org.testng.annotations.Test;

import static io.prestosql.plugin.accumulo.AccumuloQueryRunner.createAccumuloQueryRunner;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.MaterializedResult.resultBuilder;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAccumuloIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createAccumuloQueryRunner(ImmutableMap.of());
    }

    @Test
    @Override
    public void testDescribeTable()
    {
        MaterializedResult expectedColumns = resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "Accumulo row ID")
                .row("custkey", "bigint", "", "Accumulo column custkey:custkey. Indexed: false")
                .row("orderstatus", "varchar(1)", "", "Accumulo column orderstatus:orderstatus. Indexed: false")
                .row("totalprice", "double", "", "Accumulo column totalprice:totalprice. Indexed: false")
                .row("orderdate", "date", "", "Accumulo column orderdate:orderdate. Indexed: true")
                .row("orderpriority", "varchar(15)", "", "Accumulo column orderpriority:orderpriority. Indexed: false")
                .row("clerk", "varchar(15)", "", "Accumulo column clerk:clerk. Indexed: false")
                .row("shippriority", "integer", "", "Accumulo column shippriority:shippriority. Indexed: false")
                .row("comment", "varchar(79)", "", "Accumulo column comment:comment. Indexed: false")
                .build();
        MaterializedResult actualColumns = computeActual("DESCRIBE orders");
        assertEquals(actualColumns, expectedColumns);
    }

    @Override
    public void testShowCreateTable()
    {
        assertThat(computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .isEqualTo("CREATE TABLE accumulo.tpch.orders (\n" +
                        "   orderkey bigint COMMENT 'Accumulo row ID',\n" +
                        "   custkey bigint COMMENT 'Accumulo column custkey:custkey. Indexed: false',\n" +
                        "   orderstatus varchar(1) COMMENT 'Accumulo column orderstatus:orderstatus. Indexed: false',\n" +
                        "   totalprice double COMMENT 'Accumulo column totalprice:totalprice. Indexed: false',\n" +
                        "   orderdate date COMMENT 'Accumulo column orderdate:orderdate. Indexed: true',\n" +
                        "   orderpriority varchar(15) COMMENT 'Accumulo column orderpriority:orderpriority. Indexed: false',\n" +
                        "   clerk varchar(15) COMMENT 'Accumulo column clerk:clerk. Indexed: false',\n" +
                        "   shippriority integer COMMENT 'Accumulo column shippriority:shippriority. Indexed: false',\n" +
                        "   comment varchar(79) COMMENT 'Accumulo column comment:comment. Indexed: false'\n" +
                        ")");
    }
}
