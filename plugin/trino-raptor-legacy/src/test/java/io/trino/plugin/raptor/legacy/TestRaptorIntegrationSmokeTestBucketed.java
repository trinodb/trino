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
package io.trino.plugin.raptor.legacy;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.testng.annotations.Test;

import static io.trino.plugin.raptor.legacy.RaptorQueryRunner.createRaptorQueryRunner;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRaptorIntegrationSmokeTestBucketed
        extends BaseRaptorConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createRaptorQueryRunner(ImmutableMap.of(), TpchTable.getTables(), true, ImmutableMap.of());
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat(computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .isEqualTo("CREATE TABLE raptor.tpch.orders (\n" +
                        "   orderkey bigint,\n" +
                        "   custkey bigint,\n" +
                        "   orderstatus varchar(1),\n" +
                        "   totalprice double,\n" +
                        "   orderdate date,\n" +
                        "   orderpriority varchar(15),\n" +
                        "   clerk varchar(15),\n" +
                        "   shippriority integer,\n" +
                        "   comment varchar(79)\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   bucket_count = 25,\n" +
                        "   bucketed_on = ARRAY['orderkey'],\n" +
                        "   distribution_name = 'order'\n" +
                        ")");
    }

    @Test
    public void testShardsSystemTableBucketNumber()
    {
        assertQuery("" +
                        "SELECT count(DISTINCT bucket_number)\n" +
                        "FROM system.shards\n" +
                        "WHERE table_schema = 'tpch'\n" +
                        "  AND table_name = 'orders'",
                "SELECT 25");
    }
}
