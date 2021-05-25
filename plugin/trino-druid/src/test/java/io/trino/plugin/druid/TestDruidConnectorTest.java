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
import io.trino.testing.QueryRunner;

import static io.trino.plugin.druid.DruidQueryRunner.copyAndIngestTpchData;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.PART;
import static io.trino.tpch.TpchTable.REGION;

public class TestDruidConnectorTest
        extends BaseDruidConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.druidServer = new TestingDruidServer();
        QueryRunner runner = DruidQueryRunner.createDruidQueryRunnerTpch(druidServer, ImmutableMap.of());
        copyAndIngestTpchData(runner.execute(SELECT_SINGLE_ROW), this.druidServer, "singlerow");

        // there is no create API for datasource, we just have to ingest and remove the data.
        copyAndIngestTpchData(runner.execute(SELECT_SINGLE_ROW), this.druidServer, "nodata");
        this.druidServer.dropAllSegements("nodata");
        copyAndIngestTpchData(runner.execute(SELECT_FROM_ORDERS), this.druidServer, ORDERS.getTableName());
        copyAndIngestTpchData(runner.execute(SELECT_FROM_LINEITEM), this.druidServer, LINE_ITEM.getTableName());
        copyAndIngestTpchData(runner.execute(SELECT_FROM_NATION), this.druidServer, NATION.getTableName());
        copyAndIngestTpchData(runner.execute(SELECT_FROM_REGION), this.druidServer, REGION.getTableName());
        copyAndIngestTpchData(runner.execute(SELECT_FROM_PART), this.druidServer, PART.getTableName());
        copyAndIngestTpchData(runner.execute(SELECT_FROM_CUSTOMER), this.druidServer, CUSTOMER.getTableName());
        return runner;
    }
}
