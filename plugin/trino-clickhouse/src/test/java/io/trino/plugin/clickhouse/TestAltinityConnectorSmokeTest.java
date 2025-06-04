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
package io.trino.plugin.clickhouse;

import io.trino.testing.QueryRunner;

import static io.trino.plugin.clickhouse.TestingClickHouseServer.ALTINITY_DEFAULT_IMAGE;

public class TestAltinityConnectorSmokeTest
        extends BaseClickHouseConnectorSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return ClickHouseQueryRunner.builder(closeAfterClass(new TestingClickHouseServer(ALTINITY_DEFAULT_IMAGE)))
                .addConnectorProperty("clickhouse.map-string-as-varchar", "true") // To handle string types in TPCH tables as varchar instead of varbinary
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }
}
