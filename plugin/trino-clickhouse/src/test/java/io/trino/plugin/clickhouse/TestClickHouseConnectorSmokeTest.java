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

import com.google.common.collect.ImmutableMap;
import io.trino.testing.QueryRunner;

import static io.trino.plugin.clickhouse.ClickHouseQueryRunner.createClickHouseQueryRunner;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestClickHouseConnectorSmokeTest
        extends BaseClickHouseConnectorSmokeTest
{
    protected TestingClickHouseServer clickHouseServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        clickHouseServer = closeAfterClass(new TestingClickHouseServer());
        return createClickHouseQueryRunner(
                clickHouseServer,
                ImmutableMap.of(),
                ImmutableMap.<String, String>builder()
                        .put("clickhouse.map-string-as-varchar", "true") // To handle string types in TPCH tables as varchar instead of varbinary
                        .buildOrThrow(),
                REQUIRED_TPCH_TABLES);
    }

    @Override
    public void testRenameSchema()
    {
        // Override because the default database engine in version < v20.10.2.20-stable doesn't allow renaming schemas
        assertThatThrownBy(super::testRenameSchema)
                .hasMessageMatching("ClickHouse exception, code: 48,.* Ordinary: RENAME DATABASE is not supported .*\\n");

        String schemaName = "test_rename_schema_" + randomTableSuffix();
        try {
            clickHouseServer.execute("CREATE DATABASE " + schemaName + " ENGINE = Atomic");
            assertUpdate("ALTER SCHEMA " + schemaName + " RENAME TO " + schemaName + "_renamed");
        }
        finally {
            assertUpdate("DROP SCHEMA IF EXISTS " + schemaName);
            assertUpdate("DROP SCHEMA IF EXISTS " + schemaName + "_renamed");
        }
    }
}
