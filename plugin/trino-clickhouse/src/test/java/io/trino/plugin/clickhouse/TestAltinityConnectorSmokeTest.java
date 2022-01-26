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
import io.airlift.log.Logger;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import static io.trino.plugin.clickhouse.ClickHouseQueryRunner.createClickHouseQueryRunner;
import static io.trino.plugin.clickhouse.TestingClickHouseServer.ALTINITY_DEFAULT_IMAGE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestAltinityConnectorSmokeTest
        extends BaseClickHouseConnectorSmokeTest
{
    private static final Logger log = Logger.get(TestAltinityConnectorSmokeTest.class);
    private TestingClickHouseServer clickHouseServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        clickHouseServer = closeAfterClass(new TestingClickHouseServer(ALTINITY_DEFAULT_IMAGE));
        return createClickHouseQueryRunner(
                clickHouseServer,
                ImmutableMap.of(),
                ImmutableMap.<String, String>builder()
                        .put("clickhouse.map-string-as-varchar", "true") // To handle string types in TPCH tables as varchar instead of varbinary
                        .buildOrThrow(),
                REQUIRED_TPCH_TABLES);
    }

    @Test
    @Override
    public void testRenameSchema()
    {
        // Override because RENAME DATABASE statement isn't supported in version < v20.7.2.30-stable
        assertThatThrownBy(super::testRenameSchema)
                .hasMessageMatching("ClickHouse exception, code: 62,.* Syntax error.*\\n");
    }
}
