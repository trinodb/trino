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
import org.junit.jupiter.api.Test;

import java.time.ZoneId;

import static io.trino.plugin.clickhouse.TestingClickHouseServer.CLICKHOUSE_LATEST_IMAGE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestClickHouseLatestTypeMapping
        extends BaseClickHouseTypeMapping
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        clickhouseServer = closeAfterClass(new TestingClickHouseServer(CLICKHOUSE_LATEST_IMAGE));
        return ClickHouseQueryRunner.builder(clickhouseServer).build();
    }

    @Test
    public void testClickHouseDateTime64WithServerTimeZone()
            throws Exception
    {
        try (TestingClickHouseServer server = new TestingClickHouseServer(CLICKHOUSE_LATEST_IMAGE, ZoneId.of("Asia/Kathmandu"));
                QueryRunner queryRunner = ClickHouseQueryRunner.builder(server)
                        .addConnectorProperty("clickhouse.map-string-as-varchar", "true")
                        .build()) {
            assertThat(queryRunner.execute("SELECT * FROM TABLE(system.query(query => 'SELECT timezone()'))").getOnlyValue())
                    .isEqualTo("Asia/Kathmandu");
            testClickHouseDateTime64(queryRunner, server::execute, ZoneId.of("Asia/Kathmandu"));
        }
    }
}
