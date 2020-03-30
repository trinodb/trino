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
package com.starburstdata.presto.plugin.snowflake;

import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.distributedBuilder;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.impersonationDisabled;

public class TestDistributedSnowflakeIntegrationSmokeTest
        extends BaseSnowflakeIntegrationSmokeTest
{
    public TestDistributedSnowflakeIntegrationSmokeTest()
    {
        this(new SnowflakeServer());
    }

    private TestDistributedSnowflakeIntegrationSmokeTest(SnowflakeServer server)
    {
        super(server, () -> distributedBuilder()
                .withServer(server)
                .withAdditionalProperties(impersonationDisabled())
                .withConnectionPooling()
                .build());
    }
}
