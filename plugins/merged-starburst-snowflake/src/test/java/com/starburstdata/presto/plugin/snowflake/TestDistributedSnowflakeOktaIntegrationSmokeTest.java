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

import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.spi.security.Identity;
import io.prestosql.testing.QueryRunner;
import org.testng.annotations.Test;

import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.TEST_SCHEMA;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.distributedBuilder;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.oktaImpersonationEnabled;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeServer.OKTA_PASSWORD;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeServer.OKTA_USER;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;

public class TestDistributedSnowflakeOktaIntegrationSmokeTest
        extends BaseSnowflakeIntegrationSmokeTest
{
    public TestDistributedSnowflakeOktaIntegrationSmokeTest()
    {
        this(new SnowflakeServer());
    }

    private TestDistributedSnowflakeOktaIntegrationSmokeTest(SnowflakeServer server)
    {
        super(server, () -> distributedBuilder()
                .withServer(server)
                .withAdditionalProperties(oktaImpersonationEnabled(false))
                .withConnectionPooling()
                .build());
    }

    @Test
    public void testOktaWithoutConnectionPooling()
            throws Exception
    {
        try (QueryRunner queryRunner = distributedBuilder()
                .withAdditionalProperties(oktaImpersonationEnabled(false))
                .build()) {
            Session session = getSession();
            String tableName = "test_insert_" + randomTableSuffix();
            queryRunner.execute(session, format("CREATE TABLE test_schema.%s (x decimal(19, 0), y varchar(100))", tableName));
            queryRunner.execute(session, format("INSERT INTO %s VALUES (123, 'test')", tableName));
            queryRunner.execute(session, format("SELECT * FROM %s", tableName));
            queryRunner.execute(session, format("DROP TABLE test_schema.%s", tableName));
        }
    }

    @Override
    protected Session getSession()
    {
        return testSessionBuilder()
                .setCatalog("snowflake")
                .setSchema(TEST_SCHEMA)
                .setIdentity(Identity.forUser(OKTA_USER)
                        .withExtraCredentials(ImmutableMap.of(
                                "okta.user", OKTA_USER,
                                "okta.password", OKTA_PASSWORD))
                        .build())
                .build();
    }
}
