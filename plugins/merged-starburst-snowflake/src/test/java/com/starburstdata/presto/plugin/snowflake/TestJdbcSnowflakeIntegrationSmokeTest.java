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

import io.prestosql.Session;
import io.prestosql.spi.security.Identity;
import io.prestosql.testing.QueryRunner;
import org.testng.annotations.Test;

import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.impersonationDisabled;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.jdbcBuilder;
import static java.lang.String.format;

public class TestJdbcSnowflakeIntegrationSmokeTest
        extends BaseSnowflakeIntegrationSmokeTest
{
    public TestJdbcSnowflakeIntegrationSmokeTest()
    {
        this(new SnowflakeServer());
    }

    private TestJdbcSnowflakeIntegrationSmokeTest(SnowflakeServer server)
    {
        super(server, () -> jdbcBuilder()
                .withServer(server)
                .withAdditionalProperties(impersonationDisabled())
                .withConnectionPooling()
                .build());
    }

    // currently the JDBC connector cannot read timestamps greater than 9999-12-31 23:59:59.999
    @Override
    @Test
    public void testTimestampWithTimezoneValues()
    {
    }

    @Test
    public void testSnowflakeUseDefaultUserWarehouseAndDatabase()
            throws Exception
    {
        try (QueryRunner queryRunner = jdbcBuilder()
                .withoutWarehouse()
                .withoutDatabase()
                .withAdditionalProperties(impersonationDisabled())
                .build()) {
            Session session = Session.builder(queryRunner.getDefaultSession())
                    .setIdentity(Identity.ofUser(SnowflakeServer.USER))
                    .build();
            String tableName = "test_insert_" + randomTableSuffix();
            queryRunner.execute(session, format("CREATE TABLE test_schema.%s (x decimal(19, 0), y varchar(100))", tableName));
            queryRunner.execute(session, format("INSERT INTO %s VALUES (123, 'test')", tableName));
            queryRunner.execute(session, format("SELECT * FROM %s", tableName));
            queryRunner.execute(session, format("DROP TABLE test_schema.%s", tableName));
        }
    }
}
