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

package io.trino.plugin.session;

import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import java.io.File;

import static io.trino.SystemSessionProperties.QUERY_MAX_EXECUTION_TIME;

public class TestSessionPropertyManagerInTransaction
        extends AbstractTestQueryFramework
{
    public static final File CONFIG_FILE = new File("src/test/resources/io/trino/plugin/session/file/session-property-config.properties");

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = HiveQueryRunner.builder()
                .build();
        queryRunner.installPlugin(new SessionPropertyConfigurationManagerPlugin());
        queryRunner.getSessionPropertyDefaults().loadConfigurationManager(CONFIG_FILE.getAbsoluteFile());
        return queryRunner;
    }

    @Test
    public void testSessionPropertiesDefaultsDuringOpenTransaction()
    {
        // Assure session property defaults are applied
        assertQuery(
                "SHOW SESSION LIKE '" + QUERY_MAX_EXECUTION_TIME + "'",
                "VALUES('" + QUERY_MAX_EXECUTION_TIME + "','8h', '100.00d', 'varchar', 'Maximum execution time of a query')");
        // Perform operation in transaction
        newTransaction()
                .execute(getSession(), session -> {
                    getQueryRunner().execute(session, "CREATE SCHEMA test");
                });
        // Ensure that the previous statement was successful
        assertQuery(
                "SHOW SCHEMAS FROM hive",
                "VALUES('information_schema'),('test'),('tpch')");
    }
}
