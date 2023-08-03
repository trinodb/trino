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

package io.trino.plugin.firebolt;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.util.Map;

import static io.trino.plugin.firebolt.FireboltTestProperties.JDBC_ENDPOINT;
import static io.trino.plugin.firebolt.FireboltTestProperties.JDBC_PASSWORD;
import static io.trino.plugin.firebolt.FireboltTestProperties.JDBC_USER;
import static io.trino.testing.TestingSession.testSessionBuilder;

public final class FireboltQueryRunner
{
    public static final String testTable = "test_table";

    private static final String TPCH_SCHEMA = "public";

    private static final String CONNECTOR_NAME = "firebolt";
    private static final String TEST_DATABASE = "trino_test";
    private static final String TEST_CATALOG = "firebolt";

    static final String JDBC_URL = "jdbc:firebolt://" + JDBC_ENDPOINT + "/" + TEST_DATABASE;

    private FireboltQueryRunner() {}

    public static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog(TEST_CATALOG)
                .setSchema(TPCH_SCHEMA)
                .build();
    }

    public static QueryRunner createQueryRunner()
            throws Exception
    {
        Map<String, String> extraProperties = ImmutableMap.<String, String>builder()
                .put("http-server.http.port", "8080")
                .buildOrThrow();
        QueryRunner queryRunner = DistributedQueryRunner.builder(createSession())
                .setExtraProperties(extraProperties)
                .setNodeCount(1)
                .build();

        queryRunner.installPlugin(new FireboltPlugin());

        Map<String, String> connectorProperties = Map.of(
                "connection-url", JDBC_URL,
                "connection-user", JDBC_USER,
                "connection-password", JDBC_PASSWORD);
        queryRunner.createCatalog(
                TEST_CATALOG,
                CONNECTOR_NAME,
                connectorProperties);

        return queryRunner;
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging logger = Logging.initialize();
        logger.setLevel("io.trino.plugin.firebolt", Level.DEBUG);
        logger.setLevel("io.trino", Level.INFO);

        QueryRunner queryRunner = createQueryRunner();

        Logger log = Logger.get(FireboltQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", ((DistributedQueryRunner) queryRunner).getCoordinator().getBaseUrl());
    }
}
