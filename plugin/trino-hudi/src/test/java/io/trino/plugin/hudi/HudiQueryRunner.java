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
package io.trino.plugin.hudi;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.filesystem.Location;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hudi.testing.HudiTablesInitializer;
import io.trino.plugin.hudi.testing.ResourceHudiTablesInitializer;
import io.trino.spi.security.PrincipalType;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.util.Map;
import java.util.Optional;

import static io.trino.testing.TestingSession.testSessionBuilder;

public final class HudiQueryRunner
{
    static {
        Logging logging = Logging.initialize();
        logging.setLevel("org.apache.hudi", Level.OFF);
    }

    private static final String SCHEMA_NAME = "tests";

    private HudiQueryRunner() {}

    public static QueryRunner createHudiQueryRunner(
            Map<String, String> extraProperties,
            Map<String, String> connectorProperties,
            HudiTablesInitializer dataLoader)
            throws Exception
    {
        QueryRunner queryRunner = DistributedQueryRunner
                .builder(createSession())
                .setExtraProperties(extraProperties)
                .build();

        queryRunner.installPlugin(new TestingHudiPlugin(queryRunner.getCoordinator().getBaseDataDir().resolve("hudi_data")));
        queryRunner.createCatalog("hudi", "hudi", connectorProperties);

        // Hudi connector does not support creating schema or any other write operations
        ((HudiConnector) queryRunner.getCoordinator().getConnector("hudi")).getInjector()
                .getInstance(HiveMetastoreFactory.class)
                .createMetastore(Optional.empty())
                .createDatabase(Database.builder()
                        .setDatabaseName(SCHEMA_NAME)
                        .setOwnerName(Optional.of("public"))
                        .setOwnerType(Optional.of(PrincipalType.ROLE))
                        .build());

        dataLoader.initializeTables(queryRunner, Location.of("local:///"), SCHEMA_NAME);
        return queryRunner;
    }

    private static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("hudi")
                .setSchema(SCHEMA_NAME)
                .build();
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        Logger log = Logger.get(HudiQueryRunner.class);

        QueryRunner queryRunner = createHudiQueryRunner(
                ImmutableMap.of("http-server.http.port", "8080"),
                ImmutableMap.of(),
                new ResourceHudiTablesInitializer());

        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
