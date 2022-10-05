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
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.plugin.hive.SchemaAlreadyExistsException;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hudi.testing.HudiTablesInitializer;
import io.trino.plugin.hudi.testing.ResourceHudiTablesInitializer;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.security.PrincipalType;
import io.trino.testing.DistributedQueryRunner;

import java.io.File;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static io.trino.hadoop.ConfigurationInstantiator.newEmptyConfiguration;
import static io.trino.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.testing.TestingSession.testSessionBuilder;

public final class HudiQueryRunner
{
    public static final CatalogSchemaName HUDI_TESTS = new CatalogSchemaName("hudi", "tests");

    private HudiQueryRunner() {}

    public static DistributedQueryRunner createHudiQueryRunner(
            Map<String, String> serverConfig,
            Map<String, String> connectorConfig,
            HudiTablesInitializer dataLoader)
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(HUDI_TESTS.getCatalogName())
                .setSchema(HUDI_TESTS.getSchemaName())
                .build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner
                .builder(session)
                .setExtraProperties(serverConfig)
                .build();

        Path coordinatorBaseDir = queryRunner.getCoordinator().getBaseDataDir();
        File catalogDir = coordinatorBaseDir.resolve("catalog").toFile();
        HiveMetastore metastore = createTestingFileHiveMetastore(catalogDir);

        // create testing database
        Database database = Database.builder()
                .setDatabaseName(HUDI_TESTS.getSchemaName())
                .setOwnerName(Optional.of("public"))
                .setOwnerType(Optional.of(PrincipalType.ROLE))
                .build();
        try {
            metastore.createDatabase(database);
        }
        catch (SchemaAlreadyExistsException e) {
            // do nothing if database already exists
        }

        queryRunner.installPlugin(new TestingHudiPlugin(Optional.of(metastore)));
        queryRunner.createCatalog(
                "hudi",
                HUDI_TESTS.getCatalogName(),
                connectorConfig);

        String dataDir = coordinatorBaseDir.resolve("data").toString();
        dataLoader.initializeTables(queryRunner, metastore, HUDI_TESTS, dataDir, newEmptyConfiguration());
        return queryRunner;
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        Logger log = Logger.get(HudiQueryRunner.class);

        DistributedQueryRunner queryRunner = createHudiQueryRunner(
                ImmutableMap.of("http-server.http.port", "8080"),
                ImmutableMap.of(),
                new ResourceHudiTablesInitializer());

        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
