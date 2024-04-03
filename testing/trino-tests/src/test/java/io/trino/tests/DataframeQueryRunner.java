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
package io.trino.tests;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.plugin.teradata.functions.TeradataFunctionsPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.security.PrincipalType;
import io.trino.testing.DistributedQueryRunner;
import io.trino.tpch.TpchTable;

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.hive.metastore.file.TestingFileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.NATION;
import static java.lang.String.format;

public class DataframeQueryRunner
{
    protected static final List<TpchTable<?>> REQUIRED_TPCH_TABLES = ImmutableList.of(CUSTOMER, NATION);

    private DataframeQueryRunner() {}

    private static void runTestServer(Map<String, String> coordinatorProperties)
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(getDefaultSession())
                .addExtraProperty("dataframe-api-enabled", "true")
                .setCoordinatorProperties(ImmutableMap.copyOf(coordinatorProperties))
                .build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of());

        queryRunner.installPlugin(new MemoryPlugin());
        queryRunner.createCatalog("memory", "memory", ImmutableMap.of());

        queryRunner.installPlugin(new TeradataFunctionsPlugin());

        File baseDir = queryRunner.getCoordinator().getBaseDataDir().resolve("hive_data").toFile();
        HiveMetastore metastore = createTestingFileHiveMetastore(baseDir);
        metastore.createDatabase(
                Database.builder()
                        .setDatabaseName("default")
                        .setOwnerName(Optional.of("public"))
                        .setOwnerType(Optional.of(PrincipalType.ROLE))
                        .build());

        queryRunner.installPlugin(new TestingHivePlugin(baseDir.toPath(), metastore));
        ImmutableMap<String, String> hiveProperties = ImmutableMap.<String, String>builder().buildOrThrow();
        queryRunner.createCatalog("hive", "hive", hiveProperties);
        queryRunner.execute("CREATE SCHEMA hive.tpch");

        for (TpchTable<?> table : REQUIRED_TPCH_TABLES) {
            queryRunner.execute(
                    format("CREATE TABLE hive.tpch.%s AS SELECT * FROM tpch.tiny.%s",
                            table.getTableName(),
                            table.getTableName()));
        }

        queryRunner.installPlugin(new IcebergPlugin());
        Map<String, String> icebergProperties = new HashMap<>(ImmutableMap.<String, String>builder().buildOrThrow());
        String catalogType = icebergProperties.get("iceberg.catalog.type");
        Optional<File> metastoreDirectory = Optional.empty();
        Path dataDir = metastoreDirectory.map(File::toPath).orElseGet(() -> queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data"));
        if (catalogType == null) {
            icebergProperties.put("iceberg.catalog.type", "TESTING_FILE_METASTORE");
            icebergProperties.put("hive.metastore.catalog.dir", dataDir.toString());
        }

        queryRunner.createCatalog("iceberg", "iceberg", icebergProperties);
        queryRunner.execute("CREATE SCHEMA iceberg.default");
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        runTestServer(ImmutableMap.of("http-server.http.port", "8080"));

        Logger log = Logger.get(DataframeQueryRunner.class);
        log.info("======== SERVER STARTED ========");
    }

    private static Session getDefaultSession()
    {
        return testSessionBuilder()
                .setCatalog("test")
                .setSchema("tpch")
                .build();
    }
}
