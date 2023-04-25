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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.benchmark.BenchmarkSuite;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.spi.security.PrincipalType;
import io.trino.testing.LocalQueryRunner;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.hive.metastore.file.TestingFileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

public final class HiveBenchmarkQueryRunner
{
    private HiveBenchmarkQueryRunner() {}

    public static void main(String[] args)
            throws IOException
    {
        String outputDirectory = requireNonNull(System.getProperty("outputDirectory"), "Must specify -DoutputDirectory=...");
        File tempDir = Files.createTempDirectory(null).toFile();
        try (LocalQueryRunner localQueryRunner = createLocalQueryRunner(tempDir)) {
            new BenchmarkSuite(localQueryRunner, outputDirectory).runAllBenchmarks();
        }
        finally {
            deleteRecursively(tempDir.toPath(), ALLOW_INSECURE);
        }
    }

    public static LocalQueryRunner createLocalQueryRunner(File tempDir)
    {
        Session session = testSessionBuilder()
                .setCatalog("hive")
                .setSchema("tpch")
                .build();

        LocalQueryRunner localQueryRunner = LocalQueryRunner.create(session);

        // add tpch
        localQueryRunner.createCatalog("tpch", new TpchConnectorFactory(1), ImmutableMap.of());

        // add hive
        File hiveDir = new File(tempDir, "hive_data");
        HiveMetastore metastore = createTestingFileHiveMetastore(hiveDir);

        metastore.createDatabase(
                Database.builder()
                        .setDatabaseName("tpch")
                        .setOwnerName(Optional.of("public"))
                        .setOwnerType(Optional.of(PrincipalType.ROLE))
                        .build());

        Map<String, String> hiveCatalogConfig = ImmutableMap.of("hive.max-split-size", "10GB");

        localQueryRunner.createCatalog("hive", new TestingHiveConnectorFactory(metastore), hiveCatalogConfig);

        localQueryRunner.execute("CREATE TABLE orders AS SELECT * FROM tpch.sf1.orders");
        localQueryRunner.execute("CREATE TABLE lineitem AS SELECT * FROM tpch.sf1.lineitem");
        return localQueryRunner;
    }
}
