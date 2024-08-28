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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.nio.file.Path;

import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestSharedHiveMetastore
        extends BaseSharedMetastoreTest
{
    private static final String HIVE_CATALOG = "hive";
    private Path dataDirectory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session icebergSession = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema(tpchSchema)
                .build();
        Session hiveSession = testSessionBuilder()
                .setCatalog(HIVE_CATALOG)
                .setSchema(tpchSchema)
                .build();

        QueryRunner queryRunner = DistributedQueryRunner.builder(icebergSession).build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        this.dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data");
        this.dataDirectory.toFile().deleteOnExit();

        queryRunner.installPlugin(new IcebergPlugin());
        queryRunner.createCatalog(
                ICEBERG_CATALOG,
                "iceberg",
                ImmutableMap.of(
                        "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                        "hive.metastore.catalog.dir", dataDirectory.toString(),
                        "fs.hadoop.enabled", "true"));
        queryRunner.createCatalog(
                "iceberg_with_redirections",
                "iceberg",
                ImmutableMap.of(
                        "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                        "hive.metastore.catalog.dir", dataDirectory.toString(),
                        "iceberg.hive-catalog-name", "hive",
                        "fs.hadoop.enabled", "true"));

        queryRunner.installPlugin(new TestingHivePlugin(dataDirectory));
        queryRunner.createCatalog(HIVE_CATALOG, "hive");
        queryRunner.createCatalog(
                "hive_with_redirections",
                "hive",
                ImmutableMap.of("hive.iceberg-catalog-name", "iceberg"));

        queryRunner.execute("CREATE SCHEMA " + tpchSchema);
        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, icebergSession, ImmutableList.of(TpchTable.NATION));
        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, hiveSession, ImmutableList.of(TpchTable.REGION));
        queryRunner.execute("CREATE SCHEMA " + testSchema);

        return queryRunner;
    }

    @AfterAll
    public void cleanup()
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS hive." + tpchSchema + ".region");
        assertQuerySucceeds("DROP TABLE IF EXISTS iceberg." + tpchSchema + ".nation");
        assertQuerySucceeds("DROP SCHEMA IF EXISTS hive." + tpchSchema);
        assertQuerySucceeds("DROP SCHEMA IF EXISTS hive." + testSchema);
    }

    @Override
    protected String getExpectedHiveCreateSchema(String catalogName)
    {
        return """
               CREATE SCHEMA %s.%s
               WITH (
                  location = 'local:///%s'
               )"""
                .formatted(catalogName, tpchSchema, tpchSchema);
    }

    @Override
    protected String getExpectedIcebergCreateSchema(String catalogName)
    {
        String expectedIcebergCreateSchema = "CREATE SCHEMA %s.%s\n" +
                "AUTHORIZATION USER user\n" +
                "WITH (\n" +
                "   location = '%s/%s'\n" +
                ")";
        return format(expectedIcebergCreateSchema, catalogName, tpchSchema, dataDirectory, tpchSchema);
    }
}
