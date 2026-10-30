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
package io.trino.plugin.hive.metastore.lakehouse;

import io.trino.metastore.lakehouse.LakehouseHiveMetastore;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.hive.metastore.lakehouse.TestingLakehouseCatalog.PARTITIONED_TABLE;
import static io.trino.plugin.hive.metastore.lakehouse.TestingLakehouseCatalog.SCHEMA;
import static io.trino.plugin.hive.metastore.lakehouse.TestingLakehouseCatalog.UNPARTITIONED_TABLE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Runs the discovery and read workflows of the Hive connector against a fixed Lakehouse catalog.
 *
 * <p>The metadata comes from {@link TestingLakehouseCatalog} and the data from local files, so the
 * whole path from a query to a row is covered without reaching Google Cloud.
 */
public class TestLakehouseHiveMetastoreQueries
        extends AbstractTestQueryFramework
{
    private static final String CATALOG_NAME = "lakehouse";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = DistributedQueryRunner.builder(testSessionBuilder()
                        .setCatalog(CATALOG_NAME)
                        .setSchema(SCHEMA)
                        .build())
                .build();

        Path dataRoot = queryRunner.getCoordinator().getBaseDataDir().resolve("lakehouse_data");
        writeFixtures(dataRoot);

        TestingLakehouseCatalog catalog = new TestingLakehouseCatalog();
        LakehouseHiveMetastore metastore = new LakehouseHiveMetastore(() -> catalog);

        queryRunner.installPlugin(new TestingHivePlugin(dataRoot, Optional.of(metastore), false, Optional.empty()));
        queryRunner.createCatalog(CATALOG_NAME, "hive", Map.of());
        return queryRunner;
    }

    /**
     * Writes the rows the fixed catalog points at. The columns of a text table are separated by the
     * default delimiter of the Hive text format.
     */
    private static void writeFixtures(Path dataRoot)
            throws IOException
    {
        writeRows(dataRoot.resolve(UNPARTITIONED_TABLE), "1\u0001keyboard\n2\u0001monitor\n");
        writeRows(dataRoot.resolve(PARTITIONED_TABLE).resolve("region=emea"), "3\u0001desk\n");
        writeRows(dataRoot.resolve(PARTITIONED_TABLE).resolve("region=apac"), "4\u0001lamp\n");
    }

    private static void writeRows(Path directory, String rows)
            throws IOException
    {
        Files.createDirectories(directory);
        Files.writeString(directory.resolve("data.txt"), rows, UTF_8);
    }

    @Test
    public void testShowSchemas()
    {
        assertQuery("SHOW SCHEMAS", "VALUES 'information_schema', '" + SCHEMA + "'");
    }

    @Test
    public void testShowTables()
    {
        assertQuery("SHOW TABLES", "VALUES '" + UNPARTITIONED_TABLE + "', '" + PARTITIONED_TABLE + "'");
    }

    @Test
    public void testDescribeTable()
    {
        assertQuery(
                "DESCRIBE " + UNPARTITIONED_TABLE,
                "VALUES ('order_id', 'bigint', '', ''), ('item', 'varchar', '', '')");

        assertQuery(
                "DESCRIBE " + PARTITIONED_TABLE,
                "VALUES ('order_id', 'bigint', '', ''), ('item', 'varchar', '', ''), ('region', 'varchar', 'partition key', '')");
    }

    @Test
    public void testSelect()
    {
        assertQuery(
                "SELECT order_id, item FROM " + UNPARTITIONED_TABLE,
                "VALUES (1, 'keyboard'), (2, 'monitor')");

        assertQuery(
                "SELECT order_id, item, region FROM " + PARTITIONED_TABLE,
                "VALUES (3, 'desk', 'emea'), (4, 'lamp', 'apac')");

        // The predicate on the partition column is the one Lakehouse is asked to apply
        assertQuery(
                "SELECT item FROM " + PARTITIONED_TABLE + " WHERE region = 'emea'",
                "VALUES 'desk'");
    }
}
