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

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.BaseTestParquetWithBloomFilters;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.nio.file.Path;
import java.util.List;

import static io.trino.plugin.hive.parquet.TestHiveParquetWithBloomFilters.writeParquetFileWithBloomFilter;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;

public class TestIcebergParquetWithBloomFilters
        extends BaseTestParquetWithBloomFilters
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = IcebergQueryRunner.builder().build();
        dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data");

        // create hive catalog
        queryRunner.installPlugin(new TestingHivePlugin());
        queryRunner.createCatalog("hive", "hive", ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", dataDirectory.toString())
                .put("hive.security", "allow-all")
                .buildOrThrow());

        return queryRunner;
    }

    @Override
    protected CatalogSchemaTableName createParquetTableWithBloomFilter(String columnName, List<Integer> testValues)
    {
        // create the managed table
        String tableName = "parquet_with_bloom_filters_" + randomNameSuffix();
        CatalogSchemaTableName hiveCatalogSchemaTableName = new CatalogSchemaTableName("hive", new SchemaTableName("tpch", tableName));
        CatalogSchemaTableName icebergCatalogSchemaTableName = new CatalogSchemaTableName("iceberg", new SchemaTableName("tpch", tableName));
        assertUpdate(format("CREATE TABLE %s (%s INT) WITH (format = 'PARQUET')", hiveCatalogSchemaTableName, columnName));

        // directly write data to the managed table
        Path tableLocation = Path.of("%s/tpch/%s".formatted(dataDirectory, tableName));
        Path fileLocation = tableLocation.resolve("bloomFilterFile.parquet");
        writeParquetFileWithBloomFilter(fileLocation.toFile(), columnName, testValues);

        // migrate the hive table to the iceberg table
        assertUpdate("CALL iceberg.system.migrate('tpch', '" + tableName + "', 'false')");

        return icebergCatalogSchemaTableName;
    }
}
