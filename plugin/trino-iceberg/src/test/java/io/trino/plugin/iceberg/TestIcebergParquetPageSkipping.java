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

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.BaseTestParquetPageSkipping;
import io.trino.plugin.hive.HdfsConfig;
import io.trino.plugin.hive.HdfsConfiguration;
import io.trino.plugin.hive.HdfsConfigurationInitializer;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveHdfsConfiguration;
import io.trino.plugin.hive.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.file.FileMetastoreTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.hms.TrinoHiveCatalog;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TestingTypeManager;
import io.trino.testing.QueryRunner;
import org.apache.iceberg.BaseTable;
import org.intellij.lang.annotations.Language;
import org.testng.SkipException;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static com.google.common.io.Resources.getResource;
import static io.trino.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.plugin.iceberg.IcebergUtil.loadIcebergTable;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.apache.iceberg.TableProperties.DEFAULT_NAME_MAPPING;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergParquetPageSkipping
        extends BaseTestParquetPageSkipping
{
    private HiveMetastore metastore;
    private HdfsEnvironment hdfsEnvironment;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        HdfsConfig config = new HdfsConfig();
        HdfsConfiguration configuration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(config), ImmutableSet.of());
        hdfsEnvironment = new HdfsEnvironment(configuration, config, new NoHdfsAuthentication());

        Path tempDir = Files.createTempDirectory("test_iceberg_v2");
        File metastoreDir = tempDir.resolve("iceberg_data").toFile();
        metastore = createTestingFileHiveMetastore(metastoreDir);

        return IcebergQueryRunner.builder()
                .setIcebergProperties(Map.of(
                        "iceberg.file-format", "parquet",
                        // testAndPredicates method replaces Parquet file directly
                        "iceberg.use-file-size-from-metadata", "false",
                        "parquet.use-column-index", "true",
                        // Small max-buffer-size allows testing mix of small and large ranges in HdfsParquetDataSource#planRead
                        "parquet.max-buffer-size", "400B"))
                .setMetastoreDirectory(metastoreDir)
                .build();
    }

    @Override
    protected String tableDefinitionForSortedTables(String tableName, String sortByColumnName, String sortByColumnType)
    {
        if (sortByColumnType.equals("smallint")) {
            throw new SkipException("Unsupported column type: " + sortByColumnType);
        }
        if (sortByColumnType.equals("timestamp")) {
            sortByColumnType = "timestamp(6)";
        }
        String createTableTemplate =
                "CREATE TABLE %s ( " +
                        "   orderkey bigint, " +
                        "   custkey bigint, " +
                        "   orderstatus varchar(1), " +
                        "   totalprice double, " +
                        "   orderdate date, " +
                        "   orderpriority varchar(15), " +
                        "   clerk varchar(15), " +
                        "   shippriority integer, " +
                        "   comment varchar(79), " +
                        "   rvalues double array " +
                        ") " +
                        "WITH ( " +
                        "   format = 'PARQUET' " +
                        ")";
        createTableTemplate = createTableTemplate.replaceFirst(sortByColumnName + "[ ]+([^,]*)", sortByColumnName + " " + sortByColumnType);
        return format(createTableTemplate, tableName, sortByColumnName);
    }

    @Override
    public void testAndPredicates()
            throws Exception
    {
        // Override because optimized Parquet writer used in Iceberg doesn't support column index at this time
        // TODO: Remove this overridden test once https://github.com/trinodb/trino/issues/9359 is resolved
        String tableName = "test_and_predicate_" + randomTableSuffix();
        createTableForRowGroupPruning(tableName);
        @Language("JSON")
        String schemaNameMapping = "[" +
                "{\"field-id\": 1, \"names\": [\"orderkey\"]}, " +
                "{\"field-id\": 2, \"names\": [\"custkey\"]}, " +
                "{\"field-id\": 3, \"names\": [\"orderstatus\"]}, " +
                "{\"field-id\": 4, \"names\": [\"totalprice\"]}, " +
                "{\"field-id\": 5, \"names\": [\"orderdate\"]}, " +
                "{\"field-id\": 6, \"names\": [\"orderpriority\"]}, " +
                "{\"field-id\": 7, \"names\": [\"clerk\"]}, " +
                "{\"field-id\": 8, \"names\": [\"shippriority\"]}, " +
                "{\"field-id\": 9, \"names\": [\"comment\"]}, " +
                "{\"field-id\": 10, \"names\": [\"rvalues\"]}]";

        loadTable(tableName)
                .updateProperties()
                .set(DEFAULT_NAME_MAPPING, schemaNameMapping)
                .commit();

        int rowCount = assertColumnIndexResults("SELECT * FROM " + tableName + " WHERE totalprice BETWEEN 100000 AND 131280 AND clerk = 'Clerk#000000624'");
        assertThat(rowCount).isGreaterThan(0);

        // `totalprice BETWEEN 51890 AND 51900` is chosen to lie between min/max values of row group
        // but outside page level min/max boundaries to trigger pruning of row group using column index
        assertRowGroupPruning("SELECT * FROM " + tableName + " WHERE totalprice BETWEEN 51890 AND 51900 AND orderkey > 0");
        assertUpdate("DROP TABLE " + tableName);
    }

    private void createTableForRowGroupPruning(String tableName)
            throws IOException, URISyntaxException
    {
        String createTableTemplate =
                "CREATE TABLE %s ( " +
                        "   orderkey bigint, " +
                        "   custkey bigint, " +
                        "   orderstatus varchar(1), " +
                        "   totalprice double, " +
                        "   orderdate date, " +
                        "   orderpriority varchar(15), " +
                        "   clerk varchar(15), " +
                        "   shippriority integer, " +
                        "   comment varchar(79), " +
                        "   rvalues double array " +
                        ") " +
                        "WITH ( " +
                        "   format = 'PARQUET' " +
                        ")";

        assertUpdate(format(createTableTemplate, tableName));

        assertUpdate(format("INSERT INTO %s SELECT *, ARRAY[rand(), rand(), rand()] FROM tpch.tiny.orders", tableName), 15000);

        Path parquetFilePath = Path.of((String) computeScalar(format("SELECT DISTINCT file_path FROM \"%s$files\"", tableName)));
        Files.copy(new File(getResource("orders-column-indexed.parquet").toURI()).toPath(), parquetFilePath, REPLACE_EXISTING);
        Files.delete(parquetFilePath.resolveSibling(format(".%s.crc", parquetFilePath.getFileName())));
    }

    private BaseTable loadTable(String tableName)
    {
        IcebergTableOperationsProvider tableOperationsProvider = new FileMetastoreTableOperationsProvider(new HdfsFileIoProvider(hdfsEnvironment));
        TrinoCatalog catalog = new TrinoHiveCatalog(
                new CatalogName("hive"),
                CachingHiveMetastore.memoizeMetastore(metastore, 1000),
                hdfsEnvironment,
                new TestingTypeManager(),
                tableOperationsProvider,
                "test",
                false,
                false,
                false);
        return (BaseTable) loadIcebergTable(catalog, tableOperationsProvider, SESSION, new SchemaTableName("tpch", tableName));
    }
}
