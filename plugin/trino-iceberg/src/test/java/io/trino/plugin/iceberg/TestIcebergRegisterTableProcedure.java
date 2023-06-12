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
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verify;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.hadoop.ConfigurationInstantiator.newEmptyConfiguration;
import static io.trino.plugin.hive.metastore.file.TestingFileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergUtil.METADATA_FOLDER_NAME;
import static io.trino.plugin.iceberg.procedure.RegisterTableProcedure.getLatestMetadataLocation;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.apache.iceberg.Files.localInput;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergRegisterTableProcedure
        extends AbstractTestQueryFramework
{
    private HiveMetastore metastore;
    private File metastoreDir;
    private TrinoFileSystem fileSystem;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        metastoreDir = Files.createTempDirectory("test_iceberg_register_table").toFile();
        metastoreDir.deleteOnExit();
        metastore = createTestingFileHiveMetastore(metastoreDir);
        return IcebergQueryRunner.builder()
                .setMetastoreDirectory(metastoreDir)
                .setIcebergProperties(ImmutableMap.of("iceberg.register-table-procedure.enabled", "true"))
                .build();
    }

    @BeforeClass
    public void initFileSystem()
    {
        fileSystem = getFileSystemFactory(getDistributedQueryRunner()).create(SESSION);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        deleteRecursively(metastoreDir.toPath(), ALLOW_INSECURE);
    }

    @DataProvider
    public static Object[][] fileFormats()
    {
        return Stream.of(IcebergFileFormat.values())
                .map(icebergFileFormat -> new Object[] {icebergFileFormat})
                .toArray(Object[][]::new);
    }

    @Test(dataProvider = "fileFormats")
    public void testRegisterTableWithTableLocation(IcebergFileFormat icebergFileFormat)
    {
        String tableName = "test_register_table_with_table_location_" + icebergFileFormat.name().toLowerCase(ENGLISH) + "_" + randomNameSuffix();

        assertUpdate(format("CREATE TABLE %s (a int, b varchar, c boolean) with (format = '%s')", tableName, icebergFileFormat));
        assertUpdate(format("INSERT INTO %s values(1, 'INDIA', true)", tableName), 1);
        assertUpdate(format("INSERT INTO %s values(2, 'USA', false)", tableName), 1);

        String tableLocation = getTableLocation(tableName);
        // Drop table from hive metastore and use the same table name to register again with the metadata
        dropTableFromMetastore(tableName);

        assertUpdate("CALL iceberg.system.register_table (CURRENT_SCHEMA, '" + tableName + "', '" + tableLocation + "')");

        assertThat(query(format("SELECT * FROM %s", tableName)))
                .matches("VALUES " +
                        "ROW(INT '1', VARCHAR 'INDIA', BOOLEAN 'true'), " +
                        "ROW(INT '2', VARCHAR 'USA', BOOLEAN 'false')");
        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test(dataProvider = "fileFormats")
    public void testRegisterPartitionedTable(IcebergFileFormat icebergFileFormat)
    {
        String tableName = "test_register_partitioned_table_" + icebergFileFormat.name().toLowerCase(ENGLISH) + "_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (data int, part varchar) WITH (partitioning = ARRAY['part'], format = '" + icebergFileFormat + "')");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);

        MaterializedResult partitions = computeActual(format("SELECT * FROM \"%s$partitions\"", tableName));
        assertThat(partitions.getMaterializedRows()).hasSize(1);

        String tableLocation = getTableLocation(tableName);
        dropTableFromMetastore(tableName);

        assertUpdate("CALL iceberg.system.register_table (CURRENT_SCHEMA, '" + tableName + "', '" + tableLocation + "')");

        MaterializedResult partitionsAfterRegister = computeActual(format("SELECT * FROM \"%s$partitions\"", tableName));
        assertThat(partitions).isEqualTo(partitionsAfterRegister);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test(dataProvider = "fileFormats")
    public void testRegisterTableWithComments(IcebergFileFormat icebergFileFormat)
    {
        String tableName = "test_register_table_with_comments_" + icebergFileFormat.name().toLowerCase(ENGLISH) + "_" + randomNameSuffix();

        assertUpdate(format("CREATE TABLE %s (a int, b varchar, c boolean) with (format = '%s')", tableName, icebergFileFormat));
        assertUpdate(format("INSERT INTO %s values(1, 'INDIA', true)", tableName), 1);
        assertUpdate(format("INSERT INTO %s values(2, 'USA', false)", tableName), 1);
        assertUpdate(format("COMMENT ON TABLE %s is 'my-table-comment'", tableName));
        assertUpdate(format("COMMENT ON COLUMN %s.a is 'a-comment'", tableName));
        assertUpdate(format("COMMENT ON COLUMN %s.b is 'b-comment'", tableName));
        assertUpdate(format("COMMENT ON COLUMN %s.c is 'c-comment'", tableName));

        String tableLocation = getTableLocation(tableName);
        // Drop table from hive metastore and use the same table name to register again with the metadata
        dropTableFromMetastore(tableName);

        assertUpdate("CALL iceberg.system.register_table (CURRENT_SCHEMA, '" + tableName + "', '" + tableLocation + "')");

        assertThat(getTableComment(tableName)).isEqualTo("my-table-comment");
        assertThat(getColumnComment(tableName, "a")).isEqualTo("a-comment");
        assertThat(getColumnComment(tableName, "b")).isEqualTo("b-comment");
        assertThat(getColumnComment(tableName, "c")).isEqualTo("c-comment");
        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test(dataProvider = "fileFormats")
    public void testRegisterTableWithShowCreateTable(IcebergFileFormat icebergFileFormat)
    {
        String tableName = "test_register_table_with_show_create_table_" + icebergFileFormat.name().toLowerCase(ENGLISH) + "_" + randomNameSuffix();

        assertUpdate(format("CREATE TABLE %s (a int, b varchar, c boolean) with (format = '%s')", tableName, icebergFileFormat));
        assertUpdate(format("INSERT INTO %s values(1, 'INDIA', true)", tableName), 1);

        String tableLocation = getTableLocation(tableName);
        String showCreateTableOld = (String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue();
        // Drop table from hive metastore and use the same table name to register again with the metadata
        dropTableFromMetastore(tableName);

        assertUpdate("CALL system.register_table (CURRENT_SCHEMA, '" + tableName + "', '" + tableLocation + "')");
        String showCreateTableNew = (String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue();

        assertThat(showCreateTableOld).isEqualTo(showCreateTableNew);
        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test(dataProvider = "fileFormats")
    public void testRegisterTableWithReInsert(IcebergFileFormat icebergFileFormat)
    {
        String tableName = "test_register_table_with_re_insert_" + icebergFileFormat.name().toLowerCase(ENGLISH) + "_" + randomNameSuffix();

        assertUpdate(format("CREATE TABLE %s (a int, b varchar, c boolean) with (format = '%s')", tableName, icebergFileFormat));
        assertUpdate(format("INSERT INTO %s values(1, 'INDIA', true)", tableName), 1);
        assertUpdate(format("INSERT INTO %s values(2, 'USA', false)", tableName), 1);

        String tableLocation = getTableLocation(tableName);
        // Drop table from hive metastore and use the same table name to register again with the metadata
        dropTableFromMetastore(tableName);

        assertUpdate("CALL system.register_table (CURRENT_SCHEMA, '" + tableName + "', '" + tableLocation + "')");
        assertUpdate(format("INSERT INTO %s values(3, 'POLAND', true)", tableName), 1);

        assertThat(query(format("SELECT * FROM %s", tableName)))
                .matches("VALUES " +
                        "ROW(INT '1', VARCHAR 'INDIA', BOOLEAN 'true'), " +
                        "ROW(INT '2', VARCHAR 'USA', BOOLEAN 'false'), " +
                        "ROW(INT '3', VARCHAR 'POLAND', BOOLEAN 'true')");
        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test(dataProvider = "fileFormats")
    public void testRegisterTableWithDroppedTable(IcebergFileFormat icebergFileFormat)
    {
        String tableName = "test_register_table_with_dropped_table_" + icebergFileFormat.name().toLowerCase(ENGLISH) + "_" + randomNameSuffix();

        assertUpdate(format("CREATE TABLE %s (a int, b varchar, c boolean) with (format = '%s')", tableName, icebergFileFormat));
        assertUpdate(format("INSERT INTO %s values(1, 'INDIA', true)", tableName), 1);
        assertUpdate(format("INSERT INTO %s values(2, 'USA', false)", tableName), 1);

        String tableLocation = getTableLocation(tableName);
        String tableNameNew = tableName + "_new";
        // Drop table to verify register_table call fails when no metadata can be found (table doesn't exist)
        assertUpdate(format("DROP TABLE %s", tableName));

        assertQueryFails("CALL iceberg.system.register_table (CURRENT_SCHEMA, '" + tableNameNew + "', '" + tableLocation + "')",
                ".*No versioned metadata file exists at location.*");
    }

    @Test(dataProvider = "fileFormats")
    public void testRegisterTableWithDifferentTableName(IcebergFileFormat icebergFileFormat)
    {
        String tableName = "test_register_table_with_different_table_name_old_" + icebergFileFormat.name().toLowerCase(ENGLISH) + "_" + randomNameSuffix();

        assertUpdate(format("CREATE TABLE %s (a int, b varchar, c boolean) with (format = '%s')", tableName, icebergFileFormat));
        assertUpdate(format("INSERT INTO %s values(1, 'INDIA', true)", tableName), 1);
        assertUpdate(format("INSERT INTO %s values(2, 'USA', false)", tableName), 1);

        String tableLocation = getTableLocation(tableName);
        String tableNameNew = tableName + "_new";
        // Drop table from hive metastore and use the same table name to register again with the metadata
        dropTableFromMetastore(tableName);

        assertUpdate("CALL iceberg.system.register_table (CURRENT_SCHEMA, '" + tableNameNew + "', '" + tableLocation + "')");
        assertUpdate(format("INSERT INTO %s values(3, 'POLAND', true)", tableNameNew), 1);

        assertThat(query(format("SELECT * FROM %s", tableNameNew)))
                .matches("VALUES " +
                        "ROW(INT '1', VARCHAR 'INDIA', BOOLEAN 'true'), " +
                        "ROW(INT '2', VARCHAR 'USA', BOOLEAN 'false'), " +
                        "ROW(INT '3', VARCHAR 'POLAND', BOOLEAN 'true')");
        assertUpdate(format("DROP TABLE %s", tableNameNew));
    }

    @Test(dataProvider = "fileFormats")
    public void testRegisterTableWithMetadataFile(IcebergFileFormat icebergFileFormat)
    {
        String tableName = "test_register_table_with_metadata_file_" + icebergFileFormat.name().toLowerCase(ENGLISH) + "_" + randomNameSuffix();

        assertUpdate(format("CREATE TABLE %s (a int, b varchar, c boolean) with (format = '%s')", tableName, icebergFileFormat));
        assertUpdate(format("INSERT INTO %s values(1, 'INDIA', true)", tableName), 1);
        assertUpdate(format("INSERT INTO %s values(2, 'USA', false)", tableName), 1);

        String tableLocation = getTableLocation(tableName);
        String metadataLocation = getLatestMetadataLocation(fileSystem, tableLocation);
        String metadataFileName = metadataLocation.substring(metadataLocation.lastIndexOf("/") + 1);
        // Drop table from hive metastore and use the same table name to register again with the metadata
        dropTableFromMetastore(tableName);

        assertUpdate("CALL iceberg.system.register_table (CURRENT_SCHEMA, '" + tableName + "', '" + tableLocation + "', '" + metadataFileName + "')");
        assertUpdate(format("INSERT INTO %s values(3, 'POLAND', true)", tableName), 1);

        assertThat(query(format("SELECT * FROM %s", tableName)))
                .matches("VALUES " +
                        "ROW(INT '1', VARCHAR 'INDIA', BOOLEAN 'true'), " +
                        "ROW(INT '2', VARCHAR 'USA', BOOLEAN 'false'), " +
                        "ROW(INT '3', VARCHAR 'POLAND', BOOLEAN 'true')");
        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    public void testRegisterTableWithNoMetadataFile()
            throws IOException
    {
        IcebergFileFormat icebergFileFormat = IcebergFileFormat.ORC;
        String tableName = "test_register_table_with_no_metadata_file_" + icebergFileFormat.name().toLowerCase(ENGLISH) + "_" + randomNameSuffix();

        assertUpdate(format("CREATE TABLE %s (a int, b varchar, c boolean) with (format = '%s')", tableName, icebergFileFormat));
        assertUpdate(format("INSERT INTO %s values(1, 'INDIA', true)", tableName), 1);
        assertUpdate(format("INSERT INTO %s values(2, 'USA', false)", tableName), 1);

        String tableLocation = getTableLocation(tableName);
        String tableNameNew = tableName + "_new";
        // Delete files under metadata directory to verify register_table call fails
        deleteRecursively(Path.of(tableLocation, METADATA_FOLDER_NAME), ALLOW_INSECURE);

        assertQueryFails("CALL iceberg.system.register_table (CURRENT_SCHEMA, '" + tableNameNew + "', '" + tableLocation + "')",
                ".*No versioned metadata file exists at location.*");
        dropTableFromMetastore(tableName);
        deleteRecursively(Path.of(tableLocation), ALLOW_INSECURE);
    }

    @Test
    public void testRegisterTableWithInvalidMetadataFile()
            throws IOException
    {
        String tableName = "test_register_table_with_invalid_metadata_file_" + randomNameSuffix();

        assertUpdate(format("CREATE TABLE %s (a int, b varchar, c boolean)", tableName));
        assertUpdate(format("INSERT INTO %s values(1, 'INDIA', true)", tableName), 1);
        assertUpdate(format("INSERT INTO %s values(2, 'USA', false)", tableName), 1);

        Location tableLocation = Location.of(getTableLocation(tableName));
        String tableNameNew = tableName + "_new";
        Location metadataDirectoryLocation = tableLocation.appendPath(METADATA_FOLDER_NAME);
        FileIterator fileIterator = fileSystem.listFiles(metadataDirectoryLocation);
        // Find one invalid metadata file inside metadata folder
        String invalidMetadataFileName = "invalid-default.avro";
        while (fileIterator.hasNext()) {
            FileEntry fileEntry = fileIterator.next();
            if (fileEntry.location().fileName().endsWith(".avro")) {
                invalidMetadataFileName = fileEntry.location().fileName();
                break;
            }
        }

        assertQueryFails("CALL iceberg.system.register_table (CURRENT_SCHEMA, '" + tableNameNew + "', '" + tableLocation + "', '" + invalidMetadataFileName + "')",
                "Invalid metadata file: .*");
        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    public void testRegisterTableWithNonExistingTableLocation()
    {
        String tableName = "test_register_table_with_non_existing_table_location_" + randomNameSuffix();
        String tableLocation = "/test/iceberg/hive/warehouse/orders_5-581fad8517934af6be1857a903559d44";
        assertQueryFails("CALL iceberg.system.register_table (CURRENT_SCHEMA, '" + tableName + "', '" + tableLocation + "')",
                ".*No versioned metadata file exists at location.*");
    }

    @Test
    public void testRegisterTableWithNonExistingMetadataFile()
    {
        String tableName = "test_register_table_with_non_existing_metadata_file_" + randomNameSuffix();
        String nonExistingMetadataFileName = "00003-409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json";
        String tableLocation = "/test/iceberg/hive/warehouse/orders_5-581fad8517934af6be1857a903559d44";
        assertQueryFails("CALL iceberg.system.register_table (CURRENT_SCHEMA, '" + tableName + "', '" + tableLocation + "', '" + nonExistingMetadataFileName + "')",
                "Metadata file does not exist: .*");
    }

    @Test
    public void testRegisterTableWithNonExistingSchema()
    {
        String tableLocation = "/test/iceberg/hive/orders_5-581fad8517934af6be1857a903559d44";
        assertQueryFails("CALL iceberg.system.register_table ('invalid_schema', 'test_table', '" + tableLocation + "')",
                ".*Schema '(.*)' does not exist.*");
    }

    @Test
    public void testRegisterTableWithExistingTable()
    {
        String tableName = "test_register_table_with_existing_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a int, b varchar, c boolean)");
        assertUpdate("INSERT INTO " + tableName + " values(1, 'INDIA', true)", 1);
        String tableLocation = getTableLocation(tableName);

        assertQueryFails("CALL iceberg.system.register_table (CURRENT_SCHEMA, '" + tableName + "', '" + tableLocation + "')",
                ".*Table already exists.*");
        assertUpdate(format("DROP TABLE %s", tableName));
    }

    @Test
    public void testRegisterTableWithInvalidURIScheme()
    {
        String tableName = "test_register_table_with_invalid_uri_scheme_" + randomNameSuffix();
        String nonExistedMetadataFileName = "00003-409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json";
        String tableLocation = "invalid://hadoop-master:9000/test/iceberg/hive/orders_5-581fad8517934af6be1857a903559d44";
        assertQueryFails("CALL iceberg.system.register_table (CURRENT_SCHEMA, '" + tableName + "', '" + tableLocation + "', '" + nonExistedMetadataFileName + "')",
                ".*Invalid metadata file location: .*");
        assertQueryFails("CALL iceberg.system.register_table (CURRENT_SCHEMA, '" + tableName + "', '" + tableLocation + "')",
                ".*Failed checking table location: .*");
    }

    @Test
    public void testRegisterTableWithInvalidParameter()
    {
        String tableName = "test_register_table_with_invalid_parameter_" + randomNameSuffix();
        String tableLocation = "/test/iceberg/hive/table1/";

        assertQueryFails(format("CALL iceberg.system.register_table (CURRENT_SCHEMA, '%s')", tableName),
                ".*'TABLE_LOCATION' is missing.*");
        assertQueryFails("CALL iceberg.system.register_table (CURRENT_SCHEMA)",
                ".*'TABLE_NAME' is missing.*");
        assertQueryFails("CALL iceberg.system.register_table ()",
                ".*'SCHEMA_NAME' is missing.*");

        assertQueryFails("CALL iceberg.system.register_table (null, null, null)",
                ".*schema_name cannot be null or empty.*");
        assertQueryFails("CALL iceberg.system.register_table (CURRENT_SCHEMA, null, null)",
                ".*table_name cannot be null or empty.*");
        assertQueryFails("CALL iceberg.system.register_table (CURRENT_SCHEMA, '" + tableName + "', null)",
                ".*table_location cannot be null or empty.*");

        assertQueryFails("CALL iceberg.system.register_table ('', '" + tableName + "', '" + tableLocation + "')",
                ".*schema_name cannot be null or empty.*");
        assertQueryFails("CALL iceberg.system.register_table (CURRENT_SCHEMA, '', '" + tableLocation + "')",
                ".*table_name cannot be null or empty.*");
        assertQueryFails("CALL iceberg.system.register_table (CURRENT_SCHEMA, '" + tableName + "', '')",
                ".*table_location cannot be null or empty.*");
        assertQueryFails("CALL iceberg.system.register_table (CURRENT_SCHEMA, '" + tableName + "', '" + tableLocation + "', '')",
                ".*metadata_file_name cannot be empty when provided as an argument.*");
    }

    @Test
    public void testRegisterTableWithInvalidMetadataFileName()
    {
        String tableName = "test_register_table_with_invalid_metadata_file_name_" + randomNameSuffix();
        String tableLocation = "/test/iceberg/hive";

        String[] invalidMetadataFileNames = {
                "/",
                "../",
                "../../",
                "../../somefile.metadata.json",
        };

        for (String invalidMetadataFileName : invalidMetadataFileNames) {
            assertQueryFails("CALL iceberg.system.register_table (CURRENT_SCHEMA, '" + tableName + "', '" + tableLocation + "', '" + invalidMetadataFileName + "')",
                    ".*is not a valid metadata file.*");
        }
    }

    @Test
    public void testRegisterHadoopTableAndRead()
    {
        // create a temporary table to generate data file
        String tempTableName = "temp_table_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tempTableName + " (id INT, name VARCHAR) WITH (format = 'ORC')");
        assertUpdate("INSERT INTO " + tempTableName + " values(1, 'INDIA')", 1);
        String dataFilePath = (String) computeScalar("SELECT \"$path\" FROM " + tempTableName);

        // create hadoop table
        String hadoopTableName = "hadoop_table_" + randomNameSuffix();
        String hadoopTableLocation = metastoreDir.getPath() + "/" + hadoopTableName;
        HadoopTables hadoopTables = new HadoopTables(newEmptyConfiguration());
        Schema schema = new Schema(ImmutableList.of(
                Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get())));
        Table table = hadoopTables.create(
                schema,
                PartitionSpec.unpartitioned(),
                SortOrder.unsorted(),
                ImmutableMap.of("write.format.default", "ORC"),
                hadoopTableLocation);

        // append data file to hadoop table
        DataFile dataFile =
                DataFiles.builder(PartitionSpec.unpartitioned())
                        .withFormat(FileFormat.ORC)
                        .withInputFile(localInput(new File(dataFilePath)))
                        .withPath(dataFilePath)
                        .withRecordCount(1)
                        .build();
        table.newFastAppend()
                .appendFile(dataFile)
                .commit();

        // Hadoop style version number
        assertThat(Location.of(getLatestMetadataLocation(fileSystem, hadoopTableLocation)).fileName())
                .isEqualTo("v2.metadata.json");

        // Try registering hadoop table in Trino and read it
        String registeredTableName = "registered_table_" + randomNameSuffix();
        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')".formatted(registeredTableName, hadoopTableLocation));
        assertQuery("SELECT * FROM " + registeredTableName, "VALUES (1, 'INDIA')");

        // Verify the table can be written to despite using non-standard metadata file name
        assertUpdate("INSERT INTO " + registeredTableName + " VALUES (2, 'POLAND')", 1);
        assertQuery("SELECT * FROM " + registeredTableName, "VALUES (1, 'INDIA'), (2, 'POLAND')");

        // New metadata file is written using standard file name convention
        assertThat(Location.of(getLatestMetadataLocation(fileSystem, hadoopTableLocation)).fileName())
                .matches("00003-.*\\.metadata\\.json");

        assertUpdate("DROP TABLE " + registeredTableName);
        assertUpdate("DROP TABLE " + tempTableName);
    }

    private String getTableLocation(String tableName)
    {
        Pattern locationPattern = Pattern.compile(".*location = '(.*?)'.*", Pattern.DOTALL);
        Matcher matcher = locationPattern.matcher((String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue());
        if (matcher.find()) {
            String location = matcher.group(1);
            verify(!matcher.find(), "Unexpected second match");
            return location;
        }
        throw new IllegalStateException("Location not found in SHOW CREATE TABLE result");
    }

    private void dropTableFromMetastore(String tableName)
    {
        metastore.dropTable(getSession().getSchema().orElseThrow(), tableName, false);
        assertThat(metastore.getTable(getSession().getSchema().orElseThrow(), tableName)).as("Table in metastore should be dropped").isEmpty();
    }

    private String getTableComment(String tableName)
    {
        return (String) computeScalar("SELECT comment FROM system.metadata.table_comments WHERE catalog_name = 'iceberg' AND schema_name = '" + getSession().getSchema().orElseThrow() + "' AND table_name = '" + tableName + "'");
    }

    private String getColumnComment(String tableName, String columnName)
    {
        return (String) computeScalar("SELECT comment FROM information_schema.columns WHERE table_schema = '" + getSession().getSchema().orElseThrow() + "' AND table_name = '" + tableName + "' AND column_name = '" + columnName + "'");
    }
}
