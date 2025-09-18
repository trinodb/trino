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
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Path;

import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergUtil.getLatestMetadataLocation;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.apache.iceberg.Files.localInput;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRegisterHadoopTable
        extends AbstractTestQueryFramework
{
    private TrinoFileSystem fileSystem;
    private Path dataDir;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = IcebergQueryRunner.builder()
                .addIcebergProperty("fs.native-local.enabled", "true")
                .addIcebergProperty("iceberg.register-table-procedure.enabled", "true")
                // Use root directory to allow hadoop & local filesystem to read the same files
                .addIcebergProperty("local.location", "/")
                .build();

        dataDir = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data");
        fileSystem = getFileSystemFactory(queryRunner).create(SESSION);
        return queryRunner;
    }

    @Test
    public void testRegisterHadoopTableAndRead()
    {
        // create a temporary table to generate data file
        String tempTableName = "temp_table_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tempTableName + " (id INT, name VARCHAR) WITH (format = 'ORC')");
        assertUpdate("INSERT INTO " + tempTableName + " values(1, 'INDIA')", 1);
        MaterializedResult fileData = getQueryRunner().execute(
                "SELECT file_path, record_count, file_size_in_bytes FROM \"%s$files\"".formatted(tempTableName));

        String dataFilePath = (String) fileData.getMaterializedRows().getFirst().getField(0);
        long recordCount = (long) fileData.getMaterializedRows().getFirst().getField(1);
        long fileSizeInBytes = (long) fileData.getMaterializedRows().getFirst().getField(2);
        assertThat(recordCount).isEqualTo(1);

        // create hadoop table
        String hadoopTableName = "hadoop_table_" + randomNameSuffix();
        String hadoopTableLocation = dataDir + "/" + hadoopTableName;
        HadoopTables hadoopTables = new HadoopTables(new Configuration(false));
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
                        .withFileSizeInBytes(fileSizeInBytes)
                        .build();
        table.newFastAppend()
                .appendFile(dataFile)
                .commit();

        // Hadoop style version number
        assertThat(Location.of(getLatestMetadataLocation(fileSystem, hadoopTableLocation)).fileName())
                .isEqualTo("v2.metadata.json");

        // Try registering hadoop table in Trino and read it
        String registeredTableName = "registered_table_" + randomNameSuffix();
        assertUpdate("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s', 'v2.metadata.json')".formatted(registeredTableName, hadoopTableLocation));

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
}
