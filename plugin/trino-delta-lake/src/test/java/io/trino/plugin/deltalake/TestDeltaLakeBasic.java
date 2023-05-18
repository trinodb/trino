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
package io.trino.plugin.deltalake;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.io.Resources;
import io.airlift.json.ObjectMapperProvider;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.assertj.core.api.Assertions;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.google.common.collect.Iterators.getOnlyElement;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.createDeltaLakeQueryRunner;
import static io.trino.plugin.deltalake.DeltaTestingConnectorSession.SESSION;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.TransactionLogTail.getEntriesFromJson;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertFalse;

public class TestDeltaLakeBasic
        extends AbstractTestQueryFramework
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();

    private static final List<String> PERSON_TABLES = ImmutableList.of(
            "person", "person_without_last_checkpoint", "person_without_old_jsons", "person_without_checkpoints");
    private static final List<String> OTHER_TABLES = ImmutableList.of("no_column_stats");

    // The col-{uuid} pattern for delta.columnMapping.physicalName
    private static final Pattern PHYSICAL_COLUMN_NAME_PATTERN = Pattern.compile("^col-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createDeltaLakeQueryRunner(DELTA_CATALOG, ImmutableMap.of(), ImmutableMap.of("delta.register-table-procedure.enabled", "true"));
    }

    @BeforeClass
    public void registerTables()
    {
        for (String table : Iterables.concat(PERSON_TABLES, OTHER_TABLES)) {
            String dataPath = getTableLocation(table).toExternalForm();
            getQueryRunner().execute(
                    format("CALL system.register_table('%s', '%s', '%s')", getSession().getSchema().orElseThrow(), table, dataPath));
        }
    }

    private URL getTableLocation(String table)
    {
        return getClass().getClassLoader().getResource("databricks/" + table);
    }

    @DataProvider
    public Object[][] tableNames()
    {
        return PERSON_TABLES.stream()
                .map(table -> new Object[] {table})
                .toArray(Object[][]::new);
    }

    @Test(dataProvider = "tableNames")
    public void testDescribeTable(String tableName)
    {
        // the schema is actually defined in the transaction log
        assertQuery(
                format("DESCRIBE %s", tableName),
                "VALUES " +
                        "('name', 'varchar', '', ''), " +
                        "('age', 'integer', '', ''), " +
                        "('married', 'boolean', '', ''), " +
                        "('gender', 'varchar', '', ''), " +
                        "('phones', 'array(row(number varchar, label varchar))', '', ''), " +
                        "('address', 'row(street varchar, city varchar, state varchar, zip varchar)', '', ''), " +
                        "('income', 'double', '', '')");
    }

    @Test(dataProvider = "tableNames")
    public void testSimpleQueries(String tableName)
    {
        assertQuery(format("SELECT COUNT(*) FROM %s", tableName), "VALUES 12");
        assertQuery(format("SELECT income FROM %s WHERE name = 'Bob'", tableName), "VALUES 99000.00");
        assertQuery(format("SELECT name FROM %s WHERE name LIKE 'B%%'", tableName), "VALUES ('Bob'), ('Betty')");
        assertQuery(format("SELECT DISTINCT gender FROM %s", tableName), "VALUES ('M'), ('F'), (null)");
        assertQuery(format("SELECT DISTINCT age FROM %s", tableName), "VALUES (21), (25), (28), (29), (30), (42)");
        assertQuery(format("SELECT name FROM %s WHERE age = 42", tableName), "VALUES ('Alice'), ('Emma')");
    }

    @Test
    public void testNoColumnStats()
    {
        // Data generated using:
        // CREATE TABLE no_column_stats
        // USING delta
        // LOCATION 's3://starburst-alex/delta/no_column_stats'
        // TBLPROPERTIES (delta.dataSkippingNumIndexedCols=0)   -- collects only table stats (row count), but no column stats
        // AS
        // SELECT 42 AS c_int, 'foo' AS c_str
        assertQuery("SELECT c_str FROM no_column_stats WHERE c_int = 42", "VALUES 'foo'");
    }

    /**
     * @see deltalake.column_mapping_mode_id
     * @see deltalake.column_mapping_mode_name
     */
    @Test(dataProvider = "columnMappingModeDataProvider")
    public void testAddNestedColumnWithColumnMappingMode(String columnMappingMode)
            throws Exception
    {
        // The table contains 'x' column with column mapping mode
        String tableName = "test_add_column_" + randomNameSuffix();
        Path tableLocation = Files.createTempFile(tableName, null);
        copyDirectoryContents(new File(Resources.getResource("deltalake/column_mapping_mode_" + columnMappingMode).toURI()).toPath(), tableLocation);

        assertUpdate("CALL system.register_table('%s', '%s', '%s')".formatted(getSession().getSchema().orElseThrow(), tableName, tableLocation.toUri()));
        assertThat(query("DESCRIBE " + tableName)).projected("Column", "Type").skippingTypesCheck().matches("VALUES ('x', 'integer')");
        assertQueryReturnsEmptyResult("SELECT * FROM " + tableName);

        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN second_col row(a array(integer), b map(integer, integer), c row(field integer))");
        MetadataEntry metadata = loadMetadataEntry(1, tableLocation);
        Assertions.assertThat(metadata.getConfiguration().get("delta.columnMapping.maxColumnId"))
                .isEqualTo("6"); // +5 comes from second_col + second_col.a + second_col.b + second_col.c + second_col.c.field

        JsonNode schema = OBJECT_MAPPER.readTree(metadata.getSchemaString());
        List<JsonNode> fields = ImmutableList.copyOf(schema.get("fields").elements());
        Assertions.assertThat(fields).hasSize(2);
        JsonNode columnX = fields.get(0);
        JsonNode columnY = fields.get(1);

        List<JsonNode> rowFields = ImmutableList.copyOf(columnY.get("type").get("fields").elements());
        Assertions.assertThat(rowFields).hasSize(3);
        JsonNode nestedArray = rowFields.get(0);
        JsonNode nestedMap = rowFields.get(1);
        JsonNode nestedRow = rowFields.get(2);

        // Verify delta.columnMapping.id and delta.columnMapping.physicalName values
        Assertions.assertThat(columnX.get("metadata").get("delta.columnMapping.id").asInt()).isEqualTo(1);
        Assertions.assertThat(columnX.get("metadata").get("delta.columnMapping.physicalName").asText()).containsPattern(PHYSICAL_COLUMN_NAME_PATTERN);
        Assertions.assertThat(columnY.get("metadata").get("delta.columnMapping.id").asInt()).isEqualTo(6);
        Assertions.assertThat(columnY.get("metadata").get("delta.columnMapping.physicalName").asText()).containsPattern(PHYSICAL_COLUMN_NAME_PATTERN);

        Assertions.assertThat(nestedArray.get("metadata").get("delta.columnMapping.id").asInt()).isEqualTo(2);
        Assertions.assertThat(nestedArray.get("metadata").get("delta.columnMapping.physicalName").asText()).containsPattern(PHYSICAL_COLUMN_NAME_PATTERN);

        Assertions.assertThat(nestedMap.get("metadata").get("delta.columnMapping.id").asInt()).isEqualTo(3);
        Assertions.assertThat(nestedMap.get("metadata").get("delta.columnMapping.physicalName").asText()).containsPattern(PHYSICAL_COLUMN_NAME_PATTERN);

        Assertions.assertThat(nestedRow.get("metadata").get("delta.columnMapping.id").asInt()).isEqualTo(5);
        Assertions.assertThat(nestedRow.get("metadata").get("delta.columnMapping.physicalName").asText()).containsPattern(PHYSICAL_COLUMN_NAME_PATTERN);
        Assertions.assertThat(getOnlyElement(nestedRow.get("type").get("fields").elements()).get("metadata").get("delta.columnMapping.id").asInt()).isEqualTo(4);
        Assertions.assertThat(getOnlyElement(nestedRow.get("type").get("fields").elements()).get("metadata").get("delta.columnMapping.physicalName").asText()).containsPattern(PHYSICAL_COLUMN_NAME_PATTERN);

        // Repeat adding a new column and verify the existing fields are preserved
        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN third_col row(a array(integer), b map(integer, integer), c row(field integer))");
        MetadataEntry thirdMetadata = loadMetadataEntry(2, tableLocation);
        JsonNode latestSchema = OBJECT_MAPPER.readTree(thirdMetadata.getSchemaString());
        List<JsonNode> latestFields = ImmutableList.copyOf(latestSchema.get("fields").elements());
        Assertions.assertThat(latestFields).hasSize(3);
        JsonNode latestColumnX = latestFields.get(0);
        JsonNode latestColumnY = latestFields.get(1);
        Assertions.assertThat(latestColumnX).isEqualTo(columnX);
        Assertions.assertThat(latestColumnY).isEqualTo(columnY);

        Assertions.assertThat(thirdMetadata.getConfiguration())
                .containsEntry("delta.columnMapping.maxColumnId", "11");
        Assertions.assertThat(thirdMetadata.getSchemaString())
                .containsPattern("(delta\\.columnMapping\\.id.*?){11}")
                .containsPattern("(delta\\.columnMapping\\.physicalName.*?){11}");
    }

    @DataProvider
    public Object[][] columnMappingModeDataProvider()
    {
        return new Object[][] {
                {"id"},
                {"name"},
        };
    }

    @Test
    public void testCorruptedManagedTableLocation()
            throws Exception
    {
        String tableName = "bad_person_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 id, 'person1' name", 1);
        String tableLocation = (String) computeScalar("SELECT DISTINCT regexp_replace(\"$path\", '/[^/]*$', '') FROM " + tableName);
        testCorruptedTableLocation(tableName, Path.of(URI.create(tableLocation)), true);
    }

    @Test
    public void testCorruptedExternalTableLocation()
            throws Exception
    {
        // create a bad_person table which is based on person table in temporary location
        String tableName = "bad_person_" + randomNameSuffix();
        Path tableLocation = Files.createTempFile(tableName, null);
        copyDirectoryContents(Path.of(getTableLocation("person").toURI()), tableLocation);
        getQueryRunner().execute(
                format("CALL system.register_table('%s', '%s', '%s')", getSession().getSchema().orElseThrow(), tableName, tableLocation));
        testCorruptedTableLocation(tableName, tableLocation, false);
    }

    private void testCorruptedTableLocation(String tableName, Path tableLocation, boolean isManaged)
            throws Exception
    {
        Path transactionLogDirectory = tableLocation.resolve("_delta_log");

        // break the table by deleting all its files under transaction log
        deleteRecursively(transactionLogDirectory, ALLOW_INSECURE);

        // Flush the metadata cache before verifying operations on the table
        assertUpdate("CALL system.flush_metadata_cache(schema_name => CURRENT_SCHEMA, table_name => '" + tableName + "')");

        // Assert queries fail cleanly
        assertQueryFails("TABLE " + tableName, "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("SELECT * FROM " + tableName + " WHERE false", "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("SELECT 1 FROM " + tableName + " WHERE false", "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("SHOW CREATE TABLE " + tableName, "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("CREATE TABLE a_new_table (LIKE " + tableName + " EXCLUDING PROPERTIES)", "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("DESCRIBE " + tableName, "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("SHOW COLUMNS FROM " + tableName, "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("SHOW STATS FOR " + tableName, "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("ANALYZE " + tableName, "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("ALTER TABLE " + tableName + " EXECUTE optimize", "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("ALTER TABLE " + tableName + " EXECUTE vacuum", "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("ALTER TABLE " + tableName + " RENAME TO bad_person_some_new_name", "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("ALTER TABLE " + tableName + " ADD COLUMN foo int", "Metadata not found in transaction log for tpch." + tableName);
        // TODO (https://github.com/trinodb/trino/issues/16248) ADD field
        assertQueryFails("ALTER TABLE " + tableName + " DROP COLUMN foo", "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("ALTER TABLE " + tableName + " DROP COLUMN foo.bar", "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("ALTER TABLE " + tableName + " SET PROPERTIES change_data_feed_enabled = true", "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("INSERT INTO " + tableName + " VALUES (NULL)", "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("UPDATE " + tableName + " SET foo = 'bar'", "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("DELETE FROM " + tableName, "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("MERGE INTO  " + tableName + " USING (SELECT 1 a) input ON true WHEN MATCHED THEN DELETE", "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("TRUNCATE TABLE " + tableName, "This connector does not support truncating tables");
        assertQueryFails("COMMENT ON TABLE " + tableName + " IS NULL", "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("COMMENT ON COLUMN " + tableName + ".foo IS NULL", "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("CALL system.vacuum(CURRENT_SCHEMA, '" + tableName + "', '7d')", "Metadata not found in transaction log for tpch." + tableName);
        assertQueryFails("SELECT * FROM TABLE(system.table_changes('tpch', '" + tableName + "'))", "Metadata not found in transaction log for tpch." + tableName);
        assertQuerySucceeds("CALL system.drop_extended_stats(CURRENT_SCHEMA, '" + tableName + "')");

        // Avoid failing metadata queries
        assertQuery("SHOW TABLES LIKE 'bad\\_person\\_%' ESCAPE '\\'", "VALUES '" + tableName + "'");
        assertQueryReturnsEmptyResult("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = CURRENT_SCHEMA AND table_name LIKE 'bad\\_person\\_%' ESCAPE '\\'");
        assertQueryReturnsEmptyResult("SELECT column_name, data_type FROM system.jdbc.columns WHERE table_cat = CURRENT_CATALOG AND table_schem = CURRENT_SCHEMA AND table_name LIKE 'bad\\_person\\_%' ESCAPE '\\'");

        // DROP TABLE should succeed so that users can remove their corrupted table
        getQueryRunner().execute("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
        if (isManaged) {
            assertThat(tableLocation.toFile()).doesNotExist().as("Table location should not exist");
        }
        else {
            assertThat(tableLocation.toFile()).exists().as("Table location should exist");
        }
    }

    private static MetadataEntry loadMetadataEntry(long entryNumber, Path tableLocation)
            throws IOException
    {
        TrinoFileSystem fileSystem = new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS).create(SESSION);
        DeltaLakeTransactionLogEntry transactionLog = getEntriesFromJson(entryNumber, tableLocation.resolve("_delta_log").toString(), fileSystem).orElseThrow().stream()
                .filter(log -> log.getMetaData() != null)
                .collect(onlyElement());
        return transactionLog.getMetaData();
    }

    private void copyDirectoryContents(Path source, Path destination)
            throws IOException
    {
        try (Stream<Path> stream = Files.walk(source)) {
            stream.forEach(file -> {
                try {
                    Files.copy(file, destination.resolve(source.relativize(file)), REPLACE_EXISTING);
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }
}
