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
package io.trino.plugin.deltalake.transactionlog;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.deltalake.AccessTrackingFileSystem;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointSchemaManager;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HdfsConfig;
import io.trino.plugin.hive.HdfsConfiguration;
import io.trino.plugin.hive.HdfsConfigurationInitializer;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveHdfsConfiguration;
import io.trino.plugin.hive.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TypeManager;
import io.trino.testing.TestingConnectorSession;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.ADD;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointEntryIterator.EntryType.PROTOCOL;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestTableSnapshot
{
    private final ConnectorSession session = TestingConnectorSession.SESSION;
    private final TypeManager typeManager = TESTING_TYPE_MANAGER;
    private final ParquetReaderOptions parquetReaderOptions = new ParquetReaderConfig().toParquetReaderOptions();

    private CheckpointSchemaManager checkpointSchemaManager;
    private AccessTrackingFileSystem accessTrackingFileSystem;
    private Path tableLocation;
    private HdfsEnvironment hdfsEnvironment;

    @BeforeMethod
    public void setUp()
            throws IOException, URISyntaxException
    {
        checkpointSchemaManager = new CheckpointSchemaManager(typeManager);
        URI deltaLogPath = getClass().getClassLoader().getResource("databricks/person").toURI();
        tableLocation = new Path(deltaLogPath);

        Configuration conf = new Configuration(false);
        FileSystem filesystem = tableLocation.getFileSystem(conf);
        accessTrackingFileSystem = new AccessTrackingFileSystem(filesystem);

        HdfsConfig hdfsConfig = new HdfsConfig();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hdfsConfig), ImmutableSet.of());
        hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hdfsConfig, new NoHdfsAuthentication());
    }

    @Test
    public void testOnlyReadsTrailingJsonFiles()
            throws IOException
    {
        Map<String, Integer> expectedFileAccess = new HashMap<>();
        TableSnapshot tableSnapshot = TableSnapshot.load(
                new SchemaTableName("schema", "person"), accessTrackingFileSystem, tableLocation, parquetReaderOptions, true);
        expectedFileAccess.put("_last_checkpoint", 1);
        expectedFileAccess.put("00000000000000000011.json", 1);
        expectedFileAccess.put("00000000000000000012.json", 1);
        expectedFileAccess.put("00000000000000000013.json", 1);
        expectedFileAccess.put("00000000000000000014.json", 1);
        assertEquals(accessTrackingFileSystem.getOpenCount(), expectedFileAccess);

        tableSnapshot.getJsonTransactionLogEntries().forEach(entry -> {});
        assertEquals(accessTrackingFileSystem.getOpenCount(), expectedFileAccess);
    }

    // TODO: Can't test the FileSystem access here because the DeltaLakePageSourceProvider doesn't use the FileSystem passed into the TableSnapshot.
    @Test
    public void readsCheckpointFile()
            throws IOException
    {
        TableSnapshot tableSnapshot = TableSnapshot.load(
                new SchemaTableName("schema", "person"), accessTrackingFileSystem, tableLocation, parquetReaderOptions, true);
        tableSnapshot.setCachedMetadata(Optional.of(new MetadataEntry("id", "name", "description", null, "schema", ImmutableList.of(), ImmutableMap.of(), 0)));
        try (Stream<DeltaLakeTransactionLogEntry> stream = tableSnapshot.getCheckpointTransactionLogEntries(
                session, ImmutableSet.of(ADD), checkpointSchemaManager, typeManager, accessTrackingFileSystem, hdfsEnvironment, new FileFormatDataSourceStats())) {
            List<DeltaLakeTransactionLogEntry> entries = stream.collect(toImmutableList());

            assertThat(entries).hasSize(9);

            assertThat(entries).element(3).extracting(DeltaLakeTransactionLogEntry::getAdd).isEqualTo(
                    new AddFileEntry(
                            "age=42/part-00003-0f53cae3-3e34-4876-b651-e1db9584dbc3.c000.snappy.parquet",
                            Map.of("age", "42"),
                            2634,
                            1579190165000L,
                            false,
                            Optional.of("{" +
                                    "\"numRecords\":1," +
                                    "\"minValues\":{\"name\":\"Alice\",\"address\":{\"street\":\"100 Main St\",\"city\":\"Anytown\",\"state\":\"NY\",\"zip\":\"12345\"},\"income\":111000.0}," +
                                    "\"maxValues\":{\"name\":\"Alice\",\"address\":{\"street\":\"100 Main St\",\"city\":\"Anytown\",\"state\":\"NY\",\"zip\":\"12345\"},\"income\":111000.0}," +
                                    "\"nullCount\":{\"name\":0,\"married\":0,\"phones\":0,\"address\":{\"street\":0,\"city\":0,\"state\":0,\"zip\":0},\"income\":0}" +
                                    "}"),
                            Optional.empty(),
                            null));

            assertThat(entries).element(7).extracting(DeltaLakeTransactionLogEntry::getAdd).isEqualTo(
                    new AddFileEntry(
                            "age=30/part-00002-5800be2e-2373-47d8-8b86-776a8ea9d69f.c000.snappy.parquet",
                            Map.of("age", "30"),
                            2688,
                            1579190165000L,
                            false,
                            Optional.of("{" +
                                    "\"numRecords\":1," +
                                    "\"minValues\":{\"name\":\"Andy\",\"address\":{\"street\":\"101 Main St\",\"city\":\"Anytown\",\"state\":\"NY\",\"zip\":\"12345\"},\"income\":81000.0}," +
                                    "\"maxValues\":{\"name\":\"Andy\",\"address\":{\"street\":\"101 Main St\",\"city\":\"Anytown\",\"state\":\"NY\",\"zip\":\"12345\"},\"income\":81000.0}," +
                                    "\"nullCount\":{\"name\":0,\"married\":0,\"phones\":0,\"address\":{\"street\":0,\"city\":0,\"state\":0,\"zip\":0},\"income\":0}" +
                                    "}"),
                            Optional.empty(),
                            null));
        }

        // lets read two entry types in one call; add and protocol
        try (Stream<DeltaLakeTransactionLogEntry> stream = tableSnapshot.getCheckpointTransactionLogEntries(
                session, ImmutableSet.of(ADD, PROTOCOL), checkpointSchemaManager, typeManager, accessTrackingFileSystem, hdfsEnvironment, new FileFormatDataSourceStats())) {
            List<DeltaLakeTransactionLogEntry> entries = stream.collect(toImmutableList());

            assertThat(entries).hasSize(10);

            assertThat(entries).element(3).extracting(DeltaLakeTransactionLogEntry::getAdd).isEqualTo(
                    new AddFileEntry(
                            "age=42/part-00003-0f53cae3-3e34-4876-b651-e1db9584dbc3.c000.snappy.parquet",
                            Map.of("age", "42"),
                            2634,
                            1579190165000L,
                            false,
                            Optional.of("{" +
                                    "\"numRecords\":1," +
                                    "\"minValues\":{\"name\":\"Alice\",\"address\":{\"street\":\"100 Main St\",\"city\":\"Anytown\",\"state\":\"NY\",\"zip\":\"12345\"},\"income\":111000.0}," +
                                    "\"maxValues\":{\"name\":\"Alice\",\"address\":{\"street\":\"100 Main St\",\"city\":\"Anytown\",\"state\":\"NY\",\"zip\":\"12345\"},\"income\":111000.0}," +
                                    "\"nullCount\":{\"name\":0,\"married\":0,\"phones\":0,\"address\":{\"street\":0,\"city\":0,\"state\":0,\"zip\":0},\"income\":0}" +
                                    "}"),
                            Optional.empty(),
                            null));

            assertThat(entries).element(6).extracting(DeltaLakeTransactionLogEntry::getProtocol).isEqualTo(new ProtocolEntry(1, 2));

            assertThat(entries).element(8).extracting(DeltaLakeTransactionLogEntry::getAdd).isEqualTo(
                    new AddFileEntry(
                            "age=30/part-00002-5800be2e-2373-47d8-8b86-776a8ea9d69f.c000.snappy.parquet",
                            Map.of("age", "30"),
                            2688,
                            1579190165000L,
                            false,
                            Optional.of("{" +
                                    "\"numRecords\":1," +
                                    "\"minValues\":{\"name\":\"Andy\",\"address\":{\"street\":\"101 Main St\",\"city\":\"Anytown\",\"state\":\"NY\",\"zip\":\"12345\"},\"income\":81000.0}," +
                                    "\"maxValues\":{\"name\":\"Andy\",\"address\":{\"street\":\"101 Main St\",\"city\":\"Anytown\",\"state\":\"NY\",\"zip\":\"12345\"},\"income\":81000.0}," +
                                    "\"nullCount\":{\"name\":0,\"married\":0,\"phones\":0,\"address\":{\"street\":0,\"city\":0,\"state\":0,\"zip\":0},\"income\":0}" +
                                    "}"),
                            Optional.empty(),
                            null));
        }
    }

    @Test
    public void testMaxTransactionId()
            throws IOException
    {
        TableSnapshot tableSnapshot = TableSnapshot.load(
                new SchemaTableName("schema", "person"), accessTrackingFileSystem, tableLocation, parquetReaderOptions, true);
        assertEquals(tableSnapshot.getVersion(), 13L);
    }
}
