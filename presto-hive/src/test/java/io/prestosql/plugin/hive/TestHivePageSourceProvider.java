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
package io.prestosql.plugin.hive;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slices;
import io.prestosql.GroupByHashPageIndexerFactory;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.metastore.HivePageSinkMetadata;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.JoinCompiler;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.TestingNodeManager;
import org.joda.time.DateTimeZone;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Date;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.prestosql.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.prestosql.plugin.hive.HiveTestUtils.PAGE_SORTER;
import static io.prestosql.plugin.hive.HiveTestUtils.TYPE_MANAGER;
import static io.prestosql.plugin.hive.HiveTestUtils.getDefaultHiveFileWriterFactories;
import static io.prestosql.plugin.hive.HiveTestUtils.getDefaultHivePageSourceFactories;
import static io.prestosql.plugin.hive.HiveTestUtils.getDefaultHiveRecordCursorProvider;
import static io.prestosql.plugin.hive.HiveTestUtils.getHiveSession;
import static io.prestosql.plugin.hive.HiveTestUtils.getHiveSessionProperties;
import static io.prestosql.plugin.hive.HiveType.HIVE_DATE;
import static io.prestosql.plugin.hive.HiveType.HIVE_INT;
import static io.prestosql.plugin.hive.HiveType.HIVE_STRING;
import static io.prestosql.plugin.hive.LocationHandle.WriteMode.DIRECT_TO_TARGET_NEW_DIRECTORY;
import static io.prestosql.plugin.hive.TestHivePageSink.toMaterializedResult;
import static io.prestosql.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static java.nio.file.Files.walk;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.joda.time.DateTimeZone.UTC;

public class TestHivePageSourceProvider
{
    private static final HiveConfig CONFIG = new HiveConfig();
    private static final File TEMP_DIR = Files.createTempDir();
    private static final HiveMetastore METASTORE = createTestingFileHiveMetastore(new File(TEMP_DIR, "metastore"));
    private static final String SCHEMA_NAME = "test";
    private static final String TABLE_NAME = "test";

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        deleteRecursively(TEMP_DIR.toPath(), ALLOW_INSECURE);
    }

    @Test
    public void testPartitionKeyPrefilling()
            throws IOException
    {
        List<HiveColumnHandle> columns = ImmutableList.<HiveColumnHandle>builder()
                .add(new HiveColumnHandle("r_int", HIVE_INT, INTEGER, 0, REGULAR, Optional.empty()))
                .add(new HiveColumnHandle("p_string", HIVE_STRING, VARCHAR, -1, PARTITION_KEY, Optional.empty()))
                .add(new HiveColumnHandle("p_date", HIVE_DATE, DATE, -1, PARTITION_KEY, Optional.empty()))
                .build();
        List<Type> types = columns.stream().map(HiveColumnHandle::getType).collect(toImmutableList());

        PageBuilder pageBuilder1 = new PageBuilder(types);
        BlockBuilder intBlockBuilder1 = pageBuilder1.getBlockBuilder(0);
        BlockBuilder stringBlockBuilder1 = pageBuilder1.getBlockBuilder(1);
        BlockBuilder dateBlockBuilder1 = pageBuilder1.getBlockBuilder(2);

        PageBuilder pageBuilder2 = new PageBuilder(types);
        BlockBuilder intBlockBuilder2 = pageBuilder2.getBlockBuilder(0);
        BlockBuilder stringBlockBuilder2 = pageBuilder2.getBlockBuilder(1);
        BlockBuilder dateBlockBuilder2 = pageBuilder2.getBlockBuilder(2);

        String partitionString1 = "string1";
        long partitionDate1 = LocalDate.parse("2019-11-11").toEpochDay();

        String partitionString2 = "string2";
        long partitionDate2 = LocalDate.parse("2019-11-12").toEpochDay();

        int rowCount = 10;
        for (int r = 0; r < rowCount; r++) {
            pageBuilder1.declarePosition();
            INTEGER.writeLong(intBlockBuilder1, r);
            VARCHAR.writeSlice(stringBlockBuilder1, Slices.utf8Slice(partitionString1));
            DATE.writeLong(dateBlockBuilder1, partitionDate1);

            pageBuilder2.declarePosition();
            INTEGER.writeLong(intBlockBuilder2, r);
            VARCHAR.writeSlice(stringBlockBuilder2, Slices.utf8Slice(partitionString2));
            DATE.writeLong(dateBlockBuilder2, partitionDate2);
        }

        Page page1 = pageBuilder1.build();
        Page page2 = pageBuilder2.build();

        String outputPath = TEMP_DIR.getAbsolutePath() + "/test_partition_key_prefilling";
        writeTestFile(outputPath, page1, columns);

        File outputFile = getOnlyElement(walk(Paths.get(outputPath))
                .filter(java.nio.file.Files::isRegularFile)
                .filter(path -> !path.toString().endsWith("crc"))
                .collect(toImmutableList())).toFile();

        List<HivePartitionKey> partitionKeys1 = ImmutableList.<HivePartitionKey>builder()
                .add(new HivePartitionKey("p_string", partitionString1))
                .add(new HivePartitionKey("p_date", new Date(UTC.getMillisKeepLocal(DateTimeZone.getDefault(), TimeUnit.DAYS.toMillis(partitionDate1))).toString()))
                .build();
        List<Page> pages1 = readTestFileWithPartitionKeys(outputFile, columns, partitionKeys1);
        assertPageListEquals(pages1, ImmutableList.of(page1), types);

        List<HivePartitionKey> partitionKeys2 = ImmutableList.<HivePartitionKey>builder()
                .add(new HivePartitionKey("p_string", partitionString2))
                .add(new HivePartitionKey("p_date", new Date(UTC.getMillisKeepLocal(DateTimeZone.getDefault(), TimeUnit.DAYS.toMillis(partitionDate2))).toString()))
                .build();
        List<Page> pages2 = readTestFileWithPartitionKeys(outputFile, columns, partitionKeys2);
        assertPageListEquals(pages2, ImmutableList.of(page2), types);
    }

    private static void writeTestFile(String outputPath, Page page, List<HiveColumnHandle> columns)
    {
        ConnectorSession session = getHiveSession(CONFIG);
        HiveIdentity identity = new HiveIdentity(session);
        LocationHandle locationHandle = new LocationHandle(outputPath, outputPath, false, DIRECT_TO_TARGET_NEW_DIRECTORY);
        HiveOutputTableHandle outputHandle = new HiveOutputTableHandle(
                SCHEMA_NAME,
                TABLE_NAME,
                columns,
                new HivePageSinkMetadata(new SchemaTableName(SCHEMA_NAME, TABLE_NAME), METASTORE.getTable(identity, SCHEMA_NAME, TABLE_NAME), ImmutableMap.of()),
                locationHandle,
                CONFIG.getHiveStorageFormat(),
                CONFIG.getHiveStorageFormat(),
                ImmutableList.of(),
                Optional.empty(),
                "test",
                ImmutableMap.of());
        JsonCodec<PartitionUpdate> partitionUpdateCodec = JsonCodec.jsonCodec(PartitionUpdate.class);
        HiveWriterStats stats = new HiveWriterStats();
        HivePageSinkProvider provider = new HivePageSinkProvider(
                getDefaultHiveFileWriterFactories(CONFIG, HDFS_ENVIRONMENT),
                HDFS_ENVIRONMENT,
                PAGE_SORTER,
                METASTORE,
                new GroupByHashPageIndexerFactory(new JoinCompiler(createTestMetadataManager())),
                TYPE_MANAGER,
                CONFIG,
                new HiveLocationService(HDFS_ENVIRONMENT),
                partitionUpdateCodec,
                new TestingNodeManager("fake-environment"),
                new HiveEventClient(),
                getHiveSessionProperties(CONFIG),
                stats);

        HiveTransactionHandle transaction = new HiveTransactionHandle();

        ConnectorPageSink pageSink = provider.createPageSink(transaction, session, outputHandle);
        pageSink.appendPage(page);
        getFutureValue(pageSink.finish());
    }

    private static List<Page> readTestFileWithPartitionKeys(File file, List<HiveColumnHandle> columns, List<HivePartitionKey> partitionKeys)
    {
        HivePageSourceProvider pageSourceProvider = new HivePageSourceProvider(
                CONFIG,
                HDFS_ENVIRONMENT,
                getDefaultHiveRecordCursorProvider(CONFIG, HDFS_ENVIRONMENT),
                getDefaultHivePageSourceFactories(CONFIG, HDFS_ENVIRONMENT),
                TYPE_MANAGER);

        HiveSplit split1 = new HiveSplit(
                SCHEMA_NAME,
                TABLE_NAME,
                "",
                "file:///" + file.getAbsolutePath(),
                0,
                file.length(),
                file.length(),
                file.lastModified(),
                generateSplitSchema(columns),
                partitionKeys,
                ImmutableList.of(),
                OptionalInt.empty(),
                false,
                ImmutableMap.of(),
                Optional.empty(),
                false);

        ConnectorTableHandle table = new HiveTableHandle(SCHEMA_NAME, TABLE_NAME, ImmutableMap.of(), ImmutableList.of(), Optional.empty());

        ConnectorPageSource pageSource = pageSourceProvider.createPageSource(
                new HiveTransactionHandle(),
                getHiveSession(CONFIG),
                split1,
                table,
                ImmutableList.copyOf(columns));

        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        while (!pageSource.isFinished()) {
            Page nextPage = pageSource.getNextPage();
            if (nextPage != null) {
                pages.add(nextPage.getLoadedPage());
            }
        }
        return pages.build();
    }

    private static Properties generateSplitSchema(List<HiveColumnHandle> columns)
    {
        Properties splitSchema = new Properties();
        splitSchema.setProperty(FILE_INPUT_FORMAT, CONFIG.getHiveStorageFormat().getInputFormat());
        splitSchema.setProperty(SERIALIZATION_LIB, CONFIG.getHiveStorageFormat().getSerDe());
        splitSchema.setProperty("columns", Joiner.on(',').join(columns.stream().map(HiveColumnHandle::getName).collect(toImmutableList())));
        splitSchema.setProperty("columns.types", Joiner.on(',').join(columns.stream().map(HiveColumnHandle::getHiveType).map(hiveType -> hiveType.getHiveTypeName().toString()).collect(toImmutableList())));
        return splitSchema;
    }

    private static void assertPageListEquals(List<Page> actual, List<Page> expected, List<Type> types)
    {
        ConnectorSession session = getHiveSession(CONFIG);
        MaterializedResult actualResults = toMaterializedResult(session, types, actual);
        MaterializedResult expectedResults = toMaterializedResult(session, types, expected);
        assertEquals(actualResults, expectedResults);
    }
}
