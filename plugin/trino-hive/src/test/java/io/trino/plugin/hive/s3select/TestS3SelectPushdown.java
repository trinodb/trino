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
package io.trino.plugin.hive.s3select;

import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.Storage;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.connector.ConnectorSession;
import io.trino.testing.TestingConnectorSession;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hive.hcatalog.data.JsonSerDe;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;

import static io.trino.hadoop.ConfigurationInstantiator.newEmptyConfiguration;
import static io.trino.plugin.hive.HiveMetadata.SKIP_FOOTER_COUNT_KEY;
import static io.trino.plugin.hive.HiveMetadata.SKIP_HEADER_COUNT_KEY;
import static io.trino.plugin.hive.HiveStorageFormat.ORC;
import static io.trino.plugin.hive.HiveStorageFormat.TEXTFILE;
import static io.trino.plugin.hive.HiveType.HIVE_BINARY;
import static io.trino.plugin.hive.HiveType.HIVE_BOOLEAN;
import static io.trino.plugin.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static io.trino.plugin.hive.s3select.S3SelectPushdown.isCompressionCodecSupported;
import static io.trino.plugin.hive.s3select.S3SelectPushdown.isSplittable;
import static io.trino.plugin.hive.s3select.S3SelectPushdown.shouldEnablePushdownForTable;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestS3SelectPushdown
{
    private static final String S3_SELECT_PUSHDOWN_ENABLED = "s3_select_pushdown_enabled";

    private TextInputFormat inputFormat;
    private ConnectorSession session;
    private Table table;
    private Partition partition;
    private Storage storage;
    private Column column;
    private Properties schema;

    @BeforeClass
    public void setUp()
    {
        inputFormat = new TextInputFormat();
        inputFormat.configure(new JobConf(newEmptyConfiguration()));

        session = TestingConnectorSession.builder()
                .setPropertyMetadata(List.of(booleanProperty(
                        S3_SELECT_PUSHDOWN_ENABLED,
                        "S3 Select pushdown enabled",
                        true,
                        false)))
                .setPropertyValues(Map.of(S3_SELECT_PUSHDOWN_ENABLED, true))
                .build();

        column = new Column("column", HIVE_BOOLEAN, Optional.empty());

        storage = Storage.builder()
                .setStorageFormat(fromHiveStorageFormat(TEXTFILE))
                .setLocation("location")
                .build();

        partition = new Partition(
                "db",
                "table",
                emptyList(),
                storage,
                singletonList(column),
                emptyMap());

        table = new Table(
                "db",
                "table",
                Optional.of("owner"),
                "type",
                storage,
                singletonList(column),
                emptyList(),
                emptyMap(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        schema = new Properties();
        schema.setProperty(SERIALIZATION_LIB, LazySimpleSerDe.class.getName());
    }

    @Test
    public void testIsCompressionCodecSupported()
    {
        assertTrue(isCompressionCodecSupported(inputFormat, "s3://fakeBucket/fakeObject.gz"));
        assertTrue(isCompressionCodecSupported(inputFormat, "s3://fakeBucket/fakeObject"));
        assertFalse(isCompressionCodecSupported(inputFormat, "s3://fakeBucket/fakeObject.lz4"));
        assertFalse(isCompressionCodecSupported(inputFormat, "s3://fakeBucket/fakeObject.snappy"));
        assertTrue(isCompressionCodecSupported(inputFormat, "s3://fakeBucket/fakeObject.bz2"));
    }

    @Test
    public void testShouldEnableSelectPushdown()
    {
        assertTrue(shouldEnablePushdownForTable(session, table, "s3://fakeBucket/fakeObject", Optional.empty()));
        assertTrue(shouldEnablePushdownForTable(session, table, "s3://fakeBucket/fakeObject", Optional.of(partition)));
    }

    @Test
    public void testShouldNotEnableSelectPushdownWhenDisabledOnSession()
    {
        ConnectorSession testSession = TestingConnectorSession.builder()
                .setPropertyMetadata(List.of(booleanProperty(
                        S3_SELECT_PUSHDOWN_ENABLED,
                        "S3 Select pushdown enabled",
                        false,
                        false)))
                .setPropertyValues(Map.of(S3_SELECT_PUSHDOWN_ENABLED, false))
                .build();
        assertFalse(shouldEnablePushdownForTable(testSession, table, "", Optional.empty()));
    }

    @Test
    public void testShouldNotEnableSelectPushdownWhenIsNotS3StoragePath()
    {
        assertFalse(shouldEnablePushdownForTable(session, table, null, Optional.empty()));
        assertFalse(shouldEnablePushdownForTable(session, table, "", Optional.empty()));
        assertFalse(shouldEnablePushdownForTable(session, table, "s3:/invalid", Optional.empty()));
        assertFalse(shouldEnablePushdownForTable(session, table, "s3:/invalid", Optional.of(partition)));
    }

    @Test
    public void testShouldNotEnableSelectPushdownWhenIsNotSupportedSerde()
    {
        Storage newStorage = Storage.builder()
                .setStorageFormat(fromHiveStorageFormat(ORC))
                .setLocation("location")
                .build();
        Table newTable = new Table(
                "db",
                "table",
                Optional.of("owner"),
                "type",
                newStorage,
                singletonList(column),
                emptyList(),
                emptyMap(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        assertFalse(shouldEnablePushdownForTable(session, newTable, "s3://fakeBucket/fakeObject", Optional.empty()));

        Partition newPartition = new Partition("db",
                "table",
                emptyList(),
                newStorage,
                singletonList(column),
                emptyMap());
        assertFalse(shouldEnablePushdownForTable(session, newTable, "s3://fakeBucket/fakeObject", Optional.of(newPartition)));
    }

    @Test
    public void testShouldNotEnableSelectPushdownWhenIsNotSupportedInputFormat()
    {
        Storage newStorage = Storage.builder()
                .setStorageFormat(StorageFormat.create(LazySimpleSerDe.class.getName(), "inputFormat", "outputFormat"))
                .setLocation("location")
                .build();
        Table newTable = new Table("db",
                "table",
                Optional.of("owner"),
                "type",
                newStorage,
                singletonList(column),
                emptyList(),
                emptyMap(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        assertFalse(shouldEnablePushdownForTable(session, newTable, "s3://fakeBucket/fakeObject", Optional.empty()));

        Partition newPartition = new Partition("db",
                "table",
                emptyList(),
                newStorage,
                singletonList(column),
                emptyMap());
        assertFalse(shouldEnablePushdownForTable(session, newTable, "s3://fakeBucket/fakeObject", Optional.of(newPartition)));

        newStorage = Storage.builder()
                .setStorageFormat(StorageFormat.create(LazySimpleSerDe.class.getName(), TextInputFormat.class.getName(), "outputFormat"))
                .setLocation("location")
                .build();
        newTable = new Table("db",
                "table",
                Optional.of("owner"),
                "type",
                newStorage,
                singletonList(column),
                emptyList(),
                Map.of(SKIP_HEADER_COUNT_KEY, "1"),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        assertFalse(shouldEnablePushdownForTable(session, newTable, "s3://fakeBucket/fakeObject", Optional.empty()));

        newTable = new Table("db",
                "table",
                Optional.of("owner"),
                "type",
                newStorage,
                singletonList(column),
                emptyList(),
                Map.of(SKIP_FOOTER_COUNT_KEY, "1"),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        assertFalse(shouldEnablePushdownForTable(session, newTable, "s3://fakeBucket/fakeObject", Optional.empty()));
    }

    @Test
    public void testShouldNotEnableSelectPushdownWhenColumnTypesAreNotSupported()
    {
        Column newColumn = new Column("column", HIVE_BINARY, Optional.empty());
        Table newTable = new Table("db",
                "table",
                Optional.of("owner"),
                "type",
                storage,
                singletonList(newColumn),
                emptyList(),
                emptyMap(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        assertFalse(shouldEnablePushdownForTable(session, newTable, "s3://fakeBucket/fakeObject", Optional.empty()));

        Partition newPartition = new Partition("db",
                "table",
                emptyList(),
                storage,
                singletonList(newColumn),
                emptyMap());
        assertFalse(shouldEnablePushdownForTable(session, newTable, "s3://fakeBucket/fakeObject", Optional.of(newPartition)));
    }

    @Test
    public void testShouldEnableSplits()
    {
        // Uncompressed CSV
        assertTrue(isSplittable(true, schema, inputFormat, "s3://fakeBucket/fakeObject.csv"));
        // Pushdown disabled
        assertTrue(isSplittable(false, schema, inputFormat, "s3://fakeBucket/fakeObject.csv"));
        // JSON
        Properties jsonSchema = new Properties();
        jsonSchema.setProperty(SERIALIZATION_LIB, JsonSerDe.class.getName());
        assertTrue(isSplittable(true, jsonSchema, inputFormat, "s3://fakeBucket/fakeObject.json"));
    }

    @Test
    public void testShouldNotEnableSplits()
    {
        // Compressed file
        assertFalse(isSplittable(true, schema, inputFormat, "s3://fakeBucket/fakeObject.gz"));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        inputFormat = null;
        session = null;
        table = null;
        partition = null;
        storage = null;
        column = null;
        schema = null;
    }
}
