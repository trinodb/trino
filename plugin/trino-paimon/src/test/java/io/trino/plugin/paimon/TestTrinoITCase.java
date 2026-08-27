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
package io.trino.plugin.paimon;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.Session;
import io.trino.orc.MemoryOrcDataSource;
import io.trino.orc.OrcDataSourceId;
import io.trino.orc.OrcReader;
import io.trino.orc.OrcReaderOptions;
import io.trino.parquet.AbstractParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.reader.MetadataReader;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryFailedException;
import io.trino.testing.QueryRunner;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.InnerTableCommit;
import org.apache.paimon.table.sink.InnerTableWrite;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.time.ZoneOffset.UTC;
import static org.apache.paimon.data.BinaryString.fromString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

@Execution(ExecutionMode.SAME_THREAD)  // Disable concurrent execution to avoid table name conflicts
@Isolated
public class TestTrinoITCase
        extends AbstractTestQueryFramework
{
    private static final String CATALOG = "paimon";
    private static final String DB = "default";
    private static final String TRINO_PAIMON_WRITER_VERSION = "trino-paimon";
    private static final String TRINO_PAIMON_ORC_WRITER_METADATA_KEY = "trino.paimon.writer";

    private String warehouse;
    protected long t2FirstCommitTimestamp;

    // Cleanup method to ensure test isolation
    @AfterEach
    public void cleanupTestTables()
    {
        try {
            // Drop common test tables that may have been created
            sql("DROP TABLE IF EXISTS paimon.default.t5");
            sql("DROP TABLE IF EXISTS paimon.default.t6");
            sql("DROP TABLE IF EXISTS paimon.default.json_values");
            sql("DROP TABLE IF EXISTS paimon.default.json_nested_values");
            sql("DROP TABLE IF EXISTS paimon.default.json_variant_evolution_values");
            sql("DROP TABLE IF EXISTS paimon.default.direct_projection_schema_evolution");
            sql("DROP TABLE IF EXISTS paimon.default.direct_projection_schema_evolution_orc");
            sql("DROP TABLE IF EXISTS paimon.default.direct_type_evolution");
            sql("DROP TABLE IF EXISTS paimon.default.direct_type_evolution_orc");
            sql("DROP TABLE IF EXISTS paimon.default.direct_filter_values");
            sql("DROP TABLE IF EXISTS paimon.default.direct_duplicate_projection_filter_values");
            sql("DROP TABLE IF EXISTS paimon.default.csv_values");
            sql("DROP TABLE IF EXISTS paimon.default.vector_directive_values");
            sql("DROP TABLE IF EXISTS paimon.default.vector_directive_add_column");
            sql("DROP TABLE IF EXISTS paimon.default.blob_directive_values");
            sql("DROP TABLE IF EXISTS paimon.default.blob_directive_add_column");
            sql("DROP TABLE IF EXISTS paimon.default.comment_directive_values");
            sql("DROP TABLE IF EXISTS paimon.default.orders");
            sql("DROP TABLE IF EXISTS paimon.default.comment_values");
            sql("DROP TABLE IF EXISTS paimon.default.replace_values");
            sql("DROP TABLE IF EXISTS paimon.default.truncate_values");
            sql("DROP TABLE IF EXISTS paimon.default.delete_all_bucket_unaware_values");
            sql("DROP TABLE IF EXISTS paimon.default.filtered_delete_bucket_unaware_values");
            sql("DROP TABLE IF EXISTS paimon.default.merge_delete_bucket_unaware_values");
            sql("DROP TABLE IF EXISTS paimon.default.merge_update_bucket_unaware_values");
            sql("DROP TABLE IF EXISTS paimon.default.hash_fixed_mutations");
            sql("DROP TABLE IF EXISTS paimon.default.hash_dynamic_writes");
            sql("DROP TABLE IF EXISTS paimon.default.hash_dynamic_overwrite");
            sql("DROP TABLE IF EXISTS paimon.default.hash_dynamic_mutations");
            sql("DROP TABLE IF EXISTS paimon.default.drop_nn_values");
            sql("DROP TABLE IF EXISTS paimon.default.nested_field_values");
            sql("DROP TABLE IF EXISTS paimon.default.not_null_values");
            sql("DROP TABLE IF EXISTS paimon.default.insert_default_values");
            sql("DROP TABLE IF EXISTS paimon.default.time_orc_values");
            sql("DROP TABLE IF EXISTS paimon.default.time_travel_schema_evolution");
            sql("DROP TABLE IF EXISTS paimon.default.incremental_schema_evolution");
            sql("DROP TABLE IF EXISTS paimon.default.incremental_tag_snapshot_values");
            sql("DROP TABLE IF EXISTS paimon.default.incremental_auto_tag_values");
            sql("DROP TABLE IF EXISTS paimon.default.row_tracking_values");
            sql("DROP TABLE IF EXISTS paimon.default.provider_sql_parquet_values");
            sql("DROP TABLE IF EXISTS paimon.default.provider_sql_orc_values");
            sql("DROP TABLE IF EXISTS paimon.default.branch_values");
            sql("DROP TABLE IF EXISTS paimon.default.branch_schema_values");
            sql("DROP TABLE IF EXISTS paimon.default.branch_fallback_values");
            // Drop test schemas that may have been created
            sql("DROP SCHEMA IF EXISTS paimon.test CASCADE");
            sql("DROP SCHEMA IF EXISTS paimon.tpch CASCADE");
        }
        catch (Exception e) {
            // Ignore cleanup errors - table may not exist
        }
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        warehouse = Files.createTempDirectory(UUID.randomUUID().toString()).toUri().toString();
        // flink sink
        Path tablePath1 = new Path(warehouse, DB + ".db/t1");
        SimpleTableTestHelper testHelper1 = createTestHelper(tablePath1);
        testHelper1.write(GenericRow.of(1, 2L, fromString("1"), fromString("1")));
        testHelper1.write(GenericRow.of(3, 4L, fromString("2"), fromString("2")));
        testHelper1.write(GenericRow.of(5, 6L, fromString("3"), fromString("3")));
        testHelper1.write(GenericRow.ofKind(RowKind.DELETE, 3, 4L, fromString("2"), fromString("2")));
        testHelper1.commit();

        Path tablePath2 = new Path(warehouse, "default.db/t2");
        SimpleTableTestHelper testHelper2 = createTestHelper(tablePath2);
        testHelper2.write(GenericRow.of(1, 2L, fromString("1"), fromString("1")));
        testHelper2.write(GenericRow.of(3, 4L, fromString("2"), fromString("2")));
        testHelper2.commit();
        testHelper2.createTag("1");
        t2FirstCommitTimestamp = System.currentTimeMillis();
        testHelper2.write(GenericRow.of(5, 6L, fromString("3"), fromString("3")));
        testHelper2.write(GenericRow.of(7, 8L, fromString("4"), fromString("4")));
        testHelper2.commit();
        testHelper2.createTag("tag-2");

        Path versionPrecedenceTablePath = new Path(warehouse, "default.db/t_version_precedence");
        SimpleTableTestHelper versionPrecedenceHelper = createTestHelper(versionPrecedenceTablePath);
        versionPrecedenceHelper.write(GenericRow.of(1, 2L, fromString("1"), fromString("1")));
        versionPrecedenceHelper.write(GenericRow.of(3, 4L, fromString("2"), fromString("2")));
        versionPrecedenceHelper.commit();
        versionPrecedenceHelper.write(GenericRow.of(5, 6L, fromString("3"), fromString("3")));
        versionPrecedenceHelper.write(GenericRow.of(7, 8L, fromString("4"), fromString("4")));
        versionPrecedenceHelper.commit();
        versionPrecedenceHelper.createTag("2", 1L);

        createSystemChangelogTable(new Path(warehouse, "default.db/system_changelog_values"));

        {
            Path tablePath3 = new Path(warehouse, "default.db/t3");
            RowType rowType = new RowType(Arrays.asList(
                    new DataField(0, "pt", DataTypes.STRING()),
                    new DataField(1, "a", new IntType()),
                    new DataField(2, "b", new BigIntType()),
                    new DataField(3, "c", new BigIntType()),
                    new DataField(4, "d", new IntType())));
            new SchemaManager(LocalFileIO.create(), tablePath3).createTable(new Schema(
                    rowType.getFields(),
                    Collections.singletonList("pt"),
                    Collections.emptyList(),
                    new HashMap<>(),
                    ""));
            FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath3);
            InnerTableWrite writer = table.newWrite("user");
            InnerTableCommit commit = table.newCommit("user");
            writer.write(GenericRow.of(fromString("1"), 1, 1L, 1L, 1));
            writer.write(GenericRow.of(fromString("1"), 1, 2L, 2L, 2));
            writer.write(GenericRow.of(fromString("2"), 3, 3L, 3L, 3));
            commit.commit(0, writer.prepareCommit(true, 0));
        }

        {
            Path tablePath = new Path(warehouse, "default.db/empty_t");
            RowType rowType = new RowType(
                    Arrays.asList(new DataField(1, "a", new IntType()), new DataField(2, "b", new BigIntType())));
            new SchemaManager(LocalFileIO.create(), tablePath).createTable(new Schema(
                    rowType.getFields(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    new HashMap<>(),
                    ""));
        }

        {
            Path tablePath4 = new Path(warehouse, "default.db/t4");
            List<DataField> innerRowFields = new ArrayList<>();
            innerRowFields.add(new DataField(4, "innercol1", new IntType()));
            innerRowFields.add(new DataField(5, "innercol2", new VarCharType(VarCharType.MAX_LENGTH)));
            RowType rowType = new RowType(Arrays.asList(
                    new DataField(0, "i", new IntType()),
                    new DataField(1, "map",
                            new MapType(
                                    new VarCharType(VarCharType.MAX_LENGTH),
                                    new VarCharType(VarCharType.MAX_LENGTH))),
                    new DataField(2, "innerrow", new RowType(true, innerRowFields)),
                    new DataField(3, "array", new ArrayType(new IntType()))));
            new SchemaManager(LocalFileIO.create(), tablePath4)
                    .createTable(new Schema(
                            rowType.getFields(),
                            Collections.emptyList(),
                            Collections.singletonList("i"),
                            Collections.singletonMap("bucket", "1"),
                            ""));
            FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath4);
            InnerTableWrite writer = table.newWrite("user");
            InnerTableCommit commit = table.newCommit("user");
            Map<Object, Object> map = new HashMap<>();
            map.put(fromString("1"), fromString("2"));
            writer.write(GenericRow.of(
                    1,
                    new GenericMap(map),
                    GenericRow.of(2, fromString("male")),
                    new GenericArray(new int[] {1, 2, 3})));
            commit.commit(0, writer.prepareCommit(true, 0));
        }

        {
            Path tablePath6 = new Path(warehouse, "default.db/t99");
            RowType rowType = new RowType(Arrays.asList(
                    new DataField(0, "boolean", DataTypes.BOOLEAN()),
                    new DataField(1, "tinyint", DataTypes.TINYINT()),
                    new DataField(2, "smallint", DataTypes.SMALLINT()),
                    new DataField(3, "int", DataTypes.INT()),
                    new DataField(4, "bigint", DataTypes.BIGINT()),
                    new DataField(5, "float", DataTypes.FLOAT()),
                    new DataField(6, "double", DataTypes.DOUBLE()),
                    new DataField(7, "char", DataTypes.CHAR(5)),
                    new DataField(8, "varchar", DataTypes.VARCHAR(100)),
                    new DataField(9, "date", DataTypes.DATE()),
                    new DataField(10, "timestamp_0", DataTypes.TIMESTAMP(0)),
                    new DataField(11, "timestamp_3", DataTypes.TIMESTAMP(3)),
                    new DataField(12, "timestamp_6", DataTypes.TIMESTAMP(6)),
                    new DataField(13, "timestamp_tz", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)),
                    new DataField(14, "decimal", DataTypes.DECIMAL(10, 5)),
                    new DataField(15, "varbinary", DataTypes.VARBINARY(10)),
                    new DataField(16, "array", DataTypes.ARRAY(DataTypes.INT())),
                    new DataField(17, "map", DataTypes.MAP(DataTypes.INT(), DataTypes.INT())),
                    new DataField(18, "row", DataTypes.ROW(
                            DataTypes.FIELD(100, "q1", DataTypes.INT()),
                            DataTypes.FIELD(101, "q2", DataTypes.INT())))));
            new SchemaManager(LocalFileIO.create(), tablePath6).createTable(new Schema(
                    rowType.getFields(),
                    List.of("boolean",
                            "tinyint",
                            "smallint",
                            "int",
                            "bigint",
                            "float",
                            "double",
                            "char",
                            "varchar",
                            "date",
                            "timestamp_0",
                            "timestamp_3",
                            "timestamp_6",
                            "timestamp_tz",
                            "decimal"),
                    List.of("boolean",
                            "tinyint",
                            "smallint",
                            "int",
                            "bigint",
                            "float",
                            "double",
                            "char",
                            "varchar",
                            "date",
                            "timestamp_0",
                            "timestamp_3",
                            "timestamp_6",
                            "timestamp_tz",
                            "decimal",
                            "varbinary"),
                    Collections.singletonMap("bucket", "1"),
                    ""));
            FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath6);
            InnerTableWrite writer = table.newWrite("user");
            InnerTableCommit commit = table.newCommit("user");
            writer.write(GenericRow.of(
                    true,
                    (byte) 1,
                    (short) 1,
                    1,
                    1L,
                    1.0f,
                    1.0d,
                    BinaryString.fromString("char1"),
                    BinaryString.fromString("varchar1"),
                    0,
                    Timestamp.fromMicros(1694505288000000L),
                    Timestamp.fromMicros(1694505288001000L),
                    Timestamp.fromMicros(1694505288001001L),
                    Timestamp.fromMicros(1694505288002001L),
                    Decimal.fromUnscaledLong(10000, 10, 5),
                    new byte[] {0x01, 0x02, 0x03},
                    new GenericArray(new int[] {1, 1, 1}),
                    new GenericMap(Map.of(1, 1)),
                    GenericRow.of(1, 1)));
            commit.commit(0, writer.prepareCommit(true, 0));
        }

        {
            Path tablePath7 = new Path(warehouse, "default.db/t100");
            RowType rowType = new RowType(Arrays.asList(
                    new DataField(0, "boolean", DataTypes.BOOLEAN()),
                    new DataField(1, "tinyint", DataTypes.TINYINT()),
                    new DataField(2, "smallint", DataTypes.SMALLINT()),
                    new DataField(3, "int", DataTypes.INT()),
                    new DataField(4, "bigint", DataTypes.BIGINT()),
                    new DataField(5, "float", DataTypes.FLOAT()),
                    new DataField(6, "double", DataTypes.DOUBLE()),
                    new DataField(7, "char", DataTypes.CHAR(5)),
                    new DataField(8, "varchar", DataTypes.VARCHAR(100)),
                    new DataField(9, "date", DataTypes.DATE()),
                    new DataField(10, "timestamp_0", DataTypes.TIMESTAMP(3)),
                    new DataField(11, "timestamp_3", DataTypes.TIMESTAMP(3)),
                    new DataField(12, "timestamp_6", DataTypes.TIMESTAMP(6)),
                    new DataField(13, "decimal", DataTypes.DECIMAL(10, 5)),
                    new DataField(14, "varbinary", DataTypes.VARBINARY(10)),
                    new DataField(15, "array", DataTypes.ARRAY(DataTypes.INT())),
                    new DataField(16, "map", DataTypes.MAP(DataTypes.INT(), DataTypes.INT())),
                    new DataField(17, "row", DataTypes.ROW(
                            DataTypes.FIELD(100, "q1", DataTypes.INT()),
                            DataTypes.FIELD(101, "q2", DataTypes.INT())))));
            new SchemaManager(LocalFileIO.create(), tablePath7).createTable(new Schema(
                    rowType.getFields(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.singletonMap("bucket", "-1"),
                    ""));
            FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath7);
            InnerTableWrite writer = table.newWrite("user");
            InnerTableCommit commit = table.newCommit("user");
            writer.write(GenericRow.of(
                    true,
                    (byte) 1,
                    (short) 1,
                    1,
                    1L,
                    1.0f,
                    1.0d,
                    BinaryString.fromString("char1"),
                    BinaryString.fromString("varchar1"),
                    0,
                    Timestamp.fromMicros(1694505288000000L),
                    Timestamp.fromMicros(1694505288001000L),
                    Timestamp.fromMicros(1694505288001001L),
                    Decimal.fromUnscaledLong(10000, 10, 5),
                    new byte[] {0x01, 0x02, 0x03},
                    new GenericArray(new int[] {1, 1, 1}),
                    new GenericMap(Map.of(1, 1)),
                    GenericRow.of(1, 1)));
            commit.commit(0, writer.prepareCommit(true, 0));

            new SchemaManager(LocalFileIO.create(), tablePath7).commitChanges(SchemaChange.dropColumn("smallint"));
            table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath7);
            writer = table.newWrite("user");
            commit = table.newCommit("user");
            writer.write(GenericRow.of(
                    true,
                    (byte) 1,
                    1,
                    1L,
                    1.0f,
                    1.0d,
                    BinaryString.fromString("char1"),
                    BinaryString.fromString("varchar1"),
                    0,
                    Timestamp.fromMicros(1694505288000000L),
                    Timestamp.fromMicros(1694505288001000L),
                    Timestamp.fromMicros(1694505288001001L),
                    Decimal.fromUnscaledLong(10000, 10, 5),
                    new byte[] {0x01, 0x02, 0x03},
                    new GenericArray(new int[] {1, 1, 1}),
                    new GenericMap(Map.of(1, 1)),
                    GenericRow.of(1, 1)));
            commit.commit(1, writer.prepareCommit(true, 1));

            new SchemaManager(LocalFileIO.create(), tablePath7)
                    .commitChanges(SchemaChange.addColumn("smallint", DataTypes.SMALLINT()));
            table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath7);
            writer = table.newWrite("user");
            commit = table.newCommit("user");
            writer.write(GenericRow.of(
                    true,
                    (byte) 1,
                    1,
                    1L,
                    1.0f,
                    1.0d,
                    BinaryString.fromString("char1"),
                    BinaryString.fromString("varchar1"),
                    0,
                    Timestamp.fromMicros(1694505288000000L),
                    Timestamp.fromMicros(1694505288001000L),
                    Timestamp.fromMicros(1694505288001001L),
                    Decimal.fromUnscaledLong(10000, 10, 5),
                    new byte[] {0x01, 0x02, 0x03},
                    new GenericArray(new int[] {1, 1, 1}),
                    new GenericMap(Map.of(1, 1)),
                    GenericRow.of(1, 1),
                    (short) 1));
            commit.commit(1, writer.prepareCommit(true, 1));
        }

        {
            Path tablePath6 = new Path(warehouse, "default.db/t101");
            RowType rowType = new RowType(Arrays.asList(
                    new DataField(0, "a", DataTypes.STRING()),
                    new DataField(1, "b", DataTypes.INT()),
                    new DataField(2, "c", DataTypes.INT())));
            new SchemaManager(LocalFileIO.create(), tablePath6).createTable(
                    new Schema(rowType.getFields(), Collections.emptyList(), List.of("a"), Map.of(
                            CoreOptions.BUCKET.key(), "1",
                            CoreOptions.DELETION_VECTORS_ENABLED.key(), "true"), ""));
            FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath6);
            InnerTableWrite writer = table.newWrite("user");
            writer.withIOManager(new IOManagerImpl("/tmp"));
            InnerTableCommit commit = table.newCommit("user");
            for (int i = 0; i < 10; i++) {
                writer.write(GenericRow.of(BinaryString.fromString("a" + i), i, i));
            }
            commit.commit(0, writer.prepareCommit(true, 0));

            writer.write(GenericRow.ofKind(RowKind.DELETE, BinaryString.fromString("a0"), 0, 0));
            commit.commit(1, writer.prepareCommit(true, 1));
        }

        {
            Path tablePath = new Path(warehouse, "default.db/t102");
            RowType rowType = new RowType(Arrays.asList(
                    new DataField(0, "a", DataTypes.STRING()),
                    new DataField(1, "b", DataTypes.INT()),
                    new DataField(2, "c", DataTypes.INT())));
            new SchemaManager(LocalFileIO.create(), tablePath).createTable(
                    new Schema(rowType.getFields(), Collections.emptyList(), Collections.emptyList(), Map.of(
                            "file-index.bloom-filter.columns", "a,b,c"), ""));
            FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath);
            InnerTableWrite writer = table.newWrite("user");
            writer.withIOManager(new IOManagerImpl("/tmp"));
            InnerTableCommit commit = table.newCommit("user");
            for (int i = 0; i < 100; i = i + 3) {
                writer.write(GenericRow.of(BinaryString.fromString("a" + i), i, i));
            }
            commit.commit(0, writer.prepareCommit(true, 0));

            for (int i = 1; i < 100; i = i + 3) {
                writer.write(GenericRow.of(BinaryString.fromString("a" + i), i, i));
            }
            commit.commit(1, writer.prepareCommit(true, 1));

            for (int i = 2; i < 100; i = i + 3) {
                writer.write(GenericRow.of(BinaryString.fromString("a" + i), i, i));
            }
            commit.commit(2, writer.prepareCommit(true, 2));
        }

        {
            Path tablePath = new Path(warehouse, "default.db/t103");
            RowType rowType = new RowType(Arrays.asList(
                    new DataField(0, "id", DataTypes.INT()),
                    new DataField(1, "properties", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())),
                    new DataField(2, "payload", DataTypes.STRING())));
            new SchemaManager(LocalFileIO.create(), tablePath).createTable(
                    new Schema(rowType.getFields(), Collections.emptyList(), Collections.emptyList(), Map.of(
                            "file-index.bloom-filter.columns", "properties[region]"), ""));
            FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath);
            InnerTableWrite writer = table.newWrite("user");
            writer.withIOManager(new IOManagerImpl("/tmp"));
            InnerTableCommit commit = table.newCommit("user");

            Map<Object, Object> apSouth = new HashMap<>();
            apSouth.put(fromString("region"), fromString("ap-south"));
            apSouth.put(fromString("zone"), fromString("primary"));
            writer.write(GenericRow.of(1, new GenericMap(apSouth), fromString("keep-ap-south")));
            commit.commit(0, writer.prepareCommit(true, 0));

            Map<Object, Object> euWest = new HashMap<>();
            euWest.put(fromString("region"), fromString("eu-west"));
            euWest.put(fromString("zone"), fromString("secondary"));
            writer.write(GenericRow.of(2, new GenericMap(euWest), fromString("skip-eu-west")));
            commit.commit(1, writer.prepareCommit(true, 1));

            Map<Object, Object> usEast = new HashMap<>();
            usEast.put(fromString("region"), fromString("us-east"));
            usEast.put(fromString("zone"), fromString("tertiary"));
            writer.write(GenericRow.of(3, new GenericMap(usEast), fromString("skip-us-east")));
            commit.commit(2, writer.prepareCommit(true, 2));
        }

        {
            Path tablePath = new Path(warehouse, "default.db/fixed_bucket_table_wi_pk");
            RowType rowType = new RowType(Arrays.asList(
                    new DataField(0, "id", DataTypes.INT()),
                    new DataField(1, "name", DataTypes.STRING())));
            new SchemaManager(LocalFileIO.create(), tablePath).createTable(
                    new Schema(rowType.getFields(), Collections.emptyList(), Collections.emptyList(), Map.of(
                            "file.format", "orc",
                            "primary-key", "id",
                            "bucket", "2"), ""));
        }

        {
            Path tablePath = new Path(warehouse, "default.db/fixed_bucket_table_wo_pk");
            RowType rowType = new RowType(Arrays.asList(
                    new DataField(0, "id", DataTypes.INT()),
                    new DataField(1, "name", DataTypes.STRING())));
            new SchemaManager(LocalFileIO.create(), tablePath).createTable(
                    new Schema(rowType.getFields(), Collections.emptyList(), Collections.emptyList(), Map.of(
                            "file.format", "orc",
                            "bucket", "2",
                            "bucket-key", "id"), ""));
        }

        {
            Path tablePath = new Path(warehouse, "default.db/unaware_table");
            RowType rowType = new RowType(Arrays.asList(
                    new DataField(0, "id", DataTypes.INT()),
                    new DataField(1, "name", DataTypes.STRING())));
            new SchemaManager(LocalFileIO.create(), tablePath).createTable(
                    new Schema(rowType.getFields(), Collections.emptyList(), Collections.emptyList(), Map.of(
                            "file.format", "orc"), ""));
        }

        {
            Path tablePath = new Path(warehouse, "default.db/vector_values");
            RowType rowType = new RowType(Arrays.asList(
                    new DataField(0, "id", DataTypes.INT()),
                    new DataField(1, "embedding", DataTypes.VECTOR(3, DataTypes.FLOAT()))));
            new SchemaManager(LocalFileIO.create(), tablePath).createTable(new Schema(
                    rowType.getFields(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Map.of(
                            CoreOptions.FILE_FORMAT.key(), "json",
                            CoreOptions.FILE_COMPRESSION.key(), "none"),
                    ""));
            FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath);
            InnerTableWrite writer = table.newWrite("user");
            InnerTableCommit commit = table.newCommit("user");
            writer.write(GenericRow.of(1, BinaryVector.fromPrimitiveArray(new float[] {1.0f, 2.5f, 3.75f})));
            commit.commit(0, writer.prepareCommit(true, 0));
        }

        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner.builder(testSessionBuilder().setCatalog(CATALOG).setSchema(DB).build())
                    .build();
            queryRunner.installPlugin(new PaimonPlugin());
            Map<String, String> options = new HashMap<>();
            options.put("warehouse", warehouse);
            options.put("fs.local.enabled", "true");
            options.put("local.location", "/");
            queryRunner.createCatalog(CATALOG, CATALOG, options);
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    private static SimpleTableTestHelper createTestHelper(Path tablePath)
            throws Exception
    {
        RowType rowType = new RowType(
                Arrays.asList(new DataField(0, "a", new IntType()),
                        new DataField(1, "b", new BigIntType()),
                        // test field name has upper case
                        new DataField(2, "aCa", new VarCharType()),
                        new DataField(3, "d", new CharType(1))));
        return new SimpleTableTestHelper(tablePath, rowType);
    }

    private static void createSystemChangelogTable(Path tablePath)
            throws Exception
    {
        Schema schema = Schema.newBuilder()
                .column("pk", DataTypes.INT())
                .column("pt", DataTypes.INT())
                .column("col1", DataTypes.INT())
                .partitionKeys("pt")
                .primaryKey("pk", "pt")
                .option(CoreOptions.CHANGELOG_PRODUCER.key(), "input")
                .option(CoreOptions.TABLE_READ_SEQUENCE_NUMBER_ENABLED.key(), "true")
                .option("bucket", "1")
                .build();
        new SchemaManager(LocalFileIO.create(), tablePath).createTable(schema);

        FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath);
        InnerTableWrite writer = table.newWrite("user");
        InnerTableCommit commit = table.newCommit("user");
        writer.write(GenericRow.ofKind(RowKind.INSERT, 1, 1, 1));
        writer.write(GenericRow.ofKind(RowKind.DELETE, 1, 1, 1));
        writer.write(GenericRow.ofKind(RowKind.INSERT, 1, 2, 5));
        writer.write(GenericRow.ofKind(RowKind.UPDATE_BEFORE, 1, 2, 5));
        writer.write(GenericRow.ofKind(RowKind.UPDATE_AFTER, 1, 2, 6));
        writer.write(GenericRow.ofKind(RowKind.INSERT, 2, 3, 1));
        commit.commit(0, writer.prepareCommit(true, 0));
    }

    @Test
    public void testComplexTypes()
    {
        assertThat(sql("SELECT * FROM paimon.default.t4")).isEqualTo("[[1, {1=2}, [2, male], [1, 2, 3]]]");
    }

    @Test
    public void testEmptyTable()
    {
        assertThat(sql("SELECT * FROM paimon.default.empty_t")).isEqualTo("[]");
    }

    @Test
    public void testProjection()
    {
        assertThat(sql("SELECT * FROM paimon.default.t1")).isEqualTo("[[1, 2, 1, 1], [5, 6, 3, 3]]");
        assertThat(sql("SELECT a, aCa FROM paimon.default.t1")).isEqualTo("[[1, 1], [5, 3]]");
        assertThat(sql("SELECT SUM(b) FROM paimon.default.t1")).isEqualTo("[[8]]");
    }

    @Test
    public void testLimit()
    {
        assertThat(sql("SELECT * FROM paimon.default.t1 LIMIT 1")).isEqualTo("[[1, 2, 1, 1]]");
        assertThat(sql("SELECT * FROM paimon.default.t1 WHERE a = 5 LIMIT 1")).isEqualTo("[[5, 6, 3, 3]]");
    }

    @Test
    public void testSystemTable()
    {
        assertThat(sql("SELECT snapshot_id,schema_id,commit_user,commit_identifier,commit_kind FROM \"t1$snapshots\""))
                .isEqualTo("[[1, 0, user, 0, APPEND]]");
    }

    @Test
    public void testAuditLogSystemTable()
    {
        assertThat(sql("SHOW COLUMNS FROM paimon.default.\"system_changelog_values$audit_log\""))
                .isEqualTo("[[rowkind, varchar, , ], [_sequence_number, bigint, , ], [pk, integer, , ], [pt, integer, , ], [col1, integer, , ]]");
        assertThat(sql("SELECT rowkind, _sequence_number, pk, pt, col1 "
                + "FROM paimon.default.\"system_changelog_values$audit_log\" "
                + "ORDER BY _sequence_number"))
                .isEqualTo("[[+I, 0, 2, 3, 1], [-D, 1, 1, 1, 1], [+U, 2, 1, 2, 6]]");
    }

    @Test
    public void testBinlogSystemTable()
    {
        assertThat(sql("SHOW COLUMNS FROM paimon.default.\"system_changelog_values$binlog\""))
                .isEqualTo("[[rowkind, varchar, , ], [_sequence_number, bigint, , ], [pk, array(integer), , ], [pt, array(integer), , ], [col1, array(integer), , ]]");
        assertThat(sql("SELECT rowkind, _sequence_number, pk, pt, col1 "
                + "FROM paimon.default.\"system_changelog_values$binlog\" "
                + "ORDER BY _sequence_number"))
                .isEqualTo("[[+I, 0, [2], [3], [1]], [-D, 1, [1], [1], [1]], [+U, 2, [1], [2], [6]]]");
    }

    @Test
    public void testRowTrackingSystemTable()
    {
        sql("CREATE TABLE paimon.default.row_tracking_values ("
                + "id integer, "
                + "name varchar) "
                + "WITH (row_tracking_enabled = 'true')");
        sql("INSERT INTO paimon.default.row_tracking_values VALUES (11, 'alpha'), (22, 'beta')");

        assertThat(sql("SHOW COLUMNS FROM paimon.default.\"row_tracking_values$row_tracking\""))
                .isEqualTo("[[id, integer, , ], [name, varchar, , ], [_row_id, bigint, , ], [_sequence_number, bigint, , ]]");
        assertThat(sql("SELECT id, name, _row_id, _sequence_number "
                + "FROM paimon.default.\"row_tracking_values$row_tracking\" "
                + "ORDER BY id"))
                .isEqualTo("[[11, alpha, 0, 1], [22, beta, 1, 1]]");
    }

    @Test
    public void testRowTrackingHiddenColumnsOnBaseTable()
    {
        sql("CREATE TABLE paimon.default.row_tracking_values ("
                + "id integer, "
                + "name varchar) "
                + "WITH (row_tracking_enabled = 'true')");
        sql("INSERT INTO paimon.default.row_tracking_values VALUES (11, 'alpha'), (22, 'beta')");

        assertThat(sql("SHOW COLUMNS FROM paimon.default.row_tracking_values"))
                .isEqualTo("[[id, integer, , ], [name, varchar, , ]]");
        assertThat(sql("SELECT id, name, _row_id, _sequence_number "
                + "FROM paimon.default.row_tracking_values "
                + "ORDER BY id"))
                .isEqualTo("[[11, alpha, 0, 1], [22, beta, 1, 1]]");
        assertThat(sql("SELECT id, _row_id "
                + "FROM paimon.default.row_tracking_values "
                + "WHERE _row_id = BIGINT '0'"))
                .isEqualTo("[[11, 0]]");
        assertThat(sql("SELECT * FROM paimon.default.row_tracking_values ORDER BY id"))
                .isEqualTo("[[11, alpha], [22, beta]]");
    }

    @Test
    public void testRowTrackingHiddenColumnsOnOrcBaseTable()
    {
        assertRowTrackingHiddenColumnsOnFormattedBaseTable("row_tracking_values_orc", "ORC");
    }

    @Test
    public void testRowTrackingHiddenColumnsOnParquetBaseTable()
    {
        assertRowTrackingHiddenColumnsOnFormattedBaseTable("row_tracking_values_parquet", "PARQUET");
    }

    @Test
    public void testFilter()
    {
        assertThat(sql("SELECT a, aCa FROM paimon.default.t2 WHERE a < 4")).isEqualTo("[[1, 1], [3, 2]]");
    }

    private void assertRowTrackingHiddenColumnsOnFormattedBaseTable(String tableName, String fileFormat)
    {
        sql("CREATE TABLE paimon.default." + tableName + " ("
                + "id integer, "
                + "name varchar) "
                + "WITH (row_tracking_enabled = 'true', file_format = '" + fileFormat + "')");
        sql("INSERT INTO paimon.default." + tableName + " VALUES (11, 'alpha'), (22, 'beta')");

        assertThat(sql("SHOW COLUMNS FROM paimon.default." + tableName))
                .isEqualTo("[[id, integer, , ], [name, varchar, , ]]");
        assertThat(sql("SELECT id, name, _row_id, _sequence_number "
                + "FROM paimon.default." + tableName + " "
                + "ORDER BY id"))
                .isEqualTo("[[11, alpha, 0, 1], [22, beta, 1, 1]]");
        assertThat(sql("SELECT id, _row_id "
                + "FROM paimon.default." + tableName + " "
                + "WHERE _row_id = BIGINT '0'"))
                .isEqualTo("[[11, 0]]");
        assertThat(sql("SELECT * FROM paimon.default." + tableName + " ORDER BY id"))
                .isEqualTo("[[11, alpha], [22, beta]]");
    }

    @Test
    public void testGroupByWithCast()
    {
        assertThat(sql("SELECT pt, a, SUM(b), SUM(d) FROM paimon.default.t3 GROUP BY pt, a ORDER BY pt, a"))
                .isEqualTo("[[1, 1, 3, 3], [2, 3, 3, 3]]");
    }

    @Test
    public void testLimitWithPartition()
    {
        assertThat(sql("SELECT * FROM paimon.default.t3 WHERE pt = '1' LIMIT 1")).isEqualTo("[[1, 1, 1, 1, 1]]");

        assertThat(sql("SELECT * FROM paimon.default.t3 WHERE pt = '1' AND b = 2 LIMIT 1"))
                .isEqualTo("[[1, 1, 2, 2, 2]]");
    }

    @Test
    public void testShowCreateTable()
    {
        assertThat(sql("SHOW CREATE TABLE paimon.default.t3"))
                .isEqualTo("[[CREATE TABLE paimon.default.t3 (\n" + "   pt varchar,\n" + "   a integer,\n"
                        + "   b bigint,\n" + "   c bigint,\n" + "   d integer\n"
                        + ")\n"
                        + "WITH (\n"
                        + "   partitioned_by = ARRAY['pt']\n"
                        + ")]]");
    }

    @Test
    public void testShowCreateTableReflectsPaimonTableOptions()
            throws Exception
    {
        sql("CREATE TABLE paimon.default.show_create_option_values ("
                + "id integer, "
                + "picture varbinary) "
                + "WITH ("
                + "bucket = '4', "
                + "bucket_key = 'id', "
                + "primary_key = ARRAY['id'], "
                + "changelog_producer = 'input')");
        createBranch("default", "show_create_option_values", "branch_a");
        sql("ALTER TABLE paimon.default.show_create_option_values SET PROPERTIES scan_fallback_branch = 'branch_a'");

        assertThat(sql("SHOW CREATE TABLE paimon.default.show_create_option_values"))
                .contains("bucket = '4'")
                .contains("bucket_key = 'id'")
                .contains("changelog_producer = 'input'")
                .contains("scan_fallback_branch = 'branch_a'");
    }

    @Test
    public void testRuntimeReadSelectorsAreNotTableProperties()
    {
        sql("CREATE TABLE paimon.default.runtime_selector_properties (id integer)");

        assertQueryFails(
                "ALTER TABLE paimon.default.runtime_selector_properties SET PROPERTIES scan_snapshot_id = '7'",
                ".*Catalog 'paimon' table property 'scan_snapshot_id' does not exist.*");
        assertQueryFails(
                "ALTER TABLE paimon.default.runtime_selector_properties SET PROPERTIES incremental_between = '1,2'",
                ".*Catalog 'paimon' table property 'incremental_between' does not exist.*");
    }

    @Test
    public void testCreateSchema()
    {
        sql("CREATE SCHEMA paimon.test");
        assertThat(sql("SHOW SCHEMAS FROM paimon")).isEqualTo("[[default], [information_schema], [sys], [test]]");
        sql("DROP SCHEMA paimon.test");
    }

    @Test
    public void testDropSchema()
    {
        sql("CREATE SCHEMA paimon.tpch");
        sql("DROP SCHEMA paimon.tpch");
        assertThat(sql("SHOW SCHEMAS FROM paimon")).isEqualTo("[[default], [information_schema], [sys]]");
    }

    @Test
    public void testGlobalSystemTables()
    {
        assertThat(sql("SHOW TABLES FROM paimon.sys"))
                .isEqualTo("[[all_table_options], [catalog_options], [partitions], [tables]]");
        assertThat(sql("SHOW COLUMNS FROM paimon.sys.catalog_options"))
                .isEqualTo("[[key, varchar, , ], [value, varchar, , ]]");
    }

    @Test
    public void testBranchQualifiedTableReadWriteIsolation()
            throws Exception
    {
        sql("CREATE TABLE paimon.default.branch_values (id integer, name varchar) WITH (bucket = '-1')");
        sql("INSERT INTO paimon.default.branch_values VALUES (1, 'main-only')");
        createTag("default", "branch_values", "seed_tag");
        createBranch("default", "branch_values", "feature_branch", "seed_tag");

        assertThat(sql("SELECT branch_name FROM paimon.default.\"branch_values$branches\""))
                .isEqualTo("[[feature_branch]]");

        sql("INSERT INTO paimon.default.\"branch_values$branch_feature_branch\" VALUES (2, 'branch-only')");

        assertThat(sql("SELECT * FROM paimon.default.branch_values ORDER BY id"))
                .isEqualTo("[[1, main-only]]");
        assertThat(sql("SELECT * FROM paimon.default.\"branch_values$branch_feature_branch\" ORDER BY id"))
                .isEqualTo("[[1, main-only], [2, branch-only]]");
    }

    @Test
    public void testBranchQualifiedTableSupportsHistoricalRead()
            throws Exception
    {
        sql("CREATE TABLE paimon.default.branch_values (id integer, name varchar) WITH (bucket = '-1')");
        sql("INSERT INTO paimon.default.branch_values VALUES (1, 'main-only')");
        createTag("default", "branch_values", "seed_tag");
        createBranch("default", "branch_values", "feature_branch", "seed_tag");
        sql("INSERT INTO paimon.default.\"branch_values$branch_feature_branch\" VALUES (2, 'branch-only')");

        assertThat(sql("SELECT * FROM paimon.default.\"branch_values$branch_feature_branch\" FOR VERSION AS OF 'seed_tag' ORDER BY id"))
                .isEqualTo("[[1, main-only]]");
        assertThat(sql("SELECT * FROM paimon.default.\"branch_values$branch_feature_branch\" ORDER BY id"))
                .isEqualTo("[[1, main-only], [2, branch-only]]");
    }

    @Test
    public void testBranchesSystemTableColumnsAndFilter()
            throws Exception
    {
        sql("CREATE TABLE paimon.default.branch_values (id integer, name varchar) WITH (bucket = '-1')");
        createBranch("default", "branch_values", "feature_branch");

        assertThat(sql("SHOW COLUMNS FROM paimon.default.\"branch_values$branches\""))
                .isEqualTo("[[branch_name, varchar, , ], [create_time, timestamp(3), , ]]");
        assertThat(sql("SELECT branch_name FROM paimon.default.\"branch_values$branches\" WHERE branch_name = 'feature_branch'"))
                .isEqualTo("[[feature_branch]]");
    }

    @Test
    public void testTagsSystemTableColumnsAndFilter()
    {
        assertThat(sql("SHOW COLUMNS FROM paimon.default.\"t2$tags\""))
                .isEqualTo("[[tag_name, varchar, , ], [snapshot_id, bigint, , ], [schema_id, bigint, , ], "
                        + "[commit_time, timestamp(3), , ], [record_count, bigint, , ], [create_time, timestamp(3), , ], "
                        + "[time_retained, varchar, , ]]");
        assertThat(sql("SELECT tag_name, snapshot_id FROM paimon.default.\"t2$tags\" WHERE tag_name = 'tag-2'"))
                .isEqualTo("[[tag-2, 2]]");
    }

    @Test
    public void testVersionedQueriesRejectSystemTables()
    {
        assertThatExceptionOfType(QueryFailedException.class)
                .isThrownBy(() -> sql("SELECT * FROM paimon.default.\"t2$tags\" FOR VERSION AS OF 1"))
                .withMessageContaining(PaimonTableHandle.UNSUPPORTED_HISTORICAL_READ_MESSAGE);
        assertThatExceptionOfType(QueryFailedException.class)
                .isThrownBy(() -> sql("SELECT * FROM paimon.sys.catalog_options FOR VERSION AS OF 1"))
                .withMessageContaining(PaimonTableHandle.UNSUPPORTED_HISTORICAL_READ_MESSAGE);
    }

    @Test
    public void testBranchQualifiedTableSchemaEvolutionUsesBranchSchema()
            throws Exception
    {
        sql("CREATE TABLE paimon.default.branch_schema_values (id integer, name varchar) WITH (bucket = '-1')");
        sql("INSERT INTO paimon.default.branch_schema_values VALUES (1, 'main')");
        createTag("default", "branch_schema_values", "schema_seed");
        createBranch("default", "branch_schema_values", "schema_branch", "schema_seed");

        sql("ALTER TABLE paimon.default.\"branch_schema_values$branch_schema_branch\" ADD COLUMN branch_note varchar");
        sql("INSERT INTO paimon.default.\"branch_schema_values$branch_schema_branch\" VALUES (2, 'branch', 'note')");

        assertThat(sql("SHOW COLUMNS FROM paimon.default.branch_schema_values"))
                .isEqualTo("[[id, integer, , ], [name, varchar, , ]]");
        assertThat(sql("SHOW COLUMNS FROM paimon.default.\"branch_schema_values$branch_schema_branch\""))
                .isEqualTo("[[id, integer, , ], [name, varchar, , ], [branch_note, varchar, , ]]");

        assertThat(sql("SELECT * FROM paimon.default.branch_schema_values ORDER BY id"))
                .isEqualTo("[[1, main]]");
        assertThat(sql("SELECT * FROM paimon.default.\"branch_schema_values$branch_schema_branch\" ORDER BY id"))
                .isEqualTo("[[1, main, null], [2, branch, note]]");
    }

    @Test
    public void testBatchReadFallbackBranch()
            throws Exception
    {
        sql("CREATE TABLE paimon.default.branch_fallback_values (dt varchar, name varchar, amount bigint) "
                + "WITH (partitioned_by = ARRAY['dt'], bucket = '-1')");
        createBranch("default", "branch_fallback_values", "streaming_branch");

        sql("INSERT INTO paimon.default.\"branch_fallback_values$branch_streaming_branch\" VALUES "
                + "('20240725', 'apple', 4), "
                + "('20240725', 'peach', 10), "
                + "('20240726', 'cherry', 3), "
                + "('20240726', 'pear', 6)");
        sql("INSERT INTO paimon.default.branch_fallback_values VALUES "
                + "('20240725', 'apple', 5), "
                + "('20240725', 'banana', 7)");
        sql("ALTER TABLE paimon.default.branch_fallback_values SET PROPERTIES scan_fallback_branch = 'streaming_branch'");

        assertThat(sql("SELECT * FROM paimon.default.branch_fallback_values ORDER BY dt, name"))
                .isEqualTo("[[20240725, apple, 5], [20240725, banana, 7], "
                        + "[20240726, cherry, 3], [20240726, pear, 6]]");

        sql("ALTER TABLE paimon.default.branch_fallback_values SET PROPERTIES scan_fallback_branch = DEFAULT");

        assertThat(sql("SELECT * FROM paimon.default.branch_fallback_values ORDER BY dt, name"))
                .isEqualTo("[[20240725, apple, 5], [20240725, banana, 7]]");
    }

    @Test
    public void testInsertExistingPartitionsBehaviorErrorForPartitionedTable()
    {
        sql("CREATE TABLE paimon.default.insert_error_partitioned ("
                + "dt varchar, "
                + "id integer, "
                + "name varchar) "
                + "WITH (partitioned_by = ARRAY['dt'], bucket = '-1')");
        sql("INSERT INTO paimon.default.insert_error_partitioned VALUES ('20240725', 1, 'main')");

        Session errorSession = Session.builder(getSession())
                .setCatalogSessionProperty(CATALOG, PaimonSessionProperties.INSERT_EXISTING_PARTITIONS_BEHAVIOR, "error")
                .build();

        assertQueryFails(
                errorSession,
                "INSERT INTO paimon.default.insert_error_partitioned VALUES ('20240725', 2, 'conflict')",
                ".*Cannot insert into an existing partition of Paimon table: default.insert_error_partitioned.*");
        assertUpdate(
                errorSession,
                "INSERT INTO paimon.default.insert_error_partitioned VALUES ('20240726', 3, 'fresh')",
                1);

        assertThat(sql("SELECT * FROM paimon.default.insert_error_partitioned ORDER BY dt, id"))
                .isEqualTo("[[20240725, 1, main], [20240726, 3, fresh]]");
        sql("DROP TABLE paimon.default.insert_error_partitioned");
    }

    @Test
    public void testInsertExistingPartitionsBehaviorErrorForUnpartitionedTable()
    {
        sql("CREATE TABLE paimon.default.insert_error_unpartitioned (id integer, name varchar) WITH (bucket = '-1')");
        sql("INSERT INTO paimon.default.insert_error_unpartitioned VALUES (1, 'main')");

        Session errorSession = Session.builder(getSession())
                .setCatalogSessionProperty(CATALOG, PaimonSessionProperties.INSERT_EXISTING_PARTITIONS_BEHAVIOR, "ERROR")
                .build();

        assertQueryFails(
                errorSession,
                "INSERT INTO paimon.default.insert_error_unpartitioned VALUES (2, 'conflict')",
                ".*Cannot insert into an existing non-partitioned Paimon table: default.insert_error_unpartitioned.*");

        assertThat(sql("SELECT * FROM paimon.default.insert_error_unpartitioned"))
                .isEqualTo("[[1, main]]");
        sql("DROP TABLE paimon.default.insert_error_unpartitioned");
    }

    @Test
    public void testInsertExistingPartitionsBehaviorOverwriteRejectsUnsafePartitionOverwrite()
    {
        sql("CREATE TABLE paimon.default.insert_overwrite_guard ("
                + "dt varchar, "
                + "id integer, "
                + "name varchar) "
                + "WITH (partitioned_by = ARRAY['dt'], bucket = '-1', dynamic_partition_overwrite = 'false')");
        sql("INSERT INTO paimon.default.insert_overwrite_guard VALUES ('20240725', 1, 'main')");

        Session overwriteSession = Session.builder(getSession())
                .setCatalogSessionProperty(CATALOG, PaimonSessionProperties.INSERT_EXISTING_PARTITIONS_BEHAVIOR, "overwrite")
                .build();

        assertQueryFails(
                overwriteSession,
                "INSERT INTO paimon.default.insert_overwrite_guard VALUES ('20240725', 2, 'unsafe')",
                ".*Paimon insert overwrite requires dynamic-partition-overwrite=true for partitioned tables.*");

        assertThat(sql("SELECT * FROM paimon.default.insert_overwrite_guard"))
                .isEqualTo("[[20240725, 1, main]]");
        sql("DROP TABLE paimon.default.insert_overwrite_guard");
    }

    @Test
    public void testCreateTable()
    {
        sql("CREATE TABLE orders (" + "  order_key bigint," + "  order_status varchar," + "  total_price double,"
                + "  order_date date" + ")" + "WITH (" + "file_format = 'ORC',"
                + "primary_key = ARRAY['order_key','order_date']," + "partitioned_by = ARRAY['order_date'],"
                + "bucket = '2'," + "bucket_key = 'order_key'," + "changelog_producer = 'input'" + ")");
        assertThat(sql("SHOW TABLES FROM paimon.default")).contains("orders");
        sql("DROP TABLE IF EXISTS paimon.default.orders");
    }

    @Test
    public void testRenameTable()
    {
        sql("CREATE TABLE t5 (" + "  order_key bigint," + "  order_status varchar," + "  total_price double,"
                + "  order_date date" + ")" + "WITH (" + "file_format = 'ORC',"
                + "primary_key = ARRAY['order_key','order_date']," + "partitioned_by = ARRAY['order_date'],"
                + "bucket = '2'," + "bucket_key = 'order_key'," + "changelog_producer = 'input'" + ")");
        sql("ALTER TABLE paimon.default.t5 RENAME TO t6");
        String result = sql("SHOW TABLES FROM paimon.default");
        assertThat(result).doesNotContain("t5").contains("t6");
        sql("DROP TABLE IF EXISTS paimon.default.t6");
    }

    @Test
    public void testDropTable()
    {
        sql("CREATE TABLE t5 (" + "  order_key bigint," + "  order_status varchar," + "  total_price double,"
                + "  order_date date" + ")" + "WITH (" + "file_format = 'ORC',"
                + "primary_key = ARRAY['order_key','order_date']," + "partitioned_by = ARRAY['order_date'],"
                + "bucket = '2'," + "bucket_key = 'order_key'," + "changelog_producer = 'input'" + ")");
        sql("DROP TABLE IF EXISTS paimon.default.t5");
        assertThat(sql("SHOW TABLES FROM paimon.default")).doesNotContain("t5");
    }

    @Test
    public void testTruncateTable()
    {
        sql("CREATE TABLE paimon.default.truncate_values (id integer, name varchar) WITH (bucket = '-1')");
        sql("INSERT INTO paimon.default.truncate_values VALUES (1, 'one'), (2, 'two')");

        assertThat(sql("SELECT count(*) FROM paimon.default.truncate_values")).isEqualTo("[[2]]");

        sql("TRUNCATE TABLE paimon.default.truncate_values");

        assertThat(sql("SELECT count(*) FROM paimon.default.truncate_values")).isEqualTo("[[0]]");
    }

    @Test
    public void testDeleteAllRowsUsesMetadataDeleteForBucketUnawareTable()
    {
        sql("CREATE TABLE paimon.default.delete_all_bucket_unaware_values (id integer, name varchar)");
        sql("INSERT INTO paimon.default.delete_all_bucket_unaware_values VALUES (1, 'one'), (2, 'two')");

        sql("DELETE FROM paimon.default.delete_all_bucket_unaware_values");

        assertThat(sql("SELECT count(*) FROM paimon.default.delete_all_bucket_unaware_values")).isEqualTo("[[0]]");
    }

    @Test
    public void testFilteredDeleteDoesNotUseMetadataDeleteForBucketUnawareTable()
    {
        sql("CREATE TABLE paimon.default.filtered_delete_bucket_unaware_values (id integer, name varchar)");
        sql("INSERT INTO paimon.default.filtered_delete_bucket_unaware_values VALUES (1, 'one'), (2, 'two')");

        sql("DELETE FROM paimon.default.filtered_delete_bucket_unaware_values WHERE id = 99");

        assertThat(sql("SELECT count(*) FROM paimon.default.filtered_delete_bucket_unaware_values")).isEqualTo("[[2]]");

        assertQueryFails(
                "DELETE FROM paimon.default.filtered_delete_bucket_unaware_values WHERE id = 1",
                ".*Paimon metadata delete fallback can only delete all rows or complete partitions from an unlimited table handle.*");

        assertThat(sql("SELECT count(*) FROM paimon.default.filtered_delete_bucket_unaware_values")).isEqualTo("[[2]]");
    }

    @Test
    public void testMergeUpdateDoesNotUseMetadataDeleteForBucketUnawareTable()
    {
        sql("CREATE TABLE paimon.default.merge_update_bucket_unaware_values (id integer, name varchar)");
        sql("INSERT INTO paimon.default.merge_update_bucket_unaware_values VALUES (1, 'one'), (2, 'two')");

        assertQueryFails(
                "MERGE INTO paimon.default.merge_update_bucket_unaware_values t "
                        + "USING (VALUES (1, 'updated')) s(id, name) "
                        + "ON t.id = s.id "
                        + "WHEN MATCHED THEN UPDATE SET name = s.name",
                ".*Paimon metadata-delete merge sink only supports DELETE rows.*");

        assertThat(sql("SELECT * FROM paimon.default.merge_update_bucket_unaware_values ORDER BY id"))
                .isEqualTo("[[1, one], [2, two]]");
    }

    @Test
    public void testMergeDeleteAllRowsUsesMetadataDeleteFallbackForBucketUnawareTable()
    {
        sql("CREATE TABLE paimon.default.merge_delete_bucket_unaware_values (id integer, name varchar)");
        sql("INSERT INTO paimon.default.merge_delete_bucket_unaware_values VALUES (1, 'one'), (2, 'two')");

        sql("MERGE INTO paimon.default.merge_delete_bucket_unaware_values t "
                + "USING (VALUES 1) s(marker) "
                + "ON true "
                + "WHEN MATCHED THEN DELETE");

        assertThat(sql("SELECT count(*) FROM paimon.default.merge_delete_bucket_unaware_values")).isEqualTo("[[0]]");
    }

    @Test
    public void testTableAndColumnComments()
    {
        sql("CREATE TABLE paimon.default.comment_values ("
                + "id integer COMMENT 'identifier', "
                + "name varchar) "
                + "COMMENT 'table comment' "
                + "WITH (bucket = '-1')");

        assertThat(sql("SELECT comment FROM system.metadata.table_comments "
                + "WHERE catalog_name = 'paimon' AND schema_name = 'default' AND table_name = 'comment_values'"))
                .isEqualTo("[[table comment]]");
        assertThat(sql("SHOW COLUMNS FROM paimon.default.comment_values"))
                .isEqualTo("[[id, integer, , identifier], [name, varchar, , ]]");

        sql("COMMENT ON TABLE paimon.default.comment_values IS 'updated table comment'");
        assertThat(sql("SELECT comment FROM system.metadata.table_comments "
                + "WHERE catalog_name = 'paimon' AND schema_name = 'default' AND table_name = 'comment_values'"))
                .isEqualTo("[[updated table comment]]");
        assertThat(sql("SHOW CREATE TABLE paimon.default.comment_values"))
                .contains("COMMENT 'updated table comment'");

        sql("COMMENT ON COLUMN paimon.default.comment_values.name IS 'display name'");
        assertThat(sql("SHOW COLUMNS FROM paimon.default.comment_values"))
                .isEqualTo("[[id, integer, , identifier], [name, varchar, , display name]]");

        sql("ALTER TABLE paimon.default.comment_values ADD COLUMN detail varchar COMMENT 'detail column'");
        assertThat(sql("SHOW COLUMNS FROM paimon.default.comment_values"))
                .isEqualTo("[[id, integer, , identifier], [name, varchar, , display name], [detail, varchar, , detail column]]");

        sql("COMMENT ON COLUMN paimon.default.comment_values.name IS NULL");
        assertThat(sql("SHOW COLUMNS FROM paimon.default.comment_values"))
                .isEqualTo("[[id, integer, , identifier], [name, varchar, , ], [detail, varchar, , detail column]]");

        sql("COMMENT ON TABLE paimon.default.comment_values IS NULL");
        assertThat(sql("SELECT comment IS NULL FROM system.metadata.table_comments "
                + "WHERE catalog_name = 'paimon' AND schema_name = 'default' AND table_name = 'comment_values'"))
                .isEqualTo("[[true]]");
    }

    @Test
    public void testColumnCommentDirectiveDoesNotChangeExistingLogicalType()
    {
        sql("CREATE TABLE paimon.default.comment_directive_values ("
                + "id integer, "
                + "embedding array(real), "
                + "picture varbinary) "
                + "WITH (file_format = 'json', file_compression = 'none')");

        sql("COMMENT ON COLUMN paimon.default.comment_directive_values.embedding IS '__VECTOR_FIELD;3; display vector'");
        sql("COMMENT ON COLUMN paimon.default.comment_directive_values.picture IS '__BLOB_FIELD; display blob'");

        assertThat(sql("SHOW COLUMNS FROM paimon.default.comment_directive_values")).isEqualTo(
                "[[id, integer, , ], [embedding, array(real), , __VECTOR_FIELD;3; display vector], [picture, varbinary, , __BLOB_FIELD; display blob]]");
        sql("INSERT INTO paimon.default.comment_directive_values VALUES "
                + "(1, ARRAY[CAST(1.0 AS real), CAST(2.0 AS real)], X'CAFE')");
        assertThat(sql("SELECT id, embedding, to_hex(picture) FROM paimon.default.comment_directive_values"))
                .isEqualTo("[[1, [1.0, 2.0], CAFE]]");
    }

    @Test
    public void testCreateOrReplaceTable()
    {
        sql("CREATE OR REPLACE TABLE paimon.default.replace_values (id integer, name varchar) WITH (bucket = '-1')");
        assertThat(sql("SELECT count(*) FROM paimon.default.replace_values")).isEqualTo("[[0]]");

        sql("INSERT INTO paimon.default.replace_values VALUES (1, 'one'), (2, 'two')");
        assertThat(sql("SELECT count(*) FROM paimon.default.replace_values")).isEqualTo("[[2]]");

        sql("CREATE OR REPLACE TABLE paimon.default.replace_values AS SELECT 3 id, 'three' name");
        assertThat(sql("SELECT * FROM paimon.default.replace_values")).isEqualTo("[[3, three]]");
    }

    @Test
    public void testHashFixedDeleteAndMerge()
    {
        sql("CREATE TABLE paimon.default.hash_fixed_mutations ("
                + "id integer, "
                + "name varchar, "
                + "score integer) "
                + "WITH (primary_key = ARRAY['id'], bucket = '1', bucket_key = 'id')");
        sql("INSERT INTO paimon.default.hash_fixed_mutations VALUES "
                + "(1, 'one', 10), (2, 'two', 20), (3, 'three', 30)");

        sql("DELETE FROM paimon.default.hash_fixed_mutations WHERE score = 20");
        assertThat(sql("SELECT * FROM paimon.default.hash_fixed_mutations ORDER BY id"))
                .isEqualTo("[[1, one, 10], [3, three, 30]]");

        sql("UPDATE paimon.default.hash_fixed_mutations SET score = score + 1 WHERE name = 'one'");
        assertThat(sql("SELECT * FROM paimon.default.hash_fixed_mutations ORDER BY id"))
                .isEqualTo("[[1, one, 11], [3, three, 30]]");

        sql("MERGE INTO paimon.default.hash_fixed_mutations t "
                + "USING (VALUES (1, 'one-updated', 11), (3, 'three-deleted', -1), (4, 'four', 40)) "
                + "AS s(id, name, score) "
                + "ON (t.id = s.id) "
                + "WHEN MATCHED AND s.score < 0 THEN DELETE "
                + "WHEN MATCHED THEN UPDATE SET name = s.name, score = s.score "
                + "WHEN NOT MATCHED THEN INSERT (id, name, score) VALUES (s.id, s.name, s.score)");

        assertThat(sql("SELECT * FROM paimon.default.hash_fixed_mutations ORDER BY id"))
                .isEqualTo("[[1, one-updated, 11], [4, four, 40]]");
    }

    @Test
    public void testHashDynamicInsert()
    {
        sql("CREATE TABLE paimon.default.hash_dynamic_writes ("
                + "id integer, "
                + "name varchar) "
                + "WITH (primary_key = ARRAY['id'], bucket = '-1')");

        sql("INSERT INTO paimon.default.hash_dynamic_writes VALUES (1, 'one'), (2, 'two')");

        assertThat(sql("SELECT * FROM paimon.default.hash_dynamic_writes ORDER BY id"))
                .isEqualTo("[[1, one], [2, two]]");
    }

    @Test
    public void testKeyDynamicCrossPartitionInsertDeleteUpdateAndMerge()
    {
        String tableName = "key_dynamic_mutations_" + UUID.randomUUID().toString().replace('-', '_');
        String table = "paimon.default." + tableName;
        try {
            sql("CREATE TABLE " + table + " ("
                    + "dt integer, id integer, name varchar, score integer) WITH ("
                    + "partitioned_by = ARRAY['dt'], primary_key = ARRAY['id'], bucket = '-1', "
                    + "dynamic_bucket_assigner_parallelism = '2', dynamic_bucket_initial_buckets = '2', "
                    + "cross_partition_upsert_bootstrap_parallelism = '1')");
            sql("INSERT INTO " + table + " VALUES "
                    + "(10, 1, 'one', 10), (20, 2, 'two', 20)");

            // The same primary key moves from partition 10 to partition 30. The global index
            // must emit a delete in the old partition and exactly one new visible row.
            sql("INSERT INTO " + table + " VALUES (30, 1, 'one-moved', 11), (20, 3, 'three', 30)");
            assertThat(sql("SELECT dt, id, name, score FROM " + table + " ORDER BY id"))
                    .isEqualTo("[[30, 1, one-moved, 11], [20, 2, two, 20], [20, 3, three, 30]]");

            sql("DELETE FROM " + table + " WHERE id = 2");
            sql("UPDATE " + table + " SET name = 'one-updated', score = 12 WHERE id = 1");
            sql("MERGE INTO " + table + " t "
                    + "USING (VALUES (40, 1, 'one-final', 13), (50, 4, 'four', 40)) "
                    + "AS s(dt, id, name, score) ON (t.id = s.id) "
                    + "WHEN MATCHED THEN UPDATE SET dt = s.dt, name = s.name, score = s.score "
                    + "WHEN NOT MATCHED THEN INSERT (dt, id, name, score) VALUES (s.dt, s.id, s.name, s.score)");

            assertThat(sql("SELECT dt, id, name, score FROM " + table + " ORDER BY id"))
                    .isEqualTo("[[40, 1, one-final, 13], [20, 3, three, 30], [50, 4, four, 40]]");
        }
        finally {
            sql("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testKeyDynamicInsertOverwrite()
    {
        String tableName = "key_dynamic_overwrite_" + UUID.randomUUID().toString().replace('-', '_');
        String table = "paimon.default." + tableName;
        try {
            sql("CREATE TABLE " + table + " ("
                    + "dt integer, id integer, name varchar) WITH ("
                    + "partitioned_by = ARRAY['dt'], primary_key = ARRAY['id'], bucket = '-1', "
                    + "dynamic_bucket_assigner_parallelism = '2', dynamic_bucket_initial_buckets = '2', "
                    + "cross_partition_upsert_bootstrap_parallelism = '1')");
            sql("INSERT INTO " + table + " VALUES (10, 1, 'old'), (20, 2, 'stale')");

            Session overwriteSession = Session.builder(getSession())
                    .setCatalogSessionProperty(CATALOG, PaimonSessionProperties.INSERT_EXISTING_PARTITIONS_BEHAVIOR, "overwrite")
                    .build();
            getQueryRunner().execute(
                    overwriteSession,
                    "INSERT INTO " + table + " VALUES (10, 3, 'new'), (20, 4, 'fresh')");

            assertThat(sql("SELECT dt, id, name FROM " + table + " ORDER BY id"))
                    .isEqualTo("[[10, 3, new], [20, 4, fresh]]");
        }
        finally {
            sql("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testKeyDynamicCreateTableAsSelect()
    {
        String tableName = "key_dynamic_ctas_" + UUID.randomUUID().toString().replace('-', '_');
        String table = "paimon.default." + tableName;
        try {
            sql("CREATE TABLE " + table + " WITH ("
                    + "primary_key = ARRAY['id'], bucket = '-1', "
                    + "dynamic_bucket_assigner_parallelism = '2', dynamic_bucket_initial_buckets = '2') AS "
                    + "SELECT * FROM (VALUES (10, 1, 'one'), (20, 2, 'two')) "
                    + "AS source(dt, id, name)");

            assertThat(sql("SELECT dt, id, name FROM " + table + " ORDER BY id"))
                    .isEqualTo("[[10, 1, one], [20, 2, two]]");
        }
        finally {
            sql("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testHashDynamicInsertOverwrite()
    {
        sql("CREATE TABLE paimon.default.hash_dynamic_overwrite ("
                + "id integer, "
                + "name varchar) "
                + "WITH (primary_key = ARRAY['id'], bucket = '-1')");
        sql("INSERT INTO paimon.default.hash_dynamic_overwrite VALUES (1, 'old'), (2, 'stale')");

        Session overwriteSession = Session.builder(getSession())
                .setCatalogSessionProperty(CATALOG, PaimonSessionProperties.INSERT_EXISTING_PARTITIONS_BEHAVIOR, "overwrite")
                .build();
        getQueryRunner().execute(
                overwriteSession,
                "INSERT INTO paimon.default.hash_dynamic_overwrite VALUES (3, 'new'), (4, 'fresh')");

        assertThat(sql("SELECT * FROM paimon.default.hash_dynamic_overwrite ORDER BY id"))
                .isEqualTo("[[3, new], [4, fresh]]");
    }

    @Test
    public void testHashDynamicDeleteUpdateAndMerge()
    {
        sql("CREATE TABLE paimon.default.hash_dynamic_mutations ("
                + "id integer, "
                + "name varchar, "
                + "score integer) "
                + "WITH (primary_key = ARRAY['id'], bucket = '-1')");

        sql("INSERT INTO paimon.default.hash_dynamic_mutations VALUES "
                + "(1, 'one', 10), (2, 'two', 20), (3, 'three', 30)");

        assertThat(sql("SELECT * FROM paimon.default.hash_dynamic_mutations ORDER BY id"))
                .isEqualTo("[[1, one, 10], [2, two, 20], [3, three, 30]]");

        sql("DELETE FROM paimon.default.hash_dynamic_mutations WHERE id = 3");
        assertThat(sql("SELECT * FROM paimon.default.hash_dynamic_mutations ORDER BY id"))
                .isEqualTo("[[1, one, 10], [2, two, 20]]");

        sql("UPDATE paimon.default.hash_dynamic_mutations SET name = 'two-updated', score = 22 WHERE id = 2");
        assertThat(sql("SELECT * FROM paimon.default.hash_dynamic_mutations ORDER BY id"))
                .isEqualTo("[[1, one, 10], [2, two-updated, 22]]");

        sql("MERGE INTO paimon.default.hash_dynamic_mutations t "
                + "USING (VALUES (1, 'one-updated', 11), (2, 'two-deleted', -1), (4, 'four', 40)) "
                + "AS s(id, name, score) "
                + "ON (t.id = s.id) "
                + "WHEN MATCHED AND s.score < 0 THEN DELETE "
                + "WHEN MATCHED THEN UPDATE SET name = s.name, score = s.score "
                + "WHEN NOT MATCHED THEN INSERT (id, name, score) VALUES (s.id, s.name, s.score)");

        assertThat(sql("SELECT * FROM paimon.default.hash_dynamic_mutations ORDER BY id"))
                .isEqualTo("[[1, one-updated, 11], [4, four, 40]]");
    }

    @Test
    public void testHashDynamicWriterOwnershipAcrossInitialBuckets()
            throws IOException
    {
        String initialOneTable = "hash_dynamic_initial_one_" + UUID.randomUUID().toString().replace('-', '_');
        String initialTwoTable = "hash_dynamic_initial_two_" + UUID.randomUUID().toString().replace('-', '_');
        String initialOneName = "paimon.default." + initialOneTable;
        String initialTwoName = "paimon.default." + initialTwoTable;

        try {
            sql("CREATE TABLE " + initialOneName + " ("
                    + "id integer, name varchar, score integer) WITH ("
                    + "primary_key = ARRAY['id'], bucket = '-1', "
                    + "dynamic_bucket_assigner_parallelism = '2', "
                    + "dynamic_bucket_initial_buckets = '1')");
            sql("INSERT INTO " + initialOneName + " VALUES "
                    + "(1, 'one', 10), (2, 'two', 20), (3, 'three', 30), (4, 'four', 40)");
            assertThat(sql("SELECT id, name, score FROM " + initialOneName + " ORDER BY id"))
                    .isEqualTo("[[1, one, 10], [2, two, 20], [3, three, 30], [4, four, 40]]");

            Session overwriteSession = Session.builder(getSession())
                    .setCatalogSessionProperty(CATALOG, PaimonSessionProperties.INSERT_EXISTING_PARTITIONS_BEHAVIOR, "overwrite")
                    .build();
            getQueryRunner().execute(
                    overwriteSession,
                    "INSERT INTO " + initialOneName + " VALUES (10, 'ten', 100), (11, 'eleven', 110)");
            assertThat(sql("SELECT id, name, score FROM " + initialOneName + " ORDER BY id"))
                    .isEqualTo("[[10, ten, 100], [11, eleven, 110]]");

            sql("UPDATE " + initialOneName + " SET name = 'ten-updated', score = 101 WHERE id = 10");
            assertThat(sql("SELECT id, name, score FROM " + initialOneName + " ORDER BY id"))
                    .isEqualTo("[[10, ten-updated, 101], [11, eleven, 110]]");
            sql("DELETE FROM " + initialOneName + " WHERE id = 11");
            assertThat(sql("SELECT id, name, score FROM " + initialOneName + " ORDER BY id"))
                    .isEqualTo("[[10, ten-updated, 101]]");
            sql("MERGE INTO " + initialOneName + " t "
                    + "USING (VALUES (10, 'ten-final', 102), (12, 'twelve', 120)) "
                    + "AS s(id, name, score) ON (t.id = s.id) "
                    + "WHEN MATCHED THEN UPDATE SET name = s.name, score = s.score "
                    + "WHEN NOT MATCHED THEN INSERT (id, name, score) VALUES (s.id, s.name, s.score)");
            assertThat(sql("SELECT id, name, score FROM " + initialOneName + " ORDER BY id"))
                    .isEqualTo("[[10, ten-final, 102], [12, twelve, 120]]");
            assertPaimonTableHasCommittedFiles(initialOneTable);

            sql("CREATE TABLE " + initialTwoName + " ("
                    + "ds varchar, id integer, name varchar, score integer) WITH ("
                    + "partitioned_by = ARRAY['ds'], primary_key = ARRAY['ds', 'id'], bucket = '-1', "
                    + "dynamic_bucket_assigner_parallelism = '2', "
                    + "dynamic_bucket_initial_buckets = '2')");
            sql("INSERT INTO " + initialTwoName + " VALUES "
                    + "('a', 1, 'a-one', 10), ('a', 2, 'a-two', 20), "
                    + "('b', 1, 'b-one', 30), ('b', 2, 'b-two', 40)");
            assertThat(sql("SELECT ds, id, name, score FROM " + initialTwoName + " ORDER BY ds, id"))
                    .isEqualTo("[[a, 1, a-one, 10], [a, 2, a-two, 20], [b, 1, b-one, 30], [b, 2, b-two, 40]]");

            sql("UPDATE " + initialTwoName + " SET name = 'a-one-updated', score = 11 WHERE ds = 'a' AND id = 1");
            assertThat(sql("SELECT ds, id, name, score FROM " + initialTwoName + " ORDER BY ds, id"))
                    .isEqualTo("[[a, 1, a-one-updated, 11], [a, 2, a-two, 20], [b, 1, b-one, 30], [b, 2, b-two, 40]]");
            sql("DELETE FROM " + initialTwoName + " WHERE ds = 'b' AND id = 2");
            assertThat(sql("SELECT ds, id, name, score FROM " + initialTwoName + " ORDER BY ds, id"))
                    .isEqualTo("[[a, 1, a-one-updated, 11], [a, 2, a-two, 20], [b, 1, b-one, 30]]");
            sql("MERGE INTO " + initialTwoName + " t "
                    + "USING (VALUES ('a', 1, 'a-one-final', 12), ('b', 3, 'b-three', 50)) "
                    + "AS s(ds, id, name, score) ON (t.ds = s.ds AND t.id = s.id) "
                    + "WHEN MATCHED THEN UPDATE SET name = s.name, score = s.score "
                    + "WHEN NOT MATCHED THEN INSERT (ds, id, name, score) VALUES (s.ds, s.id, s.name, s.score)");
            assertThat(sql("SELECT ds, id, name, score FROM " + initialTwoName + " ORDER BY ds, id"))
                    .isEqualTo("[[a, 1, a-one-final, 12], [a, 2, a-two, 20], [b, 1, b-one, 30], [b, 3, b-three, 50]]");

            getQueryRunner().execute(
                    overwriteSession,
                    "INSERT INTO " + initialTwoName + " VALUES ('b', 4, 'b-four', 60)");
            assertThat(sql("SELECT ds, id, name, score FROM " + initialTwoName + " ORDER BY ds, id"))
                    .isEqualTo("[[a, 1, a-one-final, 12], [a, 2, a-two, 20], [b, 4, b-four, 60]]");
            assertPaimonTableHasCommittedFiles(initialTwoTable);
        }
        finally {
            sql("DROP TABLE IF EXISTS " + initialTwoName);
            sql("DROP TABLE IF EXISTS " + initialOneName);
        }
    }

    private void assertPaimonTableHasCommittedFiles(String tableName)
            throws IOException
    {
        java.nio.file.Path tablePath = java.nio.file.Path.of(URI.create(warehouse))
                .resolve(DB + ".db")
                .resolve(tableName);
        try (var files = Files.walk(tablePath)) {
            List<String> fileNames = files.filter(Files::isRegularFile)
                    .map(path -> path.getFileName().toString())
                    .toList();
            assertThat(fileNames.stream().anyMatch(name -> name.startsWith("data-")))
                    .as("Paimon table %s should contain a committed data file", tableName)
                    .isTrue();
            assertThat(fileNames.stream().anyMatch(name -> name.startsWith("index-")))
                    .as("Paimon table %s should contain a committed index file", tableName)
                    .isTrue();
            assertThat(fileNames.stream().anyMatch(name -> name.startsWith("manifest-")))
                    .as("Paimon table %s should contain a committed manifest file", tableName)
                    .isTrue();
        }
    }

    @Test
    public void testNotNullInsertValidation()
    {
        sql("CREATE TABLE paimon.default.not_null_values ("
                + "nullable_col integer, "
                + "not_null_col integer NOT NULL) "
                + "WITH (bucket = '-1')");

        assertThat(sql("SHOW CREATE TABLE paimon.default.not_null_values"))
                .contains("not_null_col integer NOT NULL");
        sql("INSERT INTO paimon.default.not_null_values (not_null_col) VALUES (2)");
        assertThat(sql("SELECT nullable_col, not_null_col FROM paimon.default.not_null_values"))
                .isEqualTo("[[null, 2]]");

        assertThatExceptionOfType(QueryFailedException.class)
                .isThrownBy(() -> sql("INSERT INTO paimon.default.not_null_values (nullable_col) VALUES (1)"))
                .withMessageContaining("not_null_col");
        assertThatExceptionOfType(QueryFailedException.class)
                .isThrownBy(() -> sql("INSERT INTO paimon.default.not_null_values "
                        + "(not_null_col, nullable_col) VALUES (NULL, 3)"))
                .withMessageContaining("NULL value not allowed for NOT NULL column: not_null_col");
    }

    @Test
    public void testInsertMissingColumnsUsesPaimonDefaultValues()
            throws Exception
    {
        Path tablePath = new Path(warehouse, "default.db/insert_default_values");
        DataField defaultStatusField = new DataField(1, "status", DataTypes.STRING()).newDefaultValue("'new'");
        new SchemaManager(LocalFileIO.create(), tablePath).createTable(new Schema(
                List.of(new DataField(0, "id", DataTypes.INT()), defaultStatusField),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.singletonMap("bucket", "-1"),
                ""));

        sql("INSERT INTO paimon.default.insert_default_values (id) VALUES (1)");

        assertThat(sql("SELECT id, status FROM paimon.default.insert_default_values"))
                .isEqualTo("[[1, new]]");
    }

    @Test
    public void testAddNotNullColumnFailsFast()
    {
        sql("CREATE TABLE paimon.default.not_null_values (id integer) WITH (bucket = '-1')");

        assertThatExceptionOfType(QueryFailedException.class)
                .isThrownBy(() -> sql("ALTER TABLE paimon.default.not_null_values "
                        + "ADD COLUMN required_value integer NOT NULL"))
                .withMessageContaining("This connector does not support adding not null columns");
    }

    @Test
    public void testAddColumn()
    {
        sql("CREATE TABLE t5 (" + "  order_key bigint," + "  order_status varchar," + "  total_price double,"
                + "  order_date date" + ")" + "WITH (" + "file_format = 'ORC',"
                + "primary_key = ARRAY['order_key','order_date']," + "partitioned_by = ARRAY['order_date'],"
                + "bucket = '2'," + "bucket_key = 'order_key'," + "changelog_producer = 'input'" + ")");
        sql("INSERT INTO paimon.default.t5 (order_key, order_status, total_price, order_date) "
                + "VALUES (1, 'old', 11.0, DATE '2026-06-11')");
        sql("ALTER TABLE paimon.default.t5 ADD COLUMN zip varchar");
        sql("INSERT INTO paimon.default.t5 (order_key, order_status, total_price, order_date, zip) "
                + "VALUES (2, 'new', 22.0, DATE '2026-06-12', '94107')");
        assertThat(sql("SHOW COLUMNS FROM paimon.default.t5")).isEqualTo(
                "[[order_key, bigint, , ], [order_status, varchar, , ], [total_price, double, , ], [order_date, date, , ], [zip, varchar, , ]]");
        assertThat(sql("SELECT order_key, order_status, zip FROM paimon.default.t5 ORDER BY order_key"))
                .isEqualTo("[[1, old, null], [2, new, 94107]]");
        sql("DROP TABLE IF EXISTS paimon.default.t5");
    }

    @Test
    public void testRenameColumn()
    {
        sql("CREATE TABLE t5 (" + "  order_key bigint," + "  order_status varchar," + "  total_price double,"
                + "  order_date date" + ")" + "WITH (" + "file_format = 'ORC',"
                + "primary_key = ARRAY['order_key','order_date']," + "partitioned_by = ARRAY['order_date'],"
                + "bucket = '2'," + "bucket_key = 'order_key'," + "changelog_producer = 'input'" + ")");
        sql("ALTER TABLE paimon.default.t5 RENAME COLUMN order_status to g");
        assertThat(sql("SHOW COLUMNS FROM paimon.default.t5")).isEqualTo(
                "[[order_key, bigint, , ], [g, varchar, , ], [total_price, double, , ], [order_date, date, , ]]");
        sql("DROP TABLE IF EXISTS paimon.default.t5");
    }

    @Test
    public void testDropColumn()
    {
        sql("CREATE TABLE t5 (" + "  order_key bigint," + "  order_status varchar," + "  total_price double,"
                + "  order_date date" + ")" + "WITH (" + "file_format = 'ORC',"
                + "primary_key = ARRAY['order_key','order_date']," + "partitioned_by = ARRAY['order_date'],"
                + "bucket = '2'," + "bucket_key = 'order_key'," + "changelog_producer = 'input'" + ")");
        sql("ALTER TABLE paimon.default.t5 DROP COLUMN order_status");
        assertThat(sql("SHOW COLUMNS FROM paimon.default.t5"))
                .isEqualTo("[[order_key, bigint, , ], [total_price, double, , ], [order_date, date, , ]]");
        sql("DROP TABLE IF EXISTS paimon.default.t5");
    }

    @Test
    public void testSetTableProperties()
    {
        sql("CREATE TABLE t5 (" + "  order_key bigint," + "  order_status varchar," + "  total_price double,"
                + "  order_date date" + ")" + "WITH (" + "file_format = 'ORC',"
                + "primary_key = ARRAY['order_key','order_date']," + "partitioned_by = ARRAY['order_date'],"
                + "bucket = '2'," + "bucket_key = 'order_key'," + "changelog_producer = 'input'" + ")");
        sql("ALTER TABLE paimon.default.t5 SET PROPERTIES bucket = '4',snapshot_time_retained = '4h'");
        sql("DROP TABLE IF EXISTS paimon.default.t5");
    }

    @Test
    public void testDropNotNullConstraint()
    {
        sql("CREATE TABLE paimon.default.drop_nn_values ("
                + "id integer, "
                + "required_col integer NOT NULL) "
                + "WITH (bucket = '-1')");

        // Verify NOT NULL is enforced
        assertThatExceptionOfType(QueryFailedException.class)
                .isThrownBy(() -> sql("INSERT INTO paimon.default.drop_nn_values (id) VALUES (1)"))
                .withMessageContaining("required_col");

        // Drop the NOT NULL constraint
        sql("ALTER TABLE paimon.default.drop_nn_values ALTER COLUMN required_col DROP NOT NULL");

        // Now null values should be accepted
        sql("INSERT INTO paimon.default.drop_nn_values (id) VALUES (1)");
        assertThat(sql("SELECT id, required_col FROM paimon.default.drop_nn_values"))
                .isEqualTo("[[1, null]]");
    }

    @Test
    public void testNestedFieldOperations()
    {
        sql("CREATE TABLE paimon.default.nested_field_values ("
                + "id integer, "
                + "info row(name varchar, age integer, city varchar)) "
                + "WITH (bucket = '-1')");
        sql("INSERT INTO paimon.default.nested_field_values VALUES "
                + "(1, ROW('alice', 30, 'NYC'))");

        // Verify initial state
        assertThat(sql("SELECT id, info.name, info.age, info.city FROM paimon.default.nested_field_values"))
                .isEqualTo("[[1, alice, 30, NYC]]");

        // Drop nested field: dropField
        sql("ALTER TABLE paimon.default.nested_field_values DROP COLUMN info.city");
        assertThat(sql("SELECT id, info.name, info.age FROM paimon.default.nested_field_values"))
                .isEqualTo("[[1, alice, 30]]");

        // Rename nested field: renameField (verify schema change, not data migration)
        sql("ALTER TABLE paimon.default.nested_field_values RENAME COLUMN info.name TO full_name");
        assertThat(sql("SHOW COLUMNS FROM paimon.default.nested_field_values"))
                .contains("[info, row(\"full_name\" varchar, \"age\" integer), , ]");

        // Insert data with new schema to verify rename works for new writes
        sql("INSERT INTO paimon.default.nested_field_values VALUES (2, ROW('bob', 25))");
        assertThat(sql("SELECT id, info.full_name, info.age FROM paimon.default.nested_field_values WHERE id = 2"))
                .isEqualTo("[[2, bob, 25]]");

        // Change nested field type: setFieldType
        sql("ALTER TABLE paimon.default.nested_field_values ALTER COLUMN info.age SET DATA TYPE bigint");
        assertThat(sql("SHOW COLUMNS FROM paimon.default.nested_field_values"))
                .contains("[info, row(\"full_name\" varchar, \"age\" bigint), , ]");

        // Add nested field: addField
        sql("ALTER TABLE paimon.default.nested_field_values ADD COLUMN info.email varchar");
        assertThat(sql("SHOW COLUMNS FROM paimon.default.nested_field_values"))
                .contains("[info, row(\"full_name\" varchar, \"age\" bigint, \"email\" varchar), , ]");

        // Verify new writes work with full schema
        sql("INSERT INTO paimon.default.nested_field_values VALUES (3, ROW('charlie', 35, 'charlie@test.com'))");
        assertThat(sql("SELECT id, info.full_name, info.age, info.email FROM paimon.default.nested_field_values WHERE id = 3"))
                .isEqualTo("[[3, charlie, 35, charlie@test.com]]");
    }

    @Test
    public void testAllType()
    {
        assertThat(sql("SELECT boolean, tinyint, smallint,int,bigint,float,double,char,varchar, date,timestamp_0, "
                + "timestamp_3, timestamp_6, decimal, to_hex(varbinary), array, map, row FROM paimon.default.t99"))
                .isEqualTo("[[true, 1, 1, 1, 1, 1.0, 1.0, char1, varchar1, 1970-01-01, "
                        + "2023-09-12T07:54:48, 2023-09-12T07:54:48.001, 2023-09-12T07:54:48.001001, "
                        + "0.10000, 010203, [1, 1, 1], {1=1}, [1, 1]]]");
    }

    @Test
    public void testOrcTimeType()
    {
        sql("CREATE TABLE paimon.default.time_orc_values ("
                + "id integer, "
                + "time_value time(3)) "
                + "WITH (file_format = 'ORC')");

        assertThatExceptionOfType(QueryFailedException.class)
                .isThrownBy(() -> sql("INSERT INTO paimon.default.time_orc_values VALUES "
                        + "(1, TIME '00:00:12.345'), "
                        + "(2, TIME '23:59:59.999')"))
                .withMessageContaining("Trino Paimon ORC writer does not support Paimon TIME columns");
    }

    @Test
    public void testSqlCreateInsertReadUsesTrinoFileFormatForParquetAndOrc()
            throws Exception
    {
        assertSqlCreateInsertReadUsesTrinoFileFormat("format_sql_parquet_values", "PARQUET");
        assertSqlCreateInsertReadUsesTrinoFileFormat("format_sql_orc_values", "ORC");
    }

    @Test
    public void testJsonVariantType()
    {
        sql("CREATE TABLE paimon.default.json_values ("
                + "id integer, "
                + "payload json, "
                + "nested array(json)) "
                + "WITH (file_format = 'PARQUET')");

        assertThat(sql("SHOW COLUMNS FROM paimon.default.json_values")).isEqualTo(
                "[[id, integer, , ], [payload, json, , ], [nested, array(json), , ]]");
        assertQueryFails(
                "INSERT INTO paimon.default.json_values VALUES "
                        + "(1, JSON '{\"name\":\"alice\",\"numbers\":[1,2,3]}', ARRAY[JSON '{\"kind\":\"home\"}', JSON '42'])",
                ".*Paimon write uses features which are not supported by the Trino connector.*");
    }

    @Test
    public void testJsonVariantNestedTypesUsePaimonVariantSchema()
            throws Exception
    {
        sql("CREATE TABLE paimon.default.json_nested_values ("
                + "id integer, "
                + "payload json, "
                + "metadata map(varchar, json), "
                + "details row(label varchar, data json), "
                + "nested array(json)) "
                + "WITH (file_format = 'PARQUET')");

        assertThat(sql("SHOW COLUMNS FROM paimon.default.json_nested_values")).isEqualTo(
                "[[id, integer, , ], [payload, json, , ], [metadata, map(varchar, json), , ], [details, row(\"label\" varchar, \"data\" json), , ], [nested, array(json), , ]]");
        assertQueryFails(
                "INSERT INTO paimon.default.json_nested_values VALUES "
                        + "("
                        + "1, "
                        + "JSON '{\"name\":\"alice\",\"active\":true}', "
                        + "MAP(ARRAY['home', 'work'], ARRAY[JSON '{\"city\":\"shenzhen\"}', JSON '{\"city\":\"hangzhou\"}']), "
                        + "CAST(ROW('primary', JSON '{\"level\":3}') AS ROW(label varchar, data json)), "
                        + "ARRAY[JSON '{\"kind\":\"home\"}', JSON '42'])",
                ".*Paimon write uses features which are not supported by the Trino connector.*");

        List<DataField> fields = loadTable("default", "json_nested_values").schema().fields();
        assertThat(fieldType(fields, "payload").getTypeRoot()).isEqualTo(DataTypeRoot.VARIANT);

        DataType metadataType = fieldType(fields, "metadata");
        assertThat(metadataType.getTypeRoot()).isEqualTo(DataTypeRoot.MAP);
        assertThat(DataTypeChecks.getNestedTypes(metadataType).get(1).getTypeRoot()).isEqualTo(DataTypeRoot.VARIANT);

        DataType detailsType = fieldType(fields, "details");
        assertThat(detailsType.getTypeRoot()).isEqualTo(DataTypeRoot.ROW);
        assertThat(DataTypeChecks.getNestedTypes(detailsType).get(1).getTypeRoot()).isEqualTo(DataTypeRoot.VARIANT);

        DataType nestedType = fieldType(fields, "nested");
        assertThat(nestedType.getTypeRoot()).isEqualTo(DataTypeRoot.ARRAY);
        assertThat(DataTypeChecks.getNestedTypes(nestedType).get(0).getTypeRoot()).isEqualTo(DataTypeRoot.VARIANT);
    }

    @Test
    public void testJsonVariantSchemaEvolutionOnAddedColumn()
            throws Exception
    {
        sql("CREATE TABLE paimon.default.json_variant_evolution_values ("
                + "id integer, "
                + "name varchar) "
                + "WITH (file_format = 'PARQUET')");
        sql("INSERT INTO paimon.default.json_variant_evolution_values VALUES "
                + "(1, 'alpha'), "
                + "(2, 'beta')");

        sql("ALTER TABLE paimon.default.json_variant_evolution_values ADD COLUMN attrs json");

        assertThat(sql("SHOW COLUMNS FROM paimon.default.json_variant_evolution_values")).isEqualTo(
                "[[id, integer, , ], [name, varchar, , ], [attrs, json, , ]]");
        assertThat(sql("SELECT id, name, json_format(attrs) "
                + "FROM paimon.default.json_variant_evolution_values ORDER BY id"))
                .isEqualTo("[[1, alpha, null], [2, beta, null]]");
        assertQueryFails(
                "INSERT INTO paimon.default.json_variant_evolution_values VALUES "
                        + "(3, 'gamma', JSON '{\"kind\":\"keep\",\"score\":3}')",
                ".*Paimon write uses features which are not supported by the Trino connector.*");

        List<DataField> fields = loadTable("default", "json_variant_evolution_values").schema().fields();
        assertThat(fieldType(fields, "attrs").getTypeRoot()).isEqualTo(DataTypeRoot.VARIANT);
    }

    @Test
    public void testCsvFileFormatReadWrite()
    {
        sql("CREATE TABLE paimon.default.csv_values ("
                + "id integer, "
                + "name varchar, "
                + "score bigint) "
                + "WITH (file_format = 'csv', file_compression = 'none')");
        sql("INSERT INTO paimon.default.csv_values VALUES "
                + "(1, 'alice', 10), "
                + "(2, 'bob', 20)");

        assertThat(sql("SHOW COLUMNS FROM paimon.default.csv_values")).isEqualTo(
                "[[id, integer, , ], [name, varchar, , ], [score, bigint, , ]]");
        assertThat(sql("SELECT * FROM paimon.default.csv_values ORDER BY id"))
                .isEqualTo("[[1, alice, 10], [2, bob, 20]]");
        assertThat(sql("SELECT name FROM paimon.default.csv_values WHERE score = 20"))
                .isEqualTo("[[bob]]");
    }

    @Test
    public void testDirectReadFilterOnUnprojectedColumnFallsBackToPaimonReader()
    {
        sql("CREATE TABLE paimon.default.direct_filter_values ("
                + "id integer, "
                + "category varchar, "
                + "payload varchar) "
                + "WITH (file_format = 'PARQUET')");
        sql("INSERT INTO paimon.default.direct_filter_values VALUES "
                + "(1, 'keep', 'alpha'), "
                + "(2, 'drop', 'beta'), "
                + "(3, 'keep', 'gamma')");

        assertThat(sql("SELECT id, payload FROM paimon.default.direct_filter_values "
                + "WHERE category = 'keep' ORDER BY id"))
                .isEqualTo("[[1, alpha], [3, gamma]]");
    }

    @Test
    public void testDirectParquetReadWithDuplicateProjectedFilterColumn()
    {
        sql("CREATE TABLE paimon.default.direct_duplicate_projection_filter_values ("
                + "id bigint, "
                + "payload varchar) "
                + "WITH (file_format = 'PARQUET')");
        sql("INSERT INTO paimon.default.direct_duplicate_projection_filter_values VALUES "
                + "(1, 'alpha'), "
                + "(2, 'beta'), "
                + "(3, 'gamma')");

        assertThat(sql("SELECT id, id FROM paimon.default.direct_duplicate_projection_filter_values "
                + "WHERE id = 2"))
                .isEqualTo("[[2, 2]]");
    }

    @Test
    public void testSchemaEvolutionFilterOnAddedColumnSkipsOldFilesOnParquetBaseTable()
    {
        assertSchemaEvolutionFilterOnAddedColumnSkipsOldFiles("direct_filter_schema_evolution", "PARQUET");
    }

    @Test
    public void testSchemaEvolutionFilterOnAddedColumnSkipsOldFilesOnOrcBaseTable()
    {
        assertSchemaEvolutionFilterOnAddedColumnSkipsOldFiles("direct_filter_schema_evolution_orc", "ORC");
    }

    @Test
    public void testSchemaEvolutionProjectedAddedColumnReadsOldFilesOnParquetBaseTable()
    {
        assertSchemaEvolutionProjectedAddedColumnReadsOldFiles("direct_projection_schema_evolution", "PARQUET");
    }

    @Test
    public void testSchemaEvolutionProjectedAddedColumnReadsOldFilesOnOrcBaseTable()
    {
        assertSchemaEvolutionProjectedAddedColumnReadsOldFiles("direct_projection_schema_evolution_orc", "ORC");
    }

    @Test
    public void testSchemaEvolutionTypeChangeUsesPaimonReaderOnParquetBaseTable()
    {
        assertSchemaEvolutionTypeChangeUsesPaimonReader("direct_type_evolution", "PARQUET");
    }

    @Test
    public void testSchemaEvolutionTypeChangeUsesPaimonReaderOnOrcBaseTable()
    {
        assertSchemaEvolutionTypeChangeUsesPaimonReader("direct_type_evolution_orc", "ORC");
    }

    private void assertSchemaEvolutionFilterOnAddedColumnSkipsOldFiles(String tableName, String fileFormat)
    {
        sql("CREATE TABLE paimon.default." + tableName + " ("
                + "id integer, "
                + "payload varchar) "
                + "WITH (file_format = '" + fileFormat + "')");
        sql("INSERT INTO paimon.default." + tableName + " VALUES "
                + "(1, 'alpha'), "
                + "(2, 'beta')");
        sql("ALTER TABLE paimon.default." + tableName + " ADD COLUMN category varchar");
        sql("INSERT INTO paimon.default." + tableName + " VALUES "
                + "(3, 'gamma', 'keep'), "
                + "(4, 'delta', 'drop')");

        assertThat(sql("SELECT id, payload FROM paimon.default." + tableName + " "
                + "WHERE category = 'keep' ORDER BY id"))
                .isEqualTo("[[3, gamma]]");
        assertThat(sql("SELECT id FROM paimon.default." + tableName + " "
                + "WHERE category = 'missing'"))
                .isEqualTo("[]");
    }

    private void assertSchemaEvolutionProjectedAddedColumnReadsOldFiles(String tableName, String fileFormat)
    {
        sql("CREATE TABLE paimon.default." + tableName + " ("
                + "id integer, "
                + "payload varchar) "
                + "WITH (file_format = '" + fileFormat + "')");
        sql("INSERT INTO paimon.default." + tableName + " VALUES "
                + "(1, 'alpha'), "
                + "(2, 'beta')");

        sql("ALTER TABLE paimon.default." + tableName + " ADD COLUMN category varchar");
        sql("INSERT INTO paimon.default." + tableName + " VALUES "
                + "(3, 'gamma', 'keep'), "
                + "(4, 'delta', 'drop')");

        assertThat(sql("SELECT id, category FROM paimon.default." + tableName + " ORDER BY id"))
                .isEqualTo("[[1, null], [2, null], [3, keep], [4, drop]]");
        assertThat(sql("SELECT id, category FROM paimon.default." + tableName + " "
                + "WHERE id <= 2 ORDER BY id"))
                .isEqualTo("[[1, null], [2, null]]");
        assertThat(sql("SELECT id FROM paimon.default." + tableName + " "
                + "WHERE category IS NULL ORDER BY id"))
                .isEqualTo("[[1], [2]]");
    }

    private void assertSchemaEvolutionTypeChangeUsesPaimonReader(String tableName, String fileFormat)
    {
        sql("CREATE TABLE paimon.default." + tableName + " ("
                + "id integer, "
                + "payload integer) "
                + "WITH (file_format = '" + fileFormat + "')");
        sql("INSERT INTO paimon.default." + tableName + " VALUES "
                + "(1, 101), "
                + "(2, 202)");

        sql("ALTER TABLE paimon.default." + tableName + " ALTER COLUMN payload SET DATA TYPE varchar");
        sql("INSERT INTO paimon.default." + tableName + " VALUES "
                + "(3, 'new-303'), "
                + "(4, 'new-404')");

        assertThat(sql("SELECT id, payload FROM paimon.default." + tableName + " ORDER BY id"))
                .isEqualTo("[[1, 101], [2, 202], [3, new-303], [4, new-404]]");
        assertThat(sql("SELECT id FROM paimon.default." + tableName + " WHERE payload = '101'"))
                .isEqualTo("[[1]]");
    }

    private void assertSqlCreateInsertReadUsesTrinoFileFormat(String tableName, String fileFormat)
            throws Exception
    {
        sql("CREATE TABLE paimon.default." + tableName + " ("
                + "id integer, "
                + "name varchar, "
                + "score bigint) "
                + "WITH (file_format = '" + fileFormat + "', bucket = '-1')");
        sql("INSERT INTO paimon.default." + tableName + " VALUES "
                + "(1, 'alpha', 11), "
                + "(2, 'beta', 22), "
                + "(3, 'gamma', 33)");

        assertThat(sql("SELECT id, name, score FROM paimon.default." + tableName + " ORDER BY id"))
                .isEqualTo("[[1, alpha, 11], [2, beta, 22], [3, gamma, 33]]");

        Path dataFile = onlyDataFilePath(tableName);
        Slice data = Slices.wrappedBuffer(Files.readAllBytes(java.nio.file.Path.of(dataFile.toUri())));
        if ("PARQUET".equals(fileFormat)) {
            assertThat(MetadataReader.readFooter(
                            new SliceParquetDataSource(data, ParquetReaderOptions.defaultOptions()),
                            Optional.empty())
                    .getFileMetaData()
                    .getCreatedBy())
                    .contains(TRINO_PAIMON_WRITER_VERSION);
            return;
        }
        assertThat(OrcReader.createOrcReader(
                        new MemoryOrcDataSource(new OrcDataSourceId(dataFile.toString()), data),
                        new OrcReaderOptions())
                .orElseThrow()
                .getFooter()
                .getUserMetadata()
                .get(TRINO_PAIMON_ORC_WRITER_METADATA_KEY)
                .toStringUtf8())
                .isEqualTo(TRINO_PAIMON_WRITER_VERSION);
    }

    private Path onlyDataFilePath(String tableName)
            throws Exception
    {
        FileStoreTable table = loadTable("default", tableName);
        List<Split> splits = new ArrayList<>(table.newScan().plan().splits());
        assertThat(splits).hasSize(1);
        DataSplit split = (DataSplit) splits.get(0);
        assertThat(split.dataFiles()).hasSize(1);
        return new Path(split.bucketPath(), split.dataFiles().get(0).fileName());
    }

    @Test
    public void testFilesystemCatalogViewCreateFailsFast()
    {
        assertThatExceptionOfType(QueryFailedException.class)
                .isThrownBy(() -> sql("CREATE VIEW paimon.default.order_view AS SELECT 1 value"))
                .withMessageContaining("This connector does not support creating views")
                .withMessageContaining("Paimon catalog does not support view create operations");
    }

    @Test
    public void testVectorType()
    {
        assertThat(sql("SHOW COLUMNS FROM paimon.default.vector_values")).isEqualTo(
                "[[id, integer, , ], [embedding, array(real), , ]]");
        assertThat(sql("SELECT id, embedding FROM paimon.default.vector_values"))
                .isEqualTo("[[1, [1.0, 2.5, 3.75]]]");

        sql("INSERT INTO paimon.default.vector_values VALUES "
                + "(2, ARRAY[CAST(4.0 AS real), CAST(5.5 AS real), CAST(6.25 AS real)])");

        assertThat(sql("SELECT id, embedding FROM paimon.default.vector_values ORDER BY id"))
                .isEqualTo("[[1, [1.0, 2.5, 3.75]], [2, [4.0, 5.5, 6.25]]]");
    }

    @Test
    public void testVectorColumnDirectiveOnCreateTable()
    {
        sql("CREATE TABLE paimon.default.vector_directive_values ("
                + "id integer, "
                + "embedding array(real) COMMENT '__VECTOR_FIELD;3; embedding vector') "
                + "WITH (file_format = 'json', file_compression = 'none')");
        sql("INSERT INTO paimon.default.vector_directive_values VALUES "
                + "(1, ARRAY[CAST(1.0 AS real), CAST(2.5 AS real), CAST(3.75 AS real)])");

        assertThat(sql("SHOW COLUMNS FROM paimon.default.vector_directive_values")).isEqualTo(
                "[[id, integer, , ], [embedding, array(real), , embedding vector]]");
        assertThat(sql("SELECT id, embedding FROM paimon.default.vector_directive_values"))
                .isEqualTo("[[1, [1.0, 2.5, 3.75]]]");
        assertThatExceptionOfType(QueryFailedException.class)
                .isThrownBy(() -> sql("INSERT INTO paimon.default.vector_directive_values VALUES "
                        + "(2, ARRAY[CAST(1.0 AS real), CAST(2.5 AS real)])"))
                .withMessageContaining("Paimon VECTOR length mismatch: expected 3, got 2");
    }

    @Test
    public void testVectorColumnDirectiveOnAddColumn()
    {
        sql("CREATE TABLE paimon.default.vector_directive_add_column ("
                + "id integer) WITH (file_format = 'json', file_compression = 'none')");
        sql("ALTER TABLE paimon.default.vector_directive_add_column "
                + "ADD COLUMN embedding array(real) COMMENT '__VECTOR_FIELD;3; added embedding'");
        sql("INSERT INTO paimon.default.vector_directive_add_column VALUES "
                + "(1, ARRAY[CAST(1.0 AS real), CAST(2.5 AS real), CAST(3.75 AS real)])");

        assertThat(sql("SHOW COLUMNS FROM paimon.default.vector_directive_add_column")).isEqualTo(
                "[[id, integer, , ], [embedding, array(real), , added embedding]]");
        assertThat(sql("SELECT id, embedding FROM paimon.default.vector_directive_add_column"))
                .isEqualTo("[[1, [1.0, 2.5, 3.75]]]");
        assertThatExceptionOfType(QueryFailedException.class)
                .isThrownBy(() -> sql("INSERT INTO paimon.default.vector_directive_add_column VALUES "
                        + "(2, ARRAY[CAST(1.0 AS real), CAST(2.5 AS real)])"))
                .withMessageContaining("Paimon VECTOR length mismatch: expected 3, got 2");
    }

    @Test
    public void testBlobColumnDirectiveOnCreateTable()
    {
        sql("CREATE TABLE paimon.default.blob_directive_values ("
                + "id integer, "
                + "picture varbinary COMMENT '__BLOB_FIELD; profile picture') "
                + "WITH (data_evolution_enabled = 'true', row_tracking_enabled = 'true')");
        sql("INSERT INTO paimon.default.blob_directive_values VALUES "
                + "(1, X'48656C6C6F')");

        assertThat(sql("SHOW COLUMNS FROM paimon.default.blob_directive_values")).isEqualTo(
                "[[id, integer, , ], [picture, varbinary, , profile picture]]");
        assertThat(sql("SELECT id, to_hex(picture) FROM paimon.default.blob_directive_values"))
                .isEqualTo("[[1, 48656C6C6F]]");
    }

    @Test
    public void testBlobColumnDirectiveOnAddColumn()
    {
        sql("CREATE TABLE paimon.default.blob_directive_add_column ("
                + "id integer) WITH (data_evolution_enabled = 'true', row_tracking_enabled = 'true')");
        sql("ALTER TABLE paimon.default.blob_directive_add_column "
                + "ADD COLUMN picture varbinary COMMENT '__BLOB_FIELD; added picture'");
        sql("INSERT INTO paimon.default.blob_directive_add_column VALUES "
                + "(1, X'5945')");

        assertThat(sql("SHOW COLUMNS FROM paimon.default.blob_directive_add_column")).isEqualTo(
                "[[id, integer, , ], [picture, varbinary, , added picture]]");
        assertThat(sql("SELECT id, to_hex(picture) FROM paimon.default.blob_directive_add_column"))
                .isEqualTo("[[1, 5945]]");
    }

    @Test
    public void testTimeTravel()
    {
        assertThat(sql("SELECT * FROM paimon.default.t2 FOR VERSION AS OF 1"))
                .isEqualTo("[[1, 2, 1, 1], [3, 4, 2, 2]]");
        assertThat(sql("SELECT * FROM paimon.default.t2 FOR VERSION AS OF 2"))
                .isEqualTo("[[1, 2, 1, 1], [3, 4, 2, 2], [5, 6, 3, 3], [7, 8, 4, 4]]");

        assertThat(sql("SELECT * FROM paimon.default.t2 FOR TIMESTAMP AS OF TIMESTAMP "
                + timestampLiteral(t2FirstCommitTimestamp, 6))).isEqualTo("[[1, 2, 1, 1], [3, 4, 2, 2]]");
        assertThat(sql("SELECT * FROM paimon.default.t2 FOR TIMESTAMP AS OF TIMESTAMP "
                + timestampLiteral(System.currentTimeMillis(), 6)))
                .isEqualTo("[[1, 2, 1, 1], [3, 4, 2, 2], [5, 6, 3, 3], [7, 8, 4, 4]]");
    }

    @Test
    public void testIncrementalRead()
    {
        assertThatExceptionOfType(QueryFailedException.class).isThrownBy(
                        () -> sql("SELECT * FROM TABLE(paimon.system.table_changes(schema_name=>'default',table_name=>'t2'))"))
                .withMessage("One of INCREMENTAL_BETWEEN, INCREMENTAL_BETWEEN_TIMESTAMP or INCREMENTAL_TO_AUTO_TAG must be provided");
        assertThat(sql(
                "SELECT * FROM TABLE(paimon.system.table_changes(schema_name=>'default',table_name=>'t2',incremental_between=>'1,2'))"))
                .isEqualTo("[[5, 6, 3, 3], [7, 8, 4, 4]]");
        assertThat(sql(
                "SELECT * FROM TABLE(paimon.system.table_changes(schema_name=>'default',table_name=>'t2',incremental_between=>'1,tag-2'))"))
                .isEqualTo("[[5, 6, 3, 3], [7, 8, 4, 4]]");
        assertThat(sql(
                "SELECT * FROM TABLE(paimon.system.table_changes(schema_name=>'default',table_name=>'t2',incremental_between_timestamp=>'%s,%s'))"
                        .formatted(t2FirstCommitTimestamp, System.currentTimeMillis())))
                .isEqualTo("[[5, 6, 3, 3], [7, 8, 4, 4]]");
    }

    @Test
    public void testIncrementalReadBetweenTagsAsSnapshots()
    {
        assertThat(sql(
                "SELECT * FROM TABLE(paimon.system.table_changes("
                        + "schema_name=>'default',"
                        + "table_name=>'t2',"
                        + "incremental_between=>'1,tag-2',"
                        + "incremental_between_tag_to_snapshot=>true))"))
                .isEqualTo("[[5, 6, 3, 3], [7, 8, 4, 4]]");

        assertThat(sql(
                "SELECT * FROM TABLE(paimon.system.table_changes("
                        + "schema_name=>'default',"
                        + "table_name=>'t2',"
                        + "incremental_between=>'1,tag-2',"
                        + "incremental_between_tag_to_snapshot=>true,"
                        + "incremental_between_scan_mode=>'delta'))"))
                .isEqualTo("[[5, 6, 3, 3], [7, 8, 4, 4]]");
    }

    @Test
    public void testIncrementalReadBetweenTagsDefaultsToTagDiffUnlessTagToSnapshotEnabled()
            throws Exception
    {
        Path tablePath = new Path(warehouse, "default.db/incremental_tag_snapshot_values");
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("group_id", DataTypes.INT())
                .column("score", DataTypes.BIGINT())
                .primaryKey("id")
                .option(CoreOptions.BUCKET.key(), "1")
                .option(CoreOptions.BUCKET_KEY.key(), "id")
                .option(CoreOptions.CHANGELOG_PRODUCER.key(), "lookup")
                .build();
        new SchemaManager(LocalFileIO.create(), tablePath).createTable(schema);

        FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath);
        InnerTableWrite writer = table.newWrite("user");
        writer.withIOManager(new IOManagerImpl("/tmp"));
        InnerTableCommit commit = table.newCommit("user");

        writer.write(GenericRow.of(1, 10, 100L));
        writer.write(GenericRow.of(2, 20, 200L));
        writer.write(GenericRow.of(3, 40, 400L));
        commit.commit(0, writer.prepareCommit(true, 0));
        createTag("default", "incremental_tag_snapshot_values", "tag-from-snapshot-1");

        writer.write(GenericRow.of(1, 10, 100L));
        writer.write(GenericRow.of(2, 20, 200L));
        writer.write(GenericRow.of(3, 40, 500L));
        commit.commit(1, writer.prepareCommit(true, 1));
        createTag("default", "incremental_tag_snapshot_values", "tag-from-snapshot-2");

        assertThat(sql(
                "SELECT * FROM TABLE(paimon.system.table_changes("
                        + "schema_name=>'default',"
                        + "table_name=>'incremental_tag_snapshot_values',"
                        + "incremental_between=>'tag-from-snapshot-1,tag-from-snapshot-2',"
                        + "incremental_between_scan_mode=>'delta')) "
                        + "ORDER BY id, score"))
                .isEqualTo("[[3, 40, 500]]");

        assertThat(sql(
                "SELECT * FROM TABLE(paimon.system.table_changes("
                        + "schema_name=>'default',"
                        + "table_name=>'incremental_tag_snapshot_values',"
                        + "incremental_between=>'tag-from-snapshot-1,tag-from-snapshot-2',"
                        + "incremental_between_tag_to_snapshot=>true,"
                        + "incremental_between_scan_mode=>'delta')) "
                        + "ORDER BY id, score"))
                .isEqualTo("[[1, 10, 100], [2, 20, 200], [3, 40, 500]]");
    }

    @Test
    public void testIncrementalReadToAutoTag()
            throws Exception
    {
        Path tablePath = new Path(warehouse, "default.db/incremental_auto_tag_values");
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("name", DataTypes.STRING())
                .primaryKey("id")
                .option(CoreOptions.BUCKET.key(), "1")
                .option(CoreOptions.TAG_CREATION_PERIOD.key(), "daily")
                .build();
        new SchemaManager(LocalFileIO.create(), tablePath).createTable(schema);

        FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath);
        InnerTableWrite writer = table.newWrite("user");
        InnerTableCommit commit = table.newCommit("user");

        writer.write(GenericRow.of(1, fromString("alpha")));
        commit.commit(0, writer.prepareCommit(true, 0));
        table.createTag("2024-12-01");

        writer.write(GenericRow.of(2, fromString("beta")));
        commit.commit(1, writer.prepareCommit(true, 1));
        table.createTag("2024-12-02");

        writer.write(GenericRow.of(3, fromString("gamma")));
        commit.commit(2, writer.prepareCommit(true, 2));
        table.createTag("2024-12-04");

        assertThat(sql(
                "SELECT * FROM TABLE(paimon.system.table_changes("
                        + "schema_name=>'default',"
                        + "table_name=>'incremental_auto_tag_values',"
                        + "incremental_to_auto_tag=>'2024-12-01'))"))
                .isEqualTo("[]");
        assertThat(sql(
                "SELECT * FROM TABLE(paimon.system.table_changes("
                        + "schema_name=>'default',"
                        + "table_name=>'incremental_auto_tag_values',"
                        + "incremental_to_auto_tag=>'2024-12-02'))"))
                .isEqualTo("[[2, beta]]");
        assertThat(sql(
                "SELECT * FROM TABLE(paimon.system.table_changes("
                        + "schema_name=>'default',"
                        + "table_name=>'incremental_auto_tag_values',"
                        + "incremental_to_auto_tag=>'2024-12-03'))"))
                .isEqualTo("[]");
        assertThat(sql(
                "SELECT * FROM TABLE(paimon.system.table_changes("
                        + "schema_name=>'default',"
                        + "table_name=>'incremental_auto_tag_values',"
                        + "incremental_to_auto_tag=>'2024-12-04'))"))
                .isEqualTo("[[3, gamma]]");
    }

    @Test
    public void testIncrementalReadIgnoresConflictingSessionTimeTravelProperties()
    {
        Session conflictingSession = Session.builder(getSession())
                .setCatalogSessionProperty(CATALOG, PaimonSessionProperties.SCAN_SNAPSHOT, "1")
                .setCatalogSessionProperty(CATALOG, PaimonSessionProperties.SCAN_TAG, "tag-2")
                .build();

        assertThat(computeActual(
                conflictingSession,
                "SELECT * FROM TABLE(paimon.system.table_changes(schema_name=>'default',table_name=>'t2',incremental_between=>'1,2'))")
                .getMaterializedRows().toString())
                .isEqualTo("[[5, 6, 3, 3], [7, 8, 4, 4]]");
    }

    @Test
    public void testIncrementalReadUsesLatestSchemaAfterSchemaChange()
    {
        sql("CREATE TABLE paimon.default.incremental_schema_evolution (id integer, name varchar)");
        sql("INSERT INTO paimon.default.incremental_schema_evolution VALUES (1, 'alpha'), (2, 'beta')");
        sql("ALTER TABLE paimon.default.incremental_schema_evolution DROP COLUMN name");
        sql("INSERT INTO paimon.default.incremental_schema_evolution VALUES (3), (4)");
        sql("ALTER TABLE paimon.default.incremental_schema_evolution ADD COLUMN comment varchar");
        sql("INSERT INTO paimon.default.incremental_schema_evolution VALUES (5, 'fifth'), (6, 'sixth')");

        long latestSnapshotId = (long) computeActual(
                "SELECT max(snapshot_id) FROM paimon.default.\"incremental_schema_evolution$snapshots\"")
                .getOnlyValue();

        String incrementalSchemaEvolutionQuery =
                """
                SELECT * FROM TABLE(paimon.system.table_changes(
                        schema_name=>'default',
                        table_name=>'incremental_schema_evolution',
                        incremental_between=>'1,%s')) ORDER BY id
                """.formatted(latestSnapshotId);
        String incrementalSchemaEvolutionDeltaQuery =
                """
                SELECT * FROM TABLE(paimon.system.table_changes(
                        schema_name=>'default',
                        table_name=>'incremental_schema_evolution',
                        incremental_between=>'1,%s',
                        incremental_between_scan_mode=>'delta')) ORDER BY id
                """.formatted(latestSnapshotId);

        assertThat(sql(incrementalSchemaEvolutionQuery))
                .isEqualTo("[[3, null], [4, null], [5, fifth], [6, sixth]]");
        assertThat(sql(incrementalSchemaEvolutionDeltaQuery))
                .isEqualTo("[[3, null], [4, null], [5, fifth], [6, sixth]]");
    }

    @Test
    public void testTimeTravelWithTag()
    {
        // tag or snapshotId is string
        assertThat(sql("SELECT * FROM paimon.default.t2 FOR VERSION AS OF '1'"))
                .isEqualTo("[[1, 2, 1, 1], [3, 4, 2, 2]]");
        assertThat(sql("SELECT * FROM paimon.default.t2 FOR VERSION AS OF 'tag-2'"))
                .isEqualTo("[[1, 2, 1, 1], [3, 4, 2, 2], [5, 6, 3, 3], [7, 8, 4, 4]]");
        // tag or snapshotId is int
        assertThat(sql("SELECT * FROM paimon.default.t2 FOR VERSION AS OF 1"))
                .isEqualTo("[[1, 2, 1, 1], [3, 4, 2, 2]]");
    }

    @Test
    public void testTimeTravelVersionPrefersTagOverSnapshotIdWithSameToken()
    {
        assertThat(sql("SELECT * FROM paimon.default.t_version_precedence FOR VERSION AS OF 2"))
                .isEqualTo("[[1, 2, 1, 1], [3, 4, 2, 2]]");
        assertThat(sql("SELECT * FROM paimon.default.t_version_precedence FOR VERSION AS OF '2'"))
                .isEqualTo("[[1, 2, 1, 1], [3, 4, 2, 2]]");
    }

    @Test
    public void testTimeTravelUsesHistoricalSchemaAfterAddColumn()
    {
        sql("CREATE TABLE paimon.default.time_travel_schema_evolution (id integer, name varchar)");
        sql("INSERT INTO paimon.default.time_travel_schema_evolution VALUES (1, 'hello'), (2, 'paimon')");
        sql("ALTER TABLE paimon.default.time_travel_schema_evolution ADD COLUMN dt varchar");
        sql("INSERT INTO paimon.default.time_travel_schema_evolution VALUES (3, 'trino', '0401'), (4, 'spark', '0402')");

        assertThat(sql("SELECT * FROM paimon.default.time_travel_schema_evolution"))
                .isEqualTo("[[1, hello, null], [2, paimon, null], [3, trino, 0401], [4, spark, 0402]]");
        assertThat(sql("SELECT * FROM paimon.default.time_travel_schema_evolution FOR VERSION AS OF 1"))
                .isEqualTo("[[1, hello], [2, paimon]]");
    }

    @Test
    public void testSessionSnapshotTimeTravel()
    {
        Session snapshotSession = Session.builder(getSession())
                .setCatalogSessionProperty(CATALOG, PaimonSessionProperties.SCAN_SNAPSHOT, "1")
                .build();

        assertThat(computeActual(snapshotSession, "SELECT * FROM paimon.default.t2").getMaterializedRows().toString())
                .isEqualTo("[[1, 2, 1, 1], [3, 4, 2, 2]]");
    }

    @Test
    public void testSessionTimestampTimeTravel()
    {
        Session timestampSession = Session.builder(getSession())
                .setCatalogSessionProperty(
                        CATALOG,
                        PaimonSessionProperties.SCAN_TIMESTAMP,
                        Long.toString(t2FirstCommitTimestamp))
                .build();

        assertThat(computeActual(timestampSession, "SELECT * FROM paimon.default.t2").getMaterializedRows().toString())
                .isEqualTo("[[1, 2, 1, 1], [3, 4, 2, 2]]");
    }

    @Test
    public void testSessionTagTimeTravel()
    {
        Session tagSession = Session.builder(getSession())
                .setCatalogSessionProperty(CATALOG, PaimonSessionProperties.SCAN_TAG, "tag-2")
                .build();

        assertThat(computeActual(tagSession, "SELECT * FROM paimon.default.t2").getMaterializedRows().toString())
                .isEqualTo("[[1, 2, 1, 1], [3, 4, 2, 2], [5, 6, 3, 3], [7, 8, 4, 4]]");
    }

    @Test
    public void testExplicitTimeTravelQueryIgnoresConflictingSessionTimeTravelProperties()
    {
        Session conflictingSession = Session.builder(getSession())
                .setCatalogSessionProperty(CATALOG, PaimonSessionProperties.SCAN_SNAPSHOT, "1")
                .setCatalogSessionProperty(CATALOG, PaimonSessionProperties.SCAN_TAG, "tag-2")
                .build();

        assertThat(computeActual(conflictingSession, "SELECT * FROM paimon.default.t2 FOR VERSION AS OF 1")
                .getMaterializedRows().toString())
                .isEqualTo("[[1, 2, 1, 1], [3, 4, 2, 2]]");
        assertThat(computeActual(
                conflictingSession,
                "SELECT * FROM paimon.default.t2 FOR TIMESTAMP AS OF TIMESTAMP " + timestampLiteral(System.currentTimeMillis(), 6))
                .getMaterializedRows().toString())
                .isEqualTo("[[1, 2, 1, 1], [3, 4, 2, 2], [5, 6, 3, 3], [7, 8, 4, 4]]");
    }

    @Test
    public void testSchemaEvolution()
    {
        assertThat(sql("SELECT boolean, tinyint, smallint, int, bigint,float,double,char,varchar, date,timestamp_0, "
                + "timestamp_3, timestamp_6, decimal, to_hex(varbinary), array, map, row FROM paimon.default.t100 "
                + "ORDER BY smallint NULLS FIRST"))
                .isEqualTo(
                        "[[true, 1, null, 1, 1, 1.0, 1.0, char1, varchar1, 1970-01-01, 2023-09-12T07:54:48, 2023-09-12T07:54:48.001, 2023-09-12T07:54:48.001001, 0.10000, 010203, [1, 1, 1], {1=1}, [1, 1]], "
                                + "[true, 1, null, 1, 1, 1.0, 1.0, char1, varchar1, 1970-01-01, 2023-09-12T07:54:48, 2023-09-12T07:54:48.001, 2023-09-12T07:54:48.001001, 0.10000, 010203, [1, 1, 1], {1=1}, [1, 1]], "
                                + "[true, 1, 1, 1, 1, 1.0, 1.0, char1, varchar1, 1970-01-01, 2023-09-12T07:54:48, 2023-09-12T07:54:48.001, 2023-09-12T07:54:48.001001, 0.10000, 010203, [1, 1, 1], {1=1}, [1, 1]]]");
    }

    @Test
    public void testDeletionFile()
    {
        assertThat(sql("SELECT * FROM paimon.default.t101")).isEqualTo(
                "[[a1, 1, 1], [a2, 2, 2], [a3, 3, 3], [a4, 4, 4], [a5, 5, 5], [a6, 6, 6], [a7, 7, 7], [a8, 8, 8], [a9, 9, 9]]");
    }

    @Test
    public void testFileIndex()
    {
        assertThat(sql("SELECT * FROM paimon.default.t102 where c = 2")).isEqualTo("[[a2, 2, 2]]");
    }

    @Test
    public void testFileIndexMapElementPredicateWithProjectedTopLevelMap()
    {
        assertThat(sql(
                "SELECT id FROM paimon.default.t103 "
                        + "WHERE element_at(properties, 'region') = 'ap-south'"))
                .isEqualTo("[[1]]");

        assertThat(sql(
                "SELECT payload FROM paimon.default.t103 "
                        + "WHERE element_at(properties, 'region') = 'eu-west'"))
                .isEqualTo("[[skip-eu-west]]");
    }

    @Test
    public void testInsertIntoFixedBucketTableWiPk()
    {
        sql("INSERT INTO paimon.default.fixed_bucket_table_wi_pk VALUES (1,'1'),(2,'2'),(3,'3'),(4,'4'),(5,'5'),(6,'6')");
        assertThat(sql("SELECT * FROM paimon.default.fixed_bucket_table_wi_pk order by id asc"))
                .isEqualTo("[[1, 1], [2, 2], [3, 3], [4, 4], [5, 5], [6, 6]]");
    }

    @Test
    public void testInsertIntoFixedBucketTableWoPk()
    {
        sql("INSERT INTO paimon.default.fixed_bucket_table_wo_pk VALUES (1,'1'),(2,'2'),(3,'3'),(4,'4'),(1,'1'),(2,'2'),(3,'3'),(4,'4')");
        assertThat(sql("SELECT * FROM paimon.default.fixed_bucket_table_wo_pk order by id asc"))
                .isEqualTo("[[1, 1], [1, 1], [2, 2], [2, 2], [3, 3], [3, 3], [4, 4], [4, 4]]");
    }

    @Test
    public void testInsertIntoUnawareTable()
    {
        sql("INSERT INTO paimon.default.unaware_table VALUES (1,'1'),(2,'2'),(3,'3'),(4,'4'),(1,'1'),(2,'2'),(3,'3'),(4,'4')");
        assertThat(sql("SELECT * FROM paimon.default.unaware_table order by id asc"))
                .isEqualTo("[[1, 1], [1, 1], [2, 2], [2, 2], [3, 3], [3, 3], [4, 4], [4, 4]]");
    }

    protected String sql(String sql)
    {
        MaterializedResult result = getQueryRunner().execute(sql);
        return result.getMaterializedRows().toString();
    }

    private void createBranch(String schemaName, String tableName, String branchName)
            throws Exception
    {
        loadTable(schemaName, tableName).createBranch(branchName);
    }

    private void createBranch(String schemaName, String tableName, String branchName, String fromTag)
            throws Exception
    {
        loadTable(schemaName, tableName).createBranch(branchName, fromTag);
    }

    private void createTag(String schemaName, String tableName, String tagName)
            throws Exception
    {
        loadTable(schemaName, tableName).createTag(tagName);
    }

    private FileStoreTable loadTable(String schemaName, String tableName)
            throws Exception
    {
        return FileStoreTableFactory.create(LocalFileIO.create(), new Path(warehouse, schemaName + ".db/" + tableName));
    }

    private static DataType fieldType(List<DataField> fields, String fieldName)
    {
        return fields.stream()
                .filter(field -> field.name().equals(fieldName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Field not found: " + fieldName))
                .type();
    }

    protected static String timestampLiteral(long epochMilliSeconds, int precision)
    {
        return DateTimeFormatter.ofPattern("''yyyy-MM-dd HH:mm:ss." + "S".repeat(precision) + " VV''")
                .format(Instant.ofEpochMilli(epochMilliSeconds).atZone(UTC));
    }

    private static class SliceParquetDataSource
            extends AbstractParquetDataSource
    {
        private final Slice data;

        private SliceParquetDataSource(Slice data, ParquetReaderOptions options)
        {
            super(new ParquetDataSourceId("slice"), data.length(), options);
            this.data = data;
        }

        @Override
        protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
        {
            data.getBytes((int) position, buffer, bufferOffset, bufferLength);
        }
    }
}
