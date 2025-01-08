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

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
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
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.time.ZoneOffset.UTC;
import static org.apache.paimon.data.BinaryString.fromString;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * ITCase for trino connector.
 */
final class TestPaimonITCase
        extends AbstractTestQueryFramework
{
    private static final String CATALOG = "paimon";
    private static final String DB = "default";

    private long t2FirstCommitTimestamp;

    private static SimpleTableTestHelper createTestHelper(Path tablePath)
            throws Exception
    {
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new DataField(0, "a", new IntType()),
                                new DataField(1, "b", new BigIntType()),
                                // test field name has upper case
                                new DataField(2, "aCa", new VarCharType()),
                                new DataField(3, "d", new CharType(1))));
        return new SimpleTableTestHelper(tablePath, rowType);
    }

    private static String timestampLiteral(long epochMilliSeconds, int precision)
    {
        return DateTimeFormatter.ofPattern(
                        "''yyyy-MM-dd HH:mm:ss." + "S".repeat(precision) + " VV''")
                .format(Instant.ofEpochMilli(epochMilliSeconds).atZone(UTC));
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        String warehouse =
                Files.createTempDirectory(UUID.randomUUID().toString()).toUri().toString();
        // flink sink
        Path tablePath1 = new Path(warehouse, DB + ".db/t1");
        SimpleTableTestHelper testHelper1 = createTestHelper(tablePath1);
        testHelper1.write(GenericRow.of(1, 2L, fromString("1"), fromString("1")));
        testHelper1.write(GenericRow.of(3, 4L, fromString("2"), fromString("2")));
        testHelper1.write(GenericRow.of(5, 6L, fromString("3"), fromString("3")));
        testHelper1.write(
                GenericRow.ofKind(RowKind.DELETE, 3, 4L, fromString("2"), fromString("2")));
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

        {
            Path tablePath3 = new Path(warehouse, "default.db/t3");
            RowType rowType =
                    new RowType(
                            Arrays.asList(
                                    new DataField(0, "pt", DataTypes.STRING()),
                                    new DataField(1, "a", new IntType()),
                                    new DataField(2, "b", new BigIntType()),
                                    new DataField(3, "c", new BigIntType()),
                                    new DataField(4, "d", new IntType())));
            new SchemaManager(LocalFileIO.create(), tablePath3)
                    .createTable(
                            new Schema(
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
            RowType rowType =
                    new RowType(
                            Arrays.asList(
                                    new DataField(1, "a", new IntType()),
                                    new DataField(2, "b", new BigIntType())));
            new SchemaManager(LocalFileIO.create(), tablePath)
                    .createTable(
                            new Schema(
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
            innerRowFields.add(
                    new DataField(5, "innercol2", new VarCharType(VarCharType.MAX_LENGTH)));
            RowType rowType =
                    new RowType(
                            Arrays.asList(
                                    new DataField(0, "i", new IntType()),
                                    new DataField(
                                            1,
                                            "map",
                                            new MapType(
                                                    new VarCharType(VarCharType.MAX_LENGTH),
                                                    new VarCharType(VarCharType.MAX_LENGTH))),
                                    new DataField(2, "innerrow", new RowType(true, innerRowFields)),
                                    new DataField(3, "array", new ArrayType(new IntType()))));
            new SchemaManager(LocalFileIO.create(), tablePath4)
                    .createTable(
                            new Schema(
                                    rowType.getFields(),
                                    Collections.emptyList(),
                                    Collections.singletonList("i"),
                                    Collections.singletonMap("bucket", "1"),
                                    ""));
            FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath4);
            InnerTableWrite writer = table.newWrite("user");
            InnerTableCommit commit = table.newCommit("user");
            writer.write(
                    GenericRow.of(
                            1,
                            new GenericMap(
                                    new HashMap<>()
                                    {
                                        {
                                            put(fromString("1"), fromString("2"));
                                        }
                                    }),
                            GenericRow.of(2, fromString("male")),
                            new GenericArray(new int[] {1, 2, 3})));
            commit.commit(0, writer.prepareCommit(true, 0));
        }

        {
            Path tablePath6 = new Path(warehouse, "default.db/t99");
            RowType rowType =
                    new RowType(
                            Arrays.asList(
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
                                    new DataField(
                                            13,
                                            "timestamp_tz",
                                            DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)),
                                    new DataField(14, "decimal", DataTypes.DECIMAL(10, 5)),
                                    new DataField(15, "varbinary", DataTypes.VARBINARY(10)),
                                    new DataField(16, "array", DataTypes.ARRAY(DataTypes.INT())),
                                    new DataField(
                                            17,
                                            "map",
                                            DataTypes.MAP(DataTypes.INT(), DataTypes.INT())),
                                    new DataField(
                                            18,
                                            "row",
                                            DataTypes.ROW(
                                                    DataTypes.FIELD(100, "q1", DataTypes.INT()),
                                                    DataTypes.FIELD(101, "q2", DataTypes.INT())))));
            new SchemaManager(LocalFileIO.create(), tablePath6)
                    .createTable(
                            new Schema(
                                    rowType.getFields(),
                                    List.of(
                                            "boolean",
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
                                    List.of(
                                            "boolean",
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
            writer.write(
                    GenericRow.of(
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
            RowType rowType =
                    new RowType(
                            Arrays.asList(
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
                                    new DataField(
                                            16,
                                            "map",
                                            DataTypes.MAP(DataTypes.INT(), DataTypes.INT())),
                                    new DataField(
                                            17,
                                            "row",
                                            DataTypes.ROW(
                                                    DataTypes.FIELD(100, "q1", DataTypes.INT()),
                                                    DataTypes.FIELD(101, "q2", DataTypes.INT())))));
            new SchemaManager(LocalFileIO.create(), tablePath7)
                    .createTable(
                            new Schema(
                                    rowType.getFields(),
                                    Collections.emptyList(),
                                    Collections.emptyList(),
                                    Collections.singletonMap("bucket", "-1"),
                                    ""));
            FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath7);
            InnerTableWrite writer = table.newWrite("user");
            InnerTableCommit commit = table.newCommit("user");
            writer.write(
                    GenericRow.of(
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

            new SchemaManager(LocalFileIO.create(), tablePath7)
                    .commitChanges(SchemaChange.dropColumn("smallint"));
            table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath7);
            writer = table.newWrite("user");
            commit = table.newCommit("user");
            writer.write(
                    GenericRow.of(
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
            writer.write(
                    GenericRow.of(
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

            new SchemaManager(LocalFileIO.create(), tablePath7)
                    .commitChanges(SchemaChange.updateColumnType("smallint", DataTypes.STRING()));
            table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath7);
            writer = table.newWrite("user");
            commit = table.newCommit("user");
            writer.write(
                    GenericRow.of(
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
                            BinaryString.fromString("10086")));
            commit.commit(1, writer.prepareCommit(true, 1));
        }

        {
            Path tablePath6 = new Path(warehouse, "default.db/t101");
            RowType rowType =
                    new RowType(
                            Arrays.asList(
                                    new DataField(0, "a", DataTypes.STRING()),
                                    new DataField(1, "b", DataTypes.INT()),
                                    new DataField(2, "c", DataTypes.INT())));
            new SchemaManager(LocalFileIO.create(), tablePath6)
                    .createTable(
                            new Schema(
                                    rowType.getFields(),
                                    Collections.emptyList(),
                                    List.of("a"),
                                    new HashMap<>()
                                    {
                                        {
                                            put(CoreOptions.BUCKET.key(), "1");
                                            put(CoreOptions.DELETION_VECTORS_ENABLED.key(), "true");
                                        }
                                    },
                                    ""));
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
            RowType rowType =
                    new RowType(
                            Arrays.asList(
                                    new DataField(0, "a", DataTypes.STRING()),
                                    new DataField(1, "b", DataTypes.INT()),
                                    new DataField(2, "c", DataTypes.INT())));
            new SchemaManager(LocalFileIO.create(), tablePath)
                    .createTable(
                            new Schema(
                                    rowType.getFields(),
                                    Collections.emptyList(),
                                    Collections.emptyList(),
                                    new HashMap<>()
                                    {
                                        {
                                            put("file-index.bloom-filter.columns", "a,b,c");
                                        }
                                    },
                                    ""));
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
            Path tablePath = new Path(warehouse, "default.db/fixed_bucket_table_wi_pk");
            RowType rowType =
                    new RowType(
                            Arrays.asList(
                                    new DataField(0, "id", DataTypes.INT()),
                                    new DataField(1, "name", DataTypes.STRING())));
            new SchemaManager(LocalFileIO.create(), tablePath)
                    .createTable(
                            new Schema(
                                    rowType.getFields(),
                                    Collections.emptyList(),
                                    Collections.emptyList(),
                                    new HashMap<>()
                                    {
                                        {
                                            put("file.format", "orc");
                                            put("primary-key", "id");
                                            put("bucket", "2");
                                        }
                                    },
                                    ""));
        }

        {
            Path tablePath = new Path(warehouse, "default.db/fixed_bucket_table_wo_pk");
            RowType rowType =
                    new RowType(
                            Arrays.asList(
                                    new DataField(0, "id", DataTypes.INT()),
                                    new DataField(1, "name", DataTypes.STRING())));
            new SchemaManager(LocalFileIO.create(), tablePath)
                    .createTable(
                            new Schema(
                                    rowType.getFields(),
                                    Collections.emptyList(),
                                    Collections.emptyList(),
                                    new HashMap<>()
                                    {
                                        {
                                            put("file.format", "orc");
                                            put("bucket", "2");
                                            put("bucket-key", "id");
                                        }
                                    },
                                    ""));
        }

        {
            Path tablePath = new Path(warehouse, "default.db/unaware_table");
            RowType rowType =
                    new RowType(
                            Arrays.asList(
                                    new DataField(0, "id", DataTypes.INT()),
                                    new DataField(1, "name", DataTypes.STRING())));
            new SchemaManager(LocalFileIO.create(), tablePath)
                    .createTable(
                            new Schema(
                                    rowType.getFields(),
                                    Collections.emptyList(),
                                    Collections.emptyList(),
                                    new HashMap<>()
                                    {
                                        {
                                            put("file.format", "orc");
                                        }
                                    },
                                    ""));
        }

        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner =
                    DistributedQueryRunner.builder(
                                    testSessionBuilder().setCatalog(CATALOG).setSchema(DB).build())
                            .build();
            queryRunner.installPlugin(new PaimonPlugin());
            Map<String, String> options = new HashMap<>();
            options.put("warehouse", warehouse);
            options.put("fs.hadoop.enabled", "true");
            queryRunner.createCatalog(CATALOG, CATALOG, options);
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    @Test
    void testComplexTypes()
    {
        assertThat(sql("SELECT * FROM paimon.default.t4"))
                .isEqualTo("[[1, {1=2}, [2, male], [1, 2, 3]]]");
    }

    @Test
    void testEmptyTable()
    {
        assertThat(sql("SELECT * FROM paimon.default.empty_t")).isEqualTo("[]");
    }

    @Test
    void testProjection()
    {
        assertThat(sql("SELECT * FROM paimon.default.t1"))
                .isEqualTo("[[1, 2, 1, 1], [5, 6, 3, 3]]");
        assertThat(sql("SELECT a, aCa FROM paimon.default.t1")).isEqualTo("[[1, 1], [5, 3]]");
        assertThat(sql("SELECT SUM(b) FROM paimon.default.t1")).isEqualTo("[[8]]");
    }

    @Test
    void testLimit()
    {
        assertThat(sql("SELECT * FROM paimon.default.t1 LIMIT 1")).isEqualTo("[[1, 2, 1, 1]]");
        assertThat(sql("SELECT * FROM paimon.default.t1 WHERE a = 5 LIMIT 1"))
                .isEqualTo("[[5, 6, 3, 3]]");
    }

    @Test
    void testSystemTable()
    {
        assertThat(
                sql(
                        "SELECT snapshot_id,schema_id,commit_user,commit_identifier,commit_kind FROM \"t1$snapshots\""))
                .isEqualTo("[[1, 0, user, 0, APPEND]]");
    }

    @Test
    void testFilter()
    {
        assertThat(sql("SELECT a, aCa FROM paimon.default.t2 WHERE a < 4"))
                .isEqualTo("[[1, 1], [3, 2]]");
    }

    @Test
    void testGroupByWithCast()
    {
        assertThat(
                sql(
                        "SELECT pt, a, SUM(b), SUM(d) FROM paimon.default.t3 GROUP BY pt, a ORDER BY pt, a"))
                .isEqualTo("[[1, 1, 3, 3], [2, 3, 3, 3]]");
    }

    @Test
    void testLimitWithPartition()
    {
        assertThat(sql("SELECT * FROM paimon.default.t3 WHERE pt = '1' LIMIT 1"))
                .isEqualTo("[[1, 1, 1, 1, 1]]");

        assertThat(sql("SELECT * FROM paimon.default.t3 WHERE pt = '1' AND b = 2 LIMIT 1"))
                .isEqualTo("[[1, 1, 2, 2, 2]]");
    }

    @Test
    void testShowCreateTable()
    {
        assertThat(sql("SHOW CREATE TABLE paimon.default.t3"))
                .isEqualTo(
                        "[[CREATE TABLE paimon.default.t3 (\n"
                                + "   pt varchar,\n"
                                + "   a integer,\n"
                                + "   b bigint,\n"
                                + "   c bigint,\n"
                                + "   d integer\n"
                                + ")]]");
    }

    @Test
    void testCreateSchema()
    {
        sql("CREATE SCHEMA paimon.test");
        assertThat(sql("SHOW SCHEMAS FROM paimon"))
                .isEqualTo("[[default], [information_schema], [test]]");
        sql("DROP SCHEMA paimon.test");
    }

    @Test
    void testDropSchema()
    {
        sql("CREATE SCHEMA paimon.tpch");
        sql("DROP SCHEMA paimon.tpch");
        assertThat(sql("SHOW SCHEMAS FROM paimon")).isEqualTo("[[default], [information_schema]]");
    }

    @Test
    void testCreateTable()
    {
        sql(
                "CREATE TABLE orders ("
                        + "  order_key bigint,"
                        + "  order_status varchar,"
                        + "  total_price double,"
                        + "  order_date date"
                        + ")"
                        + "WITH ("
                        + "file_format = 'ORC',"
                        + "primary_key = ARRAY['order_key','order_date'],"
                        + "partitioned_by = ARRAY['order_date'],"
                        + "bucket = '2',"
                        + "bucket_key = 'order_key',"
                        + "changelog_producer = 'input'"
                        + ")");
        assertThat(sql("SHOW TABLES FROM paimon.default")).contains("orders");
        sql("DROP TABLE IF EXISTS paimon.default.orders");
    }

    @Test
    void testRenameTable()
    {
        sql(
                "CREATE TABLE testRenameTable ("
                        + "  order_key bigint,"
                        + "  order_status varchar,"
                        + "  total_price double,"
                        + "  order_date date"
                        + ")"
                        + "WITH ("
                        + "file_format = 'ORC',"
                        + "primary_key = ARRAY['order_key','order_date'],"
                        + "partitioned_by = ARRAY['order_date'],"
                        + "bucket = '2',"
                        + "bucket_key = 'order_key',"
                        + "changelog_producer = 'input'"
                        + ")");
        sql("ALTER TABLE paimon.default.testRenameTable RENAME TO test_Rename_Table");
        String result = sql("SHOW TABLES FROM paimon.default");
        assertThat(result)
                .doesNotContainIgnoringCase("testRenameTable")
                .containsIgnoringCase("test_Rename_Table");
        sql("DROP TABLE IF EXISTS paimon.default.test_Rename_Table");
    }

    @Test
    void testDropTable()
    {
        sql(
                "CREATE TABLE testDropTable ("
                        + "  order_key bigint,"
                        + "  order_status varchar,"
                        + "  total_price double,"
                        + "  order_date date"
                        + ")"
                        + "WITH ("
                        + "file_format = 'ORC',"
                        + "primary_key = ARRAY['order_key','order_date'],"
                        + "partitioned_by = ARRAY['order_date'],"
                        + "bucket = '2',"
                        + "bucket_key = 'order_key',"
                        + "changelog_producer = 'input'"
                        + ")");
        sql("DROP TABLE IF EXISTS paimon.default.testDropTable");
        assertThat(sql("SHOW TABLES FROM paimon.default"))
                .doesNotContainIgnoringCase("testDropTable");
    }

    @Test
    void testAddColumn()
    {
        sql(
                "CREATE TABLE testAddColumn ("
                        + "  order_key bigint,"
                        + "  order_status varchar,"
                        + "  total_price double,"
                        + "  order_date date"
                        + ")"
                        + "WITH ("
                        + "file_format = 'ORC',"
                        + "primary_key = ARRAY['order_key','order_date'],"
                        + "partitioned_by = ARRAY['order_date'],"
                        + "bucket = '2',"
                        + "bucket_key = 'order_key',"
                        + "changelog_producer = 'input'"
                        + ")");
        sql("ALTER TABLE paimon.default.testAddColumn ADD COLUMN zip varchar");
        assertThat(sql("SHOW COLUMNS FROM paimon.default.testAddColumn"))
                .isEqualTo(
                        "[[order_key, bigint, , ], [order_status, varchar(2147483646), , ], [total_price, double, , ], [order_date, date, , ], [zip, varchar(2147483646), , ]]");
        sql("DROP TABLE IF EXISTS paimon.default.testAddColumn");
    }

    @Test
    void testRenameColumn()
    {
        sql(
                "CREATE TABLE testRenameColumn ("
                        + "  order_key bigint,"
                        + "  order_status varchar,"
                        + "  total_price double,"
                        + "  order_date date"
                        + ")"
                        + "WITH ("
                        + "file_format = 'ORC',"
                        + "primary_key = ARRAY['order_key','order_date'],"
                        + "partitioned_by = ARRAY['order_date'],"
                        + "bucket = '2',"
                        + "bucket_key = 'order_key',"
                        + "changelog_producer = 'input'"
                        + ")");
        sql("ALTER TABLE paimon.default.testRenameColumn RENAME COLUMN order_status to g");
        assertThat(sql("SHOW COLUMNS FROM paimon.default.testRenameColumn"))
                .isEqualTo(
                        "[[order_key, bigint, , ], [g, varchar(2147483646), , ], [total_price, double, , ], [order_date, date, , ]]");
        sql("DROP TABLE IF EXISTS paimon.default.testRenameColumn");
    }

    @Test
    void testDropColumn()
    {
        sql(
                "CREATE TABLE testDropColumn ("
                        + "  order_key bigint,"
                        + "  order_status varchar,"
                        + "  total_price double,"
                        + "  order_date date"
                        + ")"
                        + "WITH ("
                        + "file_format = 'ORC',"
                        + "primary_key = ARRAY['order_key','order_date'],"
                        + "partitioned_by = ARRAY['order_date'],"
                        + "bucket = '2',"
                        + "bucket_key = 'order_key',"
                        + "changelog_producer = 'input'"
                        + ")");
        sql("ALTER TABLE paimon.default.testDropColumn DROP COLUMN order_status");
        assertThat(sql("SHOW COLUMNS FROM paimon.default.testDropColumn"))
                .isEqualTo(
                        "[[order_key, bigint, , ], [total_price, double, , ], [order_date, date, , ]]");
        sql("DROP TABLE IF EXISTS paimon.default.testDropColumn");
    }

    @Test
    void testSetTableProperties()
    {
        sql(
                "CREATE TABLE testSetTableProperties ("
                        + "  order_key bigint,"
                        + "  order_status varchar,"
                        + "  total_price double,"
                        + "  order_date date"
                        + ")"
                        + "WITH ("
                        + "file_format = 'ORC',"
                        + "primary_key = ARRAY['order_key','order_date'],"
                        + "partitioned_by = ARRAY['order_date'],"
                        + "bucket = '2',"
                        + "bucket_key = 'order_key',"
                        + "changelog_producer = 'input'"
                        + ")");
        sql(
                "ALTER TABLE paimon.default.testSetTableProperties SET PROPERTIES bucket = '4',snapshot_time_retained = '4h'");
        sql("DROP TABLE IF EXISTS paimon.default.testSetTableProperties");
    }

    @Test
    void testAllType()
    {
        assertThat(
                sql(
                        "SELECT boolean, tinyint, smallint,int,bigint,float,double,char,varchar, date,timestamp_0, "
                                + "timestamp_3, timestamp_6, decimal, to_hex(varbinary), array, map, row FROM paimon.default.t99"))
                .isEqualTo(
                        "[[true, 1, 1, 1, 1, 1.0, 1.0, char1, varchar1, 1970-01-01, "
                                + "2023-09-12T07:54:48, 2023-09-12T07:54:48.001, 2023-09-12T07:54:48.001001, "
                                + "0.10000, 010203, [1, 1, 1], {1=1}, [1, 1]]]");
    }

    @Test
    void testTimeTravel()
    {
        assertThat(sql("SELECT * FROM paimon.default.t2 FOR VERSION AS OF 1"))
                .isEqualTo("[[1, 2, 1, 1], [3, 4, 2, 2]]");
        assertThat(sql("SELECT * FROM paimon.default.t2 FOR VERSION AS OF 2"))
                .isEqualTo("[[1, 2, 1, 1], [3, 4, 2, 2], [5, 6, 3, 3], [7, 8, 4, 4]]");

        assertThat(
                sql(
                        "SELECT * FROM paimon.default.t2 FOR TIMESTAMP AS OF TIMESTAMP "
                                + timestampLiteral(t2FirstCommitTimestamp, 6)))
                .isEqualTo("[[1, 2, 1, 1], [3, 4, 2, 2]]");
        assertThat(
                sql(
                        "SELECT * FROM paimon.default.t2 FOR TIMESTAMP AS OF TIMESTAMP "
                                + timestampLiteral(System.currentTimeMillis(), 6)))
                .isEqualTo("[[1, 2, 1, 1], [3, 4, 2, 2], [5, 6, 3, 3], [7, 8, 4, 4]]");
    }

    @Test
    void testTimeTravelWithTag()
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
    void testSchemaEvolution()
    {
        assertThat(
                sql(
                        "SELECT boolean, tinyint, smallint, int, bigint,float,double,char,varchar, date,timestamp_0, "
                                + "timestamp_3, timestamp_6, decimal, to_hex(varbinary), array, map, row FROM paimon.default.t100"))
                .isEqualTo(
                        "[[true, 1, null, 1, 1, 1.0, 1.0, char1, varchar1, 1970-01-01, 2023-09-12T07:54:48, 2023-09-12T07:54:48.001, 2023-09-12T07:54:48.001001, 0.10000, 010203, [1, 1, 1], {1=1}, [1, 1]], "
                                + "[true, 1, null, 1, 1, 1.0, 1.0, char1, varchar1, 1970-01-01, 2023-09-12T07:54:48, 2023-09-12T07:54:48.001, 2023-09-12T07:54:48.001001, 0.10000, 010203, [1, 1, 1], {1=1}, [1, 1]], "
                                + "[true, 1, 1, 1, 1, 1.0, 1.0, char1, varchar1, 1970-01-01, 2023-09-12T07:54:48, 2023-09-12T07:54:48.001, 2023-09-12T07:54:48.001001, 0.10000, 010203, [1, 1, 1], {1=1}, [1, 1]], "
                                + "[true, 1, 10086, 1, 1, 1.0, 1.0, char1, varchar1, 1970-01-01, 2023-09-12T07:54:48, 2023-09-12T07:54:48.001, 2023-09-12T07:54:48.001001, 0.10000, 010203, [1, 1, 1], {1=1}, [1, 1]]]");
    }

    @Test
    void testDeletionFile()
    {
        assertThat(sql("SELECT * FROM paimon.default.t101 WHERE b > 0"))
                .isEqualTo(
                        "[[a1, 1, 1], [a2, 2, 2], [a3, 3, 3], [a4, 4, 4], [a5, 5, 5], [a6, 6, 6], [a7, 7, 7], [a8, 8, 8], [a9, 9, 9]]");
    }

    @Test
    void testFileIndex()
    {
        assertThat(sql("SELECT * FROM paimon.default.t102 where c = 2")).isEqualTo("[[a2, 2, 2]]");
    }

    @Test
    void testInsertIntoFixedBucketTableWiPk()
    {
        sql(
                "INSERT INTO paimon.default.fixed_bucket_table_wi_pk VALUES (1,'1'),(2,'2'),(3,'3'),(4,'4'),(5,'5'),(6,'6')");
        assertThat(sql("SELECT * FROM paimon.default.fixed_bucket_table_wi_pk order by id asc"))
                .isEqualTo("[[1, 1], [2, 2], [3, 3], [4, 4], [5, 5], [6, 6]]");
    }

    @Test
    void testInsertIntoFixedBucketTableWoPk()
    {
        sql(
                "INSERT INTO paimon.default.fixed_bucket_table_wo_pk VALUES (1,'1'),(2,'2'),(3,'3'),(4,'4'),(1,'1'),(2,'2'),(3,'3'),(4,'4')");
        assertThat(sql("SELECT * FROM paimon.default.fixed_bucket_table_wo_pk order by id asc"))
                .isEqualTo("[[1, 1], [1, 1], [2, 2], [2, 2], [3, 3], [3, 3], [4, 4], [4, 4]]");
    }

    @Test
    void testInsertIntoUnawareTable()
    {
        sql(
                "INSERT INTO paimon.default.unaware_table VALUES (1,'1'),(2,'2'),(3,'3'),(4,'4'),(1,'1'),(2,'2'),(3,'3'),(4,'4')");
        assertThat(sql("SELECT * FROM paimon.default.unaware_table order by id asc"))
                .isEqualTo("[[1, 1], [1, 1], [2, 2], [2, 2], [3, 3], [3, 3], [4, 4], [4, 4]]");
    }

    private String sql(String sql)
    {
        MaterializedResult result = getQueryRunner().execute(sql);
        return result.getMaterializedRows().toString();
    }
}
