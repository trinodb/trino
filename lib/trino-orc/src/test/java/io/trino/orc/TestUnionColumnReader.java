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
package io.trino.orc;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlRow;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.RowType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector.StandardUnion;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.Properties;

import static io.trino.orc.OrcTester.HIVE_STORAGE_TIME_ZONE;
import static io.trino.orc.OrcTester.READER_OPTIONS;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for the bug introduced in commit b946791b573 ("Change RowBlock to be not null-suppressed"):
 * UnionColumnReader.getBlocks() had an inverted loop condition and wrong tag indexing, causing all
 * union variant fields (field0, field1, field2) to be null when the PRESENT stream is absent
 * (i.e., all union rows are non-null — the common case).
 *
 * The Hive union type {@code uniontype<varchar, bigint, double>} is represented in Trino as
 * {@code row(tag tinyint, field0 varchar, field1 bigint, field2 double)}.
 */
public class TestUnionColumnReader
{
    private static final int ROW_COUNT = 100;

    /**
     * Tests that reading a Hive ORC UNION column (all rows using variant 0 = varchar, no null rows)
     * correctly returns non-null field0 values. This exercises the no-PRESENT-stream path in
     * UnionColumnReader.getBlocks() where rowIsNull=null.
     */
    @Test
    public void testReadUnionAllNonNullVarcharVariant()
            throws Exception
    {
        // uniontype<varchar, bigint, double> maps to row(tag tinyint, field0 varchar, field1 bigint, field2 double)
        RowType unionRowType = RowType.from(ImmutableList.of(
                RowType.field("tag", TINYINT),
                RowType.field("field0", VARCHAR),
                RowType.field("field1", BIGINT),
                RowType.field("field2", DOUBLE)));

        try (TempFile tempFile = new TempFile()) {
            // Write an ORC file with UNION type using Hive's ORC writer.
            // All rows use tag=0 (varchar variant) with value "hello-N".
            writeUnionOrcFile(tempFile.getFile(), ROW_COUNT);

            OrcDataSource orcDataSource = new FileOrcDataSource(tempFile.getFile(), READER_OPTIONS);
            OrcReader orcReader = OrcReader.createOrcReader(orcDataSource, READER_OPTIONS)
                    .orElseThrow(() -> new RuntimeException("File is empty"));

            try (OrcRecordReader reader = orcReader.createRecordReader(
                    orcReader.getRootColumn().getNestedColumns(),
                    ImmutableList.of(unionRowType),
                    false,
                    OrcPredicate.TRUE,
                    HIVE_STORAGE_TIME_ZONE,
                    AggregatedMemoryContext.newSimpleAggregatedMemoryContext(),
                    OrcReader.INITIAL_BATCH_SIZE,
                    RuntimeException::new)) {
                int rowsRead = 0;
                SourcePage page;
                while ((page = reader.nextPage()) != null) {
                    Block unionBlock = page.getBlock(0);

                    for (int position = 0; position < unionBlock.getPositionCount(); position++) {
                        // unionBlock is a RowBlock with 4 fields:
                        //   [0] tag (tinyint discriminant)
                        //   [1] field0 (varchar variant)
                        //   [2] field1 (bigint variant)
                        //   [3] field2 (double variant)
                        SqlRow sqlRow = unionRowType.getObject(unionBlock, position);
                        Block tagBlock = sqlRow.getRawFieldBlock(0);
                        Block field0Block = sqlRow.getRawFieldBlock(1);
                        int rawIndex = sqlRow.getRawIndex();

                        // tag should be 0 (varchar variant)
                        assertThat(tagBlock.isNull(rawIndex))
                                .as("tag should not be null at position %d", position)
                                .isFalse();
                        assertThat(TINYINT.getByte(tagBlock, rawIndex))
                                .as("tag should be 0 (varchar variant) at position %d", position)
                                .isEqualTo((byte) 0);

                        // field0 (varchar variant) must NOT be null — this is what the bug broke
                        assertThat(field0Block.isNull(rawIndex))
                                .as("field0 must not be null at position %d (tag=0 means varchar variant is active)", position)
                                .isFalse();

                        Slice field0Value = VARCHAR.getSlice(field0Block, rawIndex);
                        assertThat(field0Value.toStringUtf8())
                                .as("field0 value at position %d", position)
                                .isEqualTo("hello-" + rowsRead);

                        rowsRead++;
                    }
                }

                assertThat(rowsRead).isEqualTo(ROW_COUNT);
            }
        }
    }

    /**
     * Tests that reading with some null union rows (PRESENT stream present) also works correctly.
     */
    @Test
    public void testReadUnionWithNullRows()
            throws Exception
    {
        RowType unionRowType = RowType.from(ImmutableList.of(
                RowType.field("tag", TINYINT),
                RowType.field("field0", VARCHAR),
                RowType.field("field1", BIGINT),
                RowType.field("field2", DOUBLE)));

        try (TempFile tempFile = new TempFile()) {
            // Write ORC file where every 3rd row is a null union value (exercises the PRESENT stream path)
            writeUnionOrcFileWithNulls(tempFile.getFile(), ROW_COUNT);

            OrcDataSource orcDataSource = new FileOrcDataSource(tempFile.getFile(), READER_OPTIONS);
            OrcReader orcReader = OrcReader.createOrcReader(orcDataSource, READER_OPTIONS)
                    .orElseThrow(() -> new RuntimeException("File is empty"));

            try (OrcRecordReader reader = orcReader.createRecordReader(
                    orcReader.getRootColumn().getNestedColumns(),
                    ImmutableList.of(unionRowType),
                    false,
                    OrcPredicate.TRUE,
                    HIVE_STORAGE_TIME_ZONE,
                    AggregatedMemoryContext.newSimpleAggregatedMemoryContext(),
                    OrcReader.INITIAL_BATCH_SIZE,
                    RuntimeException::new)) {
                int rowsRead = 0;
                int nonNullRows = 0;
                SourcePage page;
                while ((page = reader.nextPage()) != null) {
                    Block unionBlock = page.getBlock(0);

                    for (int position = 0; position < unionBlock.getPositionCount(); position++) {
                        boolean rowIsNull = unionBlock.isNull(position);
                        if (!rowIsNull) {
                            SqlRow sqlRow = unionRowType.getObject(unionBlock, position);
                            Block field0Block = sqlRow.getRawFieldBlock(1);
                            int rawIndex = sqlRow.getRawIndex();

                            assertThat(field0Block.isNull(rawIndex))
                                    .as("field0 must not be null for non-null union row at position %d", position)
                                    .isFalse();
                            nonNullRows++;
                        }
                        rowsRead++;
                    }
                }

                assertThat(rowsRead).isEqualTo(ROW_COUNT);
                assertThat(nonNullRows).isGreaterThan(0);
            }
        }
    }

    /**
     * Tests that reading an ORC file where ALL union rows are null (entire batch is null)
     * does not crash with "Invalid position 0 in block with 0 positions".
     * This exercises the all-nulls branch in UnionColumnReader.readBlock() where
     * nullValues == nextBatchSize and the PRESENT stream is fully unset.
     */
    @Test
    public void testReadUnionAllNullRows()
            throws Exception
    {
        RowType unionRowType = RowType.from(ImmutableList.of(
                RowType.field("tag", TINYINT),
                RowType.field("field0", VARCHAR),
                RowType.field("field1", BIGINT),
                RowType.field("field2", DOUBLE)));

        try (TempFile tempFile = new TempFile()) {
            writeUnionOrcFileAllNulls(tempFile.getFile(), ROW_COUNT);

            OrcDataSource orcDataSource = new FileOrcDataSource(tempFile.getFile(), READER_OPTIONS);
            OrcReader orcReader = OrcReader.createOrcReader(orcDataSource, READER_OPTIONS)
                    .orElseThrow(() -> new RuntimeException("File is empty"));

            try (OrcRecordReader reader = orcReader.createRecordReader(
                    orcReader.getRootColumn().getNestedColumns(),
                    ImmutableList.of(unionRowType),
                    false,
                    OrcPredicate.TRUE,
                    HIVE_STORAGE_TIME_ZONE,
                    AggregatedMemoryContext.newSimpleAggregatedMemoryContext(),
                    OrcReader.INITIAL_BATCH_SIZE,
                    RuntimeException::new)) {
                int rowsRead = 0;
                SourcePage page;
                while ((page = reader.nextPage()) != null) {
                    Block unionBlock = page.getBlock(0);

                    for (int position = 0; position < unionBlock.getPositionCount(); position++) {
                        assertThat(unionBlock.isNull(position))
                                .as("row at position %d should be null", position)
                                .isTrue();
                        rowsRead++;
                    }
                }

                assertThat(rowsRead).isEqualTo(ROW_COUNT);
            }
        }
    }

    /**
     * Writes an ORC file with schema {@code struct<test:uniontype<string,bigint,double>>}.
     * All rows use union tag=0 (varchar variant) with value "hello-N". No null rows.
     */
    private static void writeUnionOrcFile(File outputFile, int rowCount)
            throws Exception
    {
        SettableStructObjectInspector structOI = (SettableStructObjectInspector) ObjectInspectorFactory.getStandardStructObjectInspector(
                ImmutableList.of("test"),
                ImmutableList.of(ObjectInspectorFactory.getStandardUnionObjectInspector(ImmutableList.of(
                        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                        PrimitiveObjectInspectorFactory.javaLongObjectInspector,
                        PrimitiveObjectInspectorFactory.javaDoubleObjectInspector))));

        FileSinkOperator.RecordWriter recordWriter = createHiveRecordWriter(outputFile, structOI);
        Serializer serializer = new OrcSerde();

        List<? extends StructField> fields = structOI.getAllStructFieldRefs();
        Object row = structOI.create();

        for (int i = 0; i < rowCount; i++) {
            structOI.setStructFieldData(row, fields.get(0), new StandardUnion((byte) 0, "hello-" + i));
            Writable record = ((OrcSerde) serializer).serialize(row, structOI);
            recordWriter.write(record);
        }

        recordWriter.close(false);
    }

    /**
     * Writes an ORC file where every 3rd row is a null union value (exercises the PRESENT stream).
     * Non-null rows use tag=0 (varchar variant).
     */
    private static void writeUnionOrcFileWithNulls(File outputFile, int rowCount)
            throws Exception
    {
        SettableStructObjectInspector structOI = (SettableStructObjectInspector) ObjectInspectorFactory.getStandardStructObjectInspector(
                ImmutableList.of("test"),
                ImmutableList.of(ObjectInspectorFactory.getStandardUnionObjectInspector(ImmutableList.of(
                        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                        PrimitiveObjectInspectorFactory.javaLongObjectInspector,
                        PrimitiveObjectInspectorFactory.javaDoubleObjectInspector))));

        FileSinkOperator.RecordWriter recordWriter = createHiveRecordWriter(outputFile, structOI);
        Serializer serializer = new OrcSerde();

        List<? extends StructField> fields = structOI.getAllStructFieldRefs();
        Object row = structOI.create();
        int nonNullIndex = 0;

        for (int i = 0; i < rowCount; i++) {
            if (i % 3 == 2) {
                structOI.setStructFieldData(row, fields.get(0), null);
            }
            else {
                structOI.setStructFieldData(row, fields.get(0), new StandardUnion((byte) 0, "hello-" + nonNullIndex));
                nonNullIndex++;
            }
            Writable record = ((OrcSerde) serializer).serialize(row, structOI);
            recordWriter.write(record);
        }

        recordWriter.close(false);
    }

    /**
     * Writes an ORC file where ALL rows have a null union value.
     * This exercises the all-nulls branch in UnionColumnReader.readBlock() where nullValues == nextBatchSize.
     */
    private static void writeUnionOrcFileAllNulls(File outputFile, int rowCount)
            throws Exception
    {
        SettableStructObjectInspector structOI = (SettableStructObjectInspector) ObjectInspectorFactory.getStandardStructObjectInspector(
                ImmutableList.of("test"),
                ImmutableList.of(ObjectInspectorFactory.getStandardUnionObjectInspector(ImmutableList.of(
                        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                        PrimitiveObjectInspectorFactory.javaLongObjectInspector,
                        PrimitiveObjectInspectorFactory.javaDoubleObjectInspector))));

        FileSinkOperator.RecordWriter recordWriter = createHiveRecordWriter(outputFile, structOI);
        Serializer serializer = new OrcSerde();

        List<? extends StructField> fields = structOI.getAllStructFieldRefs();
        Object row = structOI.create();

        for (int i = 0; i < rowCount; i++) {
            structOI.setStructFieldData(row, fields.get(0), null);
            Writable record = ((OrcSerde) serializer).serialize(row, structOI);
            recordWriter.write(record);
        }

        recordWriter.close(false);
    }

    private static FileSinkOperator.RecordWriter createHiveRecordWriter(File outputFile, SettableStructObjectInspector structOI)
            throws Exception
    {
        Properties tableProperties = new Properties();
        List<? extends StructField> fields = structOI.getAllStructFieldRefs();
        tableProperties.setProperty("columns", fields.stream()
                .map(StructField::getFieldName)
                .reduce((a, b) -> a + "," + b).orElse(""));
        tableProperties.setProperty("columns.types", fields.stream()
                .map(f -> f.getFieldObjectInspector().getTypeName())
                .reduce((a, b) -> a + ":" + b).orElse(""));

        JobConf jobConf = new JobConf(new Configuration());
        Progressable progressable = () -> {};

        return new OrcOutputFormat().getHiveRecordWriter(
                jobConf,
                new Path(outputFile.toURI()),
                Text.class,
                false,
                tableProperties,
                progressable);
    }
}
