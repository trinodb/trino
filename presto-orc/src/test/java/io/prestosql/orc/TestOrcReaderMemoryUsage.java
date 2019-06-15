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
package io.prestosql.orc;

import com.google.common.base.Strings;
import io.prestosql.metadata.Metadata;
import io.prestosql.orc.metadata.CompressionKind;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.Writable;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;

import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.orc.OrcReader.INITIAL_BATCH_SIZE;
import static io.prestosql.orc.OrcReader.MAX_BATCH_SIZE;
import static io.prestosql.orc.OrcTester.Format.ORC_12;
import static io.prestosql.orc.OrcTester.createCustomOrcRecordReader;
import static io.prestosql.orc.OrcTester.createOrcRecordWriter;
import static io.prestosql.orc.OrcTester.createSettableStructObjectInspector;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

public class TestOrcReaderMemoryUsage
{
    private static final Metadata METADATA = createTestMetadataManager();

    @Test
    public void testVarcharTypeWithoutNulls()
            throws Exception
    {
        int rows = 5000;
        OrcRecordReader reader = null;
        try (TempFile tempFile = createSingleColumnVarcharFile(rows, 10)) {
            reader = createCustomOrcRecordReader(tempFile, OrcPredicate.TRUE, VARCHAR, INITIAL_BATCH_SIZE);
            assertInitialRetainedSizes(reader, rows);

            long stripeReaderRetainedSize = reader.getCurrentStripeRetainedSizeInBytes();
            long streamReaderRetainedSize = reader.getStreamReaderRetainedSizeInBytes();
            long readerRetainedSize = reader.getRetainedSizeInBytes();
            long readerSystemMemoryUsage = reader.getSystemMemoryUsage();

            while (true) {
                int batchSize = reader.nextBatch();
                if (batchSize == -1) {
                    break;
                }

                Block block = reader.readBlock(0);
                assertEquals(block.getPositionCount(), batchSize);

                // We only verify the memory usage when the batchSize reaches MAX_BATCH_SIZE as batchSize may be
                // increasing during the test, which will cause the StreamReader buffer sizes to increase too.
                if (batchSize < MAX_BATCH_SIZE) {
                    continue;
                }

                // StripeReader memory should increase after reading a block.
                assertGreaterThan(reader.getCurrentStripeRetainedSizeInBytes(), stripeReaderRetainedSize);
                // There are no local buffers needed.
                assertEquals(reader.getStreamReaderRetainedSizeInBytes() - streamReaderRetainedSize, 0L);
                // The total retained size and system memory usage should be greater than 0 byte because of the instance sizes.
                assertGreaterThan(reader.getRetainedSizeInBytes() - readerRetainedSize, 0L);
                assertGreaterThan(reader.getSystemMemoryUsage() - readerSystemMemoryUsage, 0L);
            }
        }
        finally {
            if (reader != null) {
                reader.close();
            }
        }
        assertClosedRetainedSizes(reader);
    }

    @Test
    public void testBigIntTypeWithNulls()
            throws Exception
    {
        int rows = 10000;
        OrcRecordReader reader = null;
        try (TempFile tempFile = createSingleColumnFileWithNullValues(rows)) {
            reader = createCustomOrcRecordReader(tempFile, OrcPredicate.TRUE, BIGINT, INITIAL_BATCH_SIZE);
            assertInitialRetainedSizes(reader, rows);

            long stripeReaderRetainedSize = reader.getCurrentStripeRetainedSizeInBytes();
            long streamReaderRetainedSize = reader.getStreamReaderRetainedSizeInBytes();
            long readerRetainedSize = reader.getRetainedSizeInBytes();
            long readerSystemMemoryUsage = reader.getSystemMemoryUsage();

            while (true) {
                int batchSize = reader.nextBatch();
                if (batchSize == -1) {
                    break;
                }

                Block block = reader.readBlock(0);
                assertEquals(block.getPositionCount(), batchSize);

                // We only verify the memory usage when the batchSize reaches MAX_BATCH_SIZE as batchSize may be
                // increasing during the test, which will cause the StreamReader buffer sizes to increase too.
                if (batchSize < MAX_BATCH_SIZE) {
                    continue;
                }

                // StripeReader memory should increase after reading a block.
                assertGreaterThan(reader.getCurrentStripeRetainedSizeInBytes(), stripeReaderRetainedSize);
                // There are no local buffers needed.
                assertEquals(reader.getStreamReaderRetainedSizeInBytes() - streamReaderRetainedSize, 0L);
                // The total retained size and system memory usage should be strictly larger than 0L because of the instance sizes.
                assertGreaterThan(reader.getRetainedSizeInBytes() - readerRetainedSize, 0L);
                assertGreaterThan(reader.getSystemMemoryUsage() - readerSystemMemoryUsage, 0L);
            }
        }
        finally {
            if (reader != null) {
                reader.close();
            }
        }
        assertClosedRetainedSizes(reader);
    }

    @Test
    public void testMapTypeWithNulls()
            throws Exception
    {
        Type mapType = METADATA.getType(new TypeSignature(StandardTypes.MAP, TypeSignatureParameter.of(BIGINT.getTypeSignature()), TypeSignatureParameter.of(BIGINT.getTypeSignature())));

        int rows = 10000;
        OrcRecordReader reader = null;
        try (TempFile tempFile = createSingleColumnMapFileWithNullValues(mapType, rows)) {
            reader = createCustomOrcRecordReader(tempFile, OrcPredicate.TRUE, mapType, INITIAL_BATCH_SIZE);
            assertInitialRetainedSizes(reader, rows);

            long stripeReaderRetainedSize = reader.getCurrentStripeRetainedSizeInBytes();
            long streamReaderRetainedSize = reader.getStreamReaderRetainedSizeInBytes();
            long readerRetainedSize = reader.getRetainedSizeInBytes();
            long readerSystemMemoryUsage = reader.getSystemMemoryUsage();

            while (true) {
                int batchSize = reader.nextBatch();
                if (batchSize == -1) {
                    break;
                }

                Block block = reader.readBlock(0);
                assertEquals(block.getPositionCount(), batchSize);

                // We only verify the memory usage when the batchSize reaches MAX_BATCH_SIZE as batchSize may be
                // increasing during the test, which will cause the StreamReader buffer sizes to increase too.
                if (batchSize < MAX_BATCH_SIZE) {
                    continue;
                }

                // StripeReader memory should increase after reading a block.
                assertGreaterThan(reader.getCurrentStripeRetainedSizeInBytes(), stripeReaderRetainedSize);
                // There are no local buffers needed.
                assertEquals(reader.getStreamReaderRetainedSizeInBytes() - streamReaderRetainedSize, 0L);
                // The total retained size and system memory usage should be strictly larger than 0L because of the instance sizes.
                assertGreaterThan(reader.getRetainedSizeInBytes() - readerRetainedSize, 0L);
                assertGreaterThan(reader.getSystemMemoryUsage() - readerSystemMemoryUsage, 0L);
            }
        }
        finally {
            if (reader != null) {
                reader.close();
            }
        }
        assertClosedRetainedSizes(reader);
    }

    /**
     * Write a file that contains a number of rows with 1 BIGINT column, and some rows have null values.
     */
    private static TempFile createSingleColumnFileWithNullValues(int rows)
            throws IOException, SerDeException
    {
        Serializer serde = new OrcSerde();
        TempFile tempFile = new TempFile();
        FileSinkOperator.RecordWriter writer = createOrcRecordWriter(tempFile.getFile(), ORC_12, CompressionKind.NONE, BIGINT);
        SettableStructObjectInspector objectInspector = createSettableStructObjectInspector("test", BIGINT);
        Object row = objectInspector.create();
        StructField field = objectInspector.getAllStructFieldRefs().get(0);

        for (int i = 0; i < rows; i++) {
            if (i % 10 == 0) {
                objectInspector.setStructFieldData(row, field, null);
            }
            else {
                objectInspector.setStructFieldData(row, field, (long) i);
            }

            Writable record = serde.serialize(row, objectInspector);
            writer.write(record);
        }

        writer.close(false);
        return tempFile;
    }

    /**
     * Write a file that contains a number of rows with 1 VARCHAR column, and all values are not null.
     */
    private static TempFile createSingleColumnVarcharFile(int count, int length)
            throws Exception
    {
        Serializer serde = new OrcSerde();
        TempFile tempFile = new TempFile();
        FileSinkOperator.RecordWriter writer = createOrcRecordWriter(tempFile.getFile(), ORC_12, CompressionKind.NONE, VARCHAR);
        SettableStructObjectInspector objectInspector = createSettableStructObjectInspector("test", VARCHAR);
        Object row = objectInspector.create();
        StructField field = objectInspector.getAllStructFieldRefs().get(0);

        for (int i = 0; i < count; i++) {
            objectInspector.setStructFieldData(row, field, Strings.repeat("0", length));
            Writable record = serde.serialize(row, objectInspector);
            writer.write(record);
        }

        writer.close(false);
        return tempFile;
    }

    /**
     * Write a file that contains a given number of maps where each row has 10 entries in total
     * and some entries have null keys/values.
     */
    private static TempFile createSingleColumnMapFileWithNullValues(Type mapType, int rows)
            throws IOException, SerDeException
    {
        Serializer serde = new OrcSerde();
        TempFile tempFile = new TempFile();
        FileSinkOperator.RecordWriter writer = createOrcRecordWriter(tempFile.getFile(), ORC_12, CompressionKind.NONE, mapType);
        SettableStructObjectInspector objectInspector = createSettableStructObjectInspector("test", mapType);
        Object row = objectInspector.create();
        StructField field = objectInspector.getAllStructFieldRefs().get(0);

        for (int i = 1; i <= rows; i++) {
            HashMap<Long, Long> map = new HashMap<>();

            for (int j = 1; j <= 8; j++) {
                Long value = (long) j;
                map.put(value, value);
            }

            // Add null values so that the StreamReader nullVectors are not empty.
            map.put(null, 0L);
            map.put(0L, null);

            objectInspector.setStructFieldData(row, field, map);
            Writable record = serde.serialize(row, objectInspector);
            writer.write(record);
        }
        writer.close(false);
        return tempFile;
    }

    private static void assertInitialRetainedSizes(OrcRecordReader reader, int rows)
    {
        assertEquals(reader.getReaderRowCount(), rows);
        assertEquals(reader.getReaderPosition(), 0);
        assertEquals(reader.getCurrentStripeRetainedSizeInBytes(), 0);
        // there will be object overheads
        assertGreaterThan(reader.getStreamReaderRetainedSizeInBytes(), 0L);
        // there will be object overheads
        assertGreaterThan(reader.getRetainedSizeInBytes(), 0L);
        assertEquals(reader.getSystemMemoryUsage(), 0);
    }

    private static void assertClosedRetainedSizes(OrcRecordReader reader)
    {
        assertEquals(reader.getCurrentStripeRetainedSizeInBytes(), 0);
        // after close() we still account for the StreamReader instance sizes.
        assertGreaterThan(reader.getStreamReaderRetainedSizeInBytes(), 0L);
        // after close() we still account for the StreamReader instance sizes.
        assertGreaterThan(reader.getRetainedSizeInBytes(), 0L);
        assertEquals(reader.getSystemMemoryUsage(), 0);
    }
}
