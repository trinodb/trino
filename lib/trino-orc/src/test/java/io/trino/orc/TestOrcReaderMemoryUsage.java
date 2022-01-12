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

import io.trino.orc.metadata.CompressionKind;
import io.trino.spi.Page;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.TypeSignatureParameter;
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
import static io.airlift.testing.Assertions.assertGreaterThanOrEqual;
import static io.trino.orc.OrcReader.INITIAL_BATCH_SIZE;
import static io.trino.orc.OrcReader.MAX_BATCH_SIZE;
import static io.trino.orc.OrcTester.Format.ORC_12;
import static io.trino.orc.OrcTester.createCustomOrcRecordReader;
import static io.trino.orc.OrcTester.createOrcRecordWriter;
import static io.trino.orc.OrcTester.createSettableStructObjectInspector;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.testng.Assert.assertEquals;

public class TestOrcReaderMemoryUsage
{
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
                Page page = reader.nextPage();
                if (page == null) {
                    break;
                }
                page = page.getLoadedPage();

                // We only verify the memory usage when the batchSize reaches MAX_BATCH_SIZE as batchSize may be
                // increasing during the test, which will cause the StreamReader buffer sizes to increase too.
                if (page.getPositionCount() < MAX_BATCH_SIZE) {
                    continue;
                }

                // StripeReader memory should increase after reading a block.
                assertGreaterThan(reader.getCurrentStripeRetainedSizeInBytes(), stripeReaderRetainedSize);
                // There may be some extra local buffers needed for dictionary data.
                assertGreaterThanOrEqual(reader.getStreamReaderRetainedSizeInBytes(), streamReaderRetainedSize);
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
                Page page = reader.nextPage();
                if (page == null) {
                    break;
                }
                page = page.getLoadedPage();

                // We only verify the memory usage when the batchSize reaches MAX_BATCH_SIZE as batchSize may be
                // increasing during the test, which will cause the StreamReader buffer sizes to increase too.
                if (page.getPositionCount() < MAX_BATCH_SIZE) {
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
        Type mapType = TESTING_TYPE_MANAGER.getType(new TypeSignature(StandardTypes.MAP, TypeSignatureParameter.typeParameter(BIGINT.getTypeSignature()), TypeSignatureParameter.typeParameter(BIGINT.getTypeSignature())));

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
                Page page = reader.nextPage();
                if (page == null) {
                    break;
                }
                page = page.getLoadedPage();

                // We only verify the memory usage when the batchSize reaches MAX_BATCH_SIZE as batchSize may be
                // increasing during the test, which will cause the StreamReader buffer sizes to increase too.
                if (page.getPositionCount() < MAX_BATCH_SIZE) {
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
            objectInspector.setStructFieldData(row, field, "0".repeat(length));
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
