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
package io.trino.plugin.raptor.legacy.storage;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.trino.RowPagesBuilder;
import io.trino.orc.OrcDataSource;
import io.trino.orc.OrcRecordReader;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeSignatureParameter;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.raptor.legacy.storage.OrcTestingUtil.createReader;
import static io.trino.plugin.raptor.legacy.storage.OrcTestingUtil.fileOrcDataSource;
import static io.trino.plugin.raptor.legacy.storage.OrcTestingUtil.octets;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.StructuralTestUtil.arrayBlockOf;
import static io.trino.testing.StructuralTestUtil.arrayBlocksEqual;
import static io.trino.testing.StructuralTestUtil.mapBlockOf;
import static io.trino.testing.StructuralTestUtil.mapBlocksEqual;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.nio.file.Files.createTempDirectory;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestShardWriter
{
    private Path directory;

    private static final JsonCodec<OrcFileMetadata> METADATA_CODEC = jsonCodec(OrcFileMetadata.class);

    @BeforeClass
    public void setup()
            throws IOException
    {
        directory = createTempDirectory(null);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        deleteRecursively(directory, ALLOW_INSECURE);
    }

    @Test
    public void testWriter()
            throws Exception
    {
        List<Long> columnIds = ImmutableList.of(1L, 2L, 4L, 6L, 7L, 8L, 9L, 10L);
        ArrayType arrayType = new ArrayType(BIGINT);
        ArrayType arrayOfArrayType = new ArrayType(arrayType);
        Type mapType = TESTING_TYPE_MANAGER.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                TypeSignatureParameter.typeParameter(createVarcharType(10).getTypeSignature()),
                TypeSignatureParameter.typeParameter(BOOLEAN.getTypeSignature())));
        List<Type> columnTypes = ImmutableList.of(BIGINT, createVarcharType(10), VARBINARY, DOUBLE, BOOLEAN, arrayType, mapType, arrayOfArrayType);
        File file = directory.resolve(System.nanoTime() + ".orc").toFile();

        byte[] bytes1 = octets(0x00, 0xFE, 0xFF);
        byte[] bytes3 = octets(0x01, 0x02, 0x19, 0x80);

        RowPagesBuilder rowPagesBuilder = RowPagesBuilder.rowPagesBuilder(columnTypes)
                .row(123L, "hello", wrappedBuffer(bytes1), 123.456, true, arrayBlockOf(BIGINT, 1, 2), mapBlockOf(createVarcharType(5), BOOLEAN, "k1", true), arrayBlockOf(arrayType, arrayBlockOf(BIGINT, 5)))
                .row(null, "world", null, Double.POSITIVE_INFINITY, null, arrayBlockOf(BIGINT, 3, null), mapBlockOf(createVarcharType(5), BOOLEAN, "k2", null), arrayBlockOf(arrayType, null, arrayBlockOf(BIGINT, 6, 7)))
                .row(456L, "bye \u2603", wrappedBuffer(bytes3), Double.NaN, false, arrayBlockOf(BIGINT), mapBlockOf(createVarcharType(5), BOOLEAN, "k3", false), arrayBlockOf(arrayType, arrayBlockOf(BIGINT)));

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(new EmptyClassLoader());
                OrcFileWriter writer = new OrcFileWriter(columnIds, columnTypes, file)) {
            writer.appendPages(rowPagesBuilder.build());
        }

        try (OrcDataSource dataSource = fileOrcDataSource(file)) {
            OrcRecordReader reader = createReader(dataSource, columnIds, columnTypes);
            assertEquals(reader.getReaderRowCount(), 3);
            assertEquals(reader.getReaderPosition(), 0);
            assertEquals(reader.getFileRowCount(), reader.getReaderRowCount());
            assertEquals(reader.getFilePosition(), reader.getFilePosition());

            Page page = reader.nextPage();
            assertEquals(page.getPositionCount(), 3);
            assertEquals(reader.getReaderPosition(), 0);
            assertEquals(reader.getFilePosition(), reader.getFilePosition());

            Block column0 = page.getBlock(0);
            assertEquals(column0.isNull(0), false);
            assertEquals(column0.isNull(1), true);
            assertEquals(column0.isNull(2), false);
            assertEquals(BIGINT.getLong(column0, 0), 123L);
            assertEquals(BIGINT.getLong(column0, 2), 456L);

            Block column1 = page.getBlock(1);
            assertEquals(createVarcharType(10).getSlice(column1, 0), utf8Slice("hello"));
            assertEquals(createVarcharType(10).getSlice(column1, 1), utf8Slice("world"));
            assertEquals(createVarcharType(10).getSlice(column1, 2), utf8Slice("bye \u2603"));

            Block column2 = page.getBlock(2);
            assertEquals(VARBINARY.getSlice(column2, 0), wrappedBuffer(bytes1));
            assertEquals(column2.isNull(1), true);
            assertEquals(VARBINARY.getSlice(column2, 2), wrappedBuffer(bytes3));

            Block column3 = page.getBlock(3);
            assertEquals(column3.isNull(0), false);
            assertEquals(column3.isNull(1), false);
            assertEquals(column3.isNull(2), false);
            assertEquals(DOUBLE.getDouble(column3, 0), 123.456);
            assertEquals(DOUBLE.getDouble(column3, 1), Double.POSITIVE_INFINITY);
            assertEquals(DOUBLE.getDouble(column3, 2), Double.NaN);

            Block column4 = page.getBlock(4);
            assertEquals(column4.isNull(0), false);
            assertEquals(column4.isNull(1), true);
            assertEquals(column4.isNull(2), false);
            assertEquals(BOOLEAN.getBoolean(column4, 0), true);
            assertEquals(BOOLEAN.getBoolean(column4, 2), false);

            Block column5 = page.getBlock(5);
            assertEquals(column5.getPositionCount(), 3);

            assertTrue(arrayBlocksEqual(BIGINT, arrayType.getObject(column5, 0), arrayBlockOf(BIGINT, 1, 2)));
            assertTrue(arrayBlocksEqual(BIGINT, arrayType.getObject(column5, 1), arrayBlockOf(BIGINT, 3, null)));
            assertTrue(arrayBlocksEqual(BIGINT, arrayType.getObject(column5, 2), arrayBlockOf(BIGINT)));

            Block column6 = page.getBlock(6);
            assertEquals(column6.getPositionCount(), 3);

            assertTrue(mapBlocksEqual(createVarcharType(5), BOOLEAN, arrayType.getObject(column6, 0), mapBlockOf(createVarcharType(5), BOOLEAN, "k1", true)));
            Block object = arrayType.getObject(column6, 1);
            Block k2 = mapBlockOf(createVarcharType(5), BOOLEAN, "k2", null);
            assertTrue(mapBlocksEqual(createVarcharType(5), BOOLEAN, object, k2));
            assertTrue(mapBlocksEqual(createVarcharType(5), BOOLEAN, arrayType.getObject(column6, 2), mapBlockOf(createVarcharType(5), BOOLEAN, "k3", false)));

            Block column7 = page.getBlock(7);
            assertEquals(column7.getPositionCount(), 3);

            assertTrue(arrayBlocksEqual(arrayType, arrayOfArrayType.getObject(column7, 0), arrayBlockOf(arrayType, arrayBlockOf(BIGINT, 5))));
            assertTrue(arrayBlocksEqual(arrayType, arrayOfArrayType.getObject(column7, 1), arrayBlockOf(arrayType, null, arrayBlockOf(BIGINT, 6, 7))));
            assertTrue(arrayBlocksEqual(arrayType, arrayOfArrayType.getObject(column7, 2), arrayBlockOf(arrayType, arrayBlockOf(BIGINT))));

            assertNull(reader.nextPage());
            assertEquals(reader.getReaderPosition(), 3);
            assertEquals(reader.getFilePosition(), reader.getFilePosition());

            OrcFileMetadata orcFileMetadata = METADATA_CODEC.fromJson(reader.getUserMetadata().get(OrcFileMetadata.KEY).getBytes());
            assertEquals(orcFileMetadata, new OrcFileMetadata(ImmutableMap.<Long, TypeId>builder()
                    .put(1L, BIGINT.getTypeId())
                    .put(2L, createVarcharType(10).getTypeId())
                    .put(4L, VARBINARY.getTypeId())
                    .put(6L, DOUBLE.getTypeId())
                    .put(7L, BOOLEAN.getTypeId())
                    .put(8L, arrayType.getTypeId())
                    .put(9L, mapType.getTypeId())
                    .put(10L, arrayOfArrayType.getTypeId())
                    .buildOrThrow()));
        }

        File crcFile = new File(file.getParentFile(), "." + file.getName() + ".crc");
        assertFalse(crcFile.exists());
    }

    @SuppressWarnings("EmptyClass")
    private static class EmptyClassLoader
            extends ClassLoader
    {
        protected EmptyClassLoader()
        {
            super(null);
        }
    }
}
