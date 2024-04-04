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
import io.trino.spi.block.SqlMap;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeSignatureParameter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

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
import static io.trino.testing.StructuralTestUtil.sqlMapEqual;
import static io.trino.testing.StructuralTestUtil.sqlMapOf;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestShardWriter
{
    private final Path directory;

    private static final JsonCodec<OrcFileMetadata> METADATA_CODEC = jsonCodec(OrcFileMetadata.class);

    public TestShardWriter()
            throws IOException
    {
        directory = createTempDirectory(null);
    }

    @AfterAll
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
                .row(123L, "hello", wrappedBuffer(bytes1), 123.456, true, arrayBlockOf(BIGINT, 1, 2), sqlMapOf(createVarcharType(5), BOOLEAN, "k1", true), arrayBlockOf(arrayType, arrayBlockOf(BIGINT, 5)))
                .row(null, "world", null, Double.POSITIVE_INFINITY, null, arrayBlockOf(BIGINT, 3, null), sqlMapOf(createVarcharType(5), BOOLEAN, "k2", null), arrayBlockOf(arrayType, null, arrayBlockOf(BIGINT, 6, 7)))
                .row(456L, "bye \u2603", wrappedBuffer(bytes3), Double.NaN, false, arrayBlockOf(BIGINT), sqlMapOf(createVarcharType(5), BOOLEAN, "k3", false), arrayBlockOf(arrayType, arrayBlockOf(BIGINT)));

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(new EmptyClassLoader());
                OrcFileWriter writer = new OrcFileWriter(TESTING_TYPE_MANAGER, columnIds, columnTypes, file)) {
            writer.appendPages(rowPagesBuilder.build());
        }

        try (OrcDataSource dataSource = fileOrcDataSource(file)) {
            OrcRecordReader reader = createReader(dataSource, columnIds, columnTypes);
            assertThat(reader.getReaderRowCount()).isEqualTo(3);
            assertThat(reader.getReaderPosition()).isEqualTo(0);
            assertThat(reader.getFileRowCount()).isEqualTo(reader.getReaderRowCount());
            assertThat(reader.getFilePosition()).isEqualTo(reader.getFilePosition());

            Page page = reader.nextPage();
            assertThat(page.getPositionCount()).isEqualTo(3);
            assertThat(reader.getReaderPosition()).isEqualTo(0);
            assertThat(reader.getFilePosition()).isEqualTo(reader.getFilePosition());

            Block column0 = page.getBlock(0);
            assertThat(column0.isNull(0)).isEqualTo(false);
            assertThat(column0.isNull(1)).isEqualTo(true);
            assertThat(column0.isNull(2)).isEqualTo(false);
            assertThat(BIGINT.getLong(column0, 0)).isEqualTo(123L);
            assertThat(BIGINT.getLong(column0, 2)).isEqualTo(456L);

            Block column1 = page.getBlock(1);
            assertThat(createVarcharType(10).getSlice(column1, 0)).isEqualTo(utf8Slice("hello"));
            assertThat(createVarcharType(10).getSlice(column1, 1)).isEqualTo(utf8Slice("world"));
            assertThat(createVarcharType(10).getSlice(column1, 2)).isEqualTo(utf8Slice("bye \u2603"));

            Block column2 = page.getBlock(2);
            assertThat(VARBINARY.getSlice(column2, 0)).isEqualTo(wrappedBuffer(bytes1));
            assertThat(column2.isNull(1)).isEqualTo(true);
            assertThat(VARBINARY.getSlice(column2, 2)).isEqualTo(wrappedBuffer(bytes3));

            Block column3 = page.getBlock(3);
            assertThat(column3.isNull(0)).isEqualTo(false);
            assertThat(column3.isNull(1)).isEqualTo(false);
            assertThat(column3.isNull(2)).isEqualTo(false);
            assertThat(DOUBLE.getDouble(column3, 0)).isEqualTo(123.456);
            assertThat(DOUBLE.getDouble(column3, 1)).isEqualTo(Double.POSITIVE_INFINITY);
            assertThat(DOUBLE.getDouble(column3, 2)).isNaN();

            Block column4 = page.getBlock(4);
            assertThat(column4.isNull(0)).isEqualTo(false);
            assertThat(column4.isNull(1)).isEqualTo(true);
            assertThat(column4.isNull(2)).isEqualTo(false);
            assertThat(BOOLEAN.getBoolean(column4, 0)).isEqualTo(true);
            assertThat(BOOLEAN.getBoolean(column4, 2)).isEqualTo(false);

            Block column5 = page.getBlock(5);
            assertThat(column5.getPositionCount()).isEqualTo(3);

            assertThat(arrayBlocksEqual(BIGINT, arrayType.getObject(column5, 0), arrayBlockOf(BIGINT, 1, 2))).isTrue();
            assertThat(arrayBlocksEqual(BIGINT, arrayType.getObject(column5, 1), arrayBlockOf(BIGINT, 3, null))).isTrue();
            assertThat(arrayBlocksEqual(BIGINT, arrayType.getObject(column5, 2), arrayBlockOf(BIGINT))).isTrue();

            Block column6 = page.getBlock(6);
            assertThat(column6.getPositionCount()).isEqualTo(3);

            assertThat(sqlMapEqual(createVarcharType(5), BOOLEAN, (SqlMap) mapType.getObject(column6, 0), sqlMapOf(createVarcharType(5), BOOLEAN, "k1", true))).isTrue();
            SqlMap object = (SqlMap) mapType.getObject(column6, 1);
            SqlMap k2 = sqlMapOf(createVarcharType(5), BOOLEAN, "k2", null);
            assertThat(sqlMapEqual(createVarcharType(5), BOOLEAN, object, k2)).isTrue();
            assertThat(sqlMapEqual(createVarcharType(5), BOOLEAN, (SqlMap) mapType.getObject(column6, 2), sqlMapOf(createVarcharType(5), BOOLEAN, "k3", false))).isTrue();

            Block column7 = page.getBlock(7);
            assertThat(column7.getPositionCount()).isEqualTo(3);

            assertThat(arrayBlocksEqual(arrayType, arrayOfArrayType.getObject(column7, 0), arrayBlockOf(arrayType, arrayBlockOf(BIGINT, 5)))).isTrue();
            assertThat(arrayBlocksEqual(arrayType, arrayOfArrayType.getObject(column7, 1), arrayBlockOf(arrayType, null, arrayBlockOf(BIGINT, 6, 7)))).isTrue();
            assertThat(arrayBlocksEqual(arrayType, arrayOfArrayType.getObject(column7, 2), arrayBlockOf(arrayType, arrayBlockOf(BIGINT)))).isTrue();

            assertThat(reader.nextPage()).isNull();
            assertThat(reader.getReaderPosition()).isEqualTo(3);
            assertThat(reader.getFilePosition()).isEqualTo(reader.getFilePosition());

            OrcFileMetadata orcFileMetadata = METADATA_CODEC.fromJson(reader.getUserMetadata().get(OrcFileMetadata.KEY).getBytes());
            assertThat(orcFileMetadata).isEqualTo(new OrcFileMetadata(ImmutableMap.<Long, TypeId>builder()
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
        assertThat(crcFile.exists()).isFalse();
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
