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
import io.trino.orc.OrcDataSource;
import io.trino.orc.OrcRecordReader;
import io.trino.plugin.raptor.legacy.storage.OrcFileRewriter.OrcFileInfo;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlMap;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
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
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.BitSet;
import java.util.List;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.plugin.raptor.legacy.storage.OrcTestingUtil.createReader;
import static io.trino.plugin.raptor.legacy.storage.OrcTestingUtil.fileOrcDataSource;
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
import static java.nio.file.Files.readAllBytes;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestOrcFileRewriter
{
    private static final JsonCodec<OrcFileMetadata> METADATA_CODEC = jsonCodec(OrcFileMetadata.class);

    private final Path temporary;

    public TestOrcFileRewriter()
            throws IOException
    {
        temporary = createTempDirectory(null);
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        deleteRecursively(temporary, ALLOW_INSECURE);
    }

    @Test
    public void testRewrite()
            throws Exception
    {
        ArrayType arrayType = new ArrayType(BIGINT);
        ArrayType arrayOfArrayType = new ArrayType(arrayType);
        Type mapType = TESTING_TYPE_MANAGER.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                TypeSignatureParameter.typeParameter(createVarcharType(5).getTypeSignature()),
                TypeSignatureParameter.typeParameter(BOOLEAN.getTypeSignature())));
        List<Long> columnIds = ImmutableList.of(3L, 7L, 9L, 10L, 11L, 12L);
        DecimalType decimalType = DecimalType.createDecimalType(4, 4);

        List<Type> columnTypes = ImmutableList.of(BIGINT, createVarcharType(20), arrayType, mapType, arrayOfArrayType, decimalType);

        File file = temporary.resolve(randomUUID().toString()).toFile();
        try (OrcFileWriter writer = new OrcFileWriter(TESTING_TYPE_MANAGER, columnIds, columnTypes, file)) {
            List<Page> pages = rowPagesBuilder(columnTypes)
                    .row(123L, "hello", arrayBlockOf(BIGINT, 1, 2), sqlMapOf(createVarcharType(5), BOOLEAN, "k1", true), arrayBlockOf(arrayType, arrayBlockOf(BIGINT, 5)), new BigDecimal("2.3"))
                    .row(777L, "sky", arrayBlockOf(BIGINT, 3, 4), sqlMapOf(createVarcharType(5), BOOLEAN, "k2", false), arrayBlockOf(arrayType, arrayBlockOf(BIGINT, 6)), new BigDecimal("2.3"))
                    .row(456L, "bye", arrayBlockOf(BIGINT, 5, 6), sqlMapOf(createVarcharType(5), BOOLEAN, "k3", true), arrayBlockOf(arrayType, arrayBlockOf(BIGINT, 7)), new BigDecimal("2.3"))
                    .row(888L, "world", arrayBlockOf(BIGINT, 7, 8), sqlMapOf(createVarcharType(5), BOOLEAN, "k4", true), arrayBlockOf(arrayType, null, arrayBlockOf(BIGINT, 8), null), new BigDecimal("2.3"))
                    .row(999L, "done", arrayBlockOf(BIGINT, 9, 10), sqlMapOf(createVarcharType(5), BOOLEAN, "k5", true), arrayBlockOf(arrayType, arrayBlockOf(BIGINT, 9, 10)), new BigDecimal("2.3"))
                    .build();
            writer.appendPages(pages);
        }

        try (OrcDataSource dataSource = fileOrcDataSource(file)) {
            OrcRecordReader reader = createReader(dataSource, columnIds, columnTypes);

            assertThat(reader.getReaderRowCount()).isEqualTo(5);
            assertThat(reader.getFileRowCount()).isEqualTo(5);
            assertThat(reader.getSplitLength()).isEqualTo(file.length());

            Page page = reader.nextPage();
            assertThat(page.getPositionCount()).isEqualTo(5);

            Block column0 = page.getBlock(0);
            assertThat(column0.getPositionCount()).isEqualTo(5);
            for (int i = 0; i < 5; i++) {
                assertThat(column0.isNull(i)).isEqualTo(false);
            }
            assertThat(BIGINT.getLong(column0, 0)).isEqualTo(123L);
            assertThat(BIGINT.getLong(column0, 1)).isEqualTo(777L);
            assertThat(BIGINT.getLong(column0, 2)).isEqualTo(456L);
            assertThat(BIGINT.getLong(column0, 3)).isEqualTo(888L);
            assertThat(BIGINT.getLong(column0, 4)).isEqualTo(999L);

            Block column1 = page.getBlock(1);
            assertThat(column1.getPositionCount()).isEqualTo(5);
            for (int i = 0; i < 5; i++) {
                assertThat(column1.isNull(i)).isEqualTo(false);
            }
            assertThat(createVarcharType(20).getSlice(column1, 0)).isEqualTo(utf8Slice("hello"));
            assertThat(createVarcharType(20).getSlice(column1, 1)).isEqualTo(utf8Slice("sky"));
            assertThat(createVarcharType(20).getSlice(column1, 2)).isEqualTo(utf8Slice("bye"));
            assertThat(createVarcharType(20).getSlice(column1, 3)).isEqualTo(utf8Slice("world"));
            assertThat(createVarcharType(20).getSlice(column1, 4)).isEqualTo(utf8Slice("done"));

            Block column2 = page.getBlock(2);
            assertThat(column2.getPositionCount()).isEqualTo(5);
            for (int i = 0; i < 5; i++) {
                assertThat(column2.isNull(i)).isEqualTo(false);
            }
            assertThat(arrayBlocksEqual(BIGINT, arrayType.getObject(column2, 0), arrayBlockOf(BIGINT, 1, 2))).isTrue();
            assertThat(arrayBlocksEqual(BIGINT, arrayType.getObject(column2, 1), arrayBlockOf(BIGINT, 3, 4))).isTrue();
            assertThat(arrayBlocksEqual(BIGINT, arrayType.getObject(column2, 2), arrayBlockOf(BIGINT, 5, 6))).isTrue();
            assertThat(arrayBlocksEqual(BIGINT, arrayType.getObject(column2, 3), arrayBlockOf(BIGINT, 7, 8))).isTrue();
            assertThat(arrayBlocksEqual(BIGINT, arrayType.getObject(column2, 4), arrayBlockOf(BIGINT, 9, 10))).isTrue();

            Block column3 = page.getBlock(3);
            assertThat(column3.getPositionCount()).isEqualTo(5);
            for (int i = 0; i < 5; i++) {
                assertThat(column3.isNull(i)).isEqualTo(false);
            }
            assertThat(sqlMapEqual(createVarcharType(5), BOOLEAN, (SqlMap) mapType.getObject(column3, 0), sqlMapOf(createVarcharType(5), BOOLEAN, "k1", true))).isTrue();
            assertThat(sqlMapEqual(createVarcharType(5), BOOLEAN, (SqlMap) mapType.getObject(column3, 1), sqlMapOf(createVarcharType(5), BOOLEAN, "k2", false))).isTrue();
            assertThat(sqlMapEqual(createVarcharType(5), BOOLEAN, (SqlMap) mapType.getObject(column3, 2), sqlMapOf(createVarcharType(5), BOOLEAN, "k3", true))).isTrue();
            assertThat(sqlMapEqual(createVarcharType(5), BOOLEAN, (SqlMap) mapType.getObject(column3, 3), sqlMapOf(createVarcharType(5), BOOLEAN, "k4", true))).isTrue();
            assertThat(sqlMapEqual(createVarcharType(5), BOOLEAN, (SqlMap) mapType.getObject(column3, 4), sqlMapOf(createVarcharType(5), BOOLEAN, "k5", true))).isTrue();

            Block column4 = page.getBlock(4);
            assertThat(column4.getPositionCount()).isEqualTo(5);
            for (int i = 0; i < 5; i++) {
                assertThat(column4.isNull(i)).isEqualTo(false);
            }
            assertThat(arrayBlocksEqual(arrayType, arrayOfArrayType.getObject(column4, 0), arrayBlockOf(arrayType, arrayBlockOf(BIGINT, 5)))).isTrue();
            assertThat(arrayBlocksEqual(arrayType, arrayOfArrayType.getObject(column4, 1), arrayBlockOf(arrayType, arrayBlockOf(BIGINT, 6)))).isTrue();
            assertThat(arrayBlocksEqual(arrayType, arrayOfArrayType.getObject(column4, 2), arrayBlockOf(arrayType, arrayBlockOf(BIGINT, 7)))).isTrue();
            assertThat(arrayBlocksEqual(arrayType, arrayOfArrayType.getObject(column4, 3), arrayBlockOf(arrayType, null, arrayBlockOf(BIGINT, 8), null))).isTrue();
            assertThat(arrayBlocksEqual(arrayType, arrayOfArrayType.getObject(column4, 4), arrayBlockOf(arrayType, arrayBlockOf(BIGINT, 9, 10)))).isTrue();

            assertThat(reader.nextPage()).isNull();

            OrcFileMetadata orcFileMetadata = METADATA_CODEC.fromJson(reader.getUserMetadata().get(OrcFileMetadata.KEY).getBytes());
            assertThat(orcFileMetadata).isEqualTo(new OrcFileMetadata(ImmutableMap.<Long, TypeId>builder()
                    .put(3L, BIGINT.getTypeId())
                    .put(7L, createVarcharType(20).getTypeId())
                    .put(9L, arrayType.getTypeId())
                    .put(10L, mapType.getTypeId())
                    .put(11L, arrayOfArrayType.getTypeId())
                    .put(12L, decimalType.getTypeId())
                    .buildOrThrow()));
        }

        BitSet rowsToDelete = new BitSet(5);
        rowsToDelete.set(1);
        rowsToDelete.set(3);
        rowsToDelete.set(4);

        File newFile = temporary.resolve(randomUUID().toString()).toFile();
        OrcFileInfo info = OrcFileRewriter.rewrite(TESTING_TYPE_MANAGER, file, newFile, rowsToDelete);
        assertThat(info.getRowCount()).isEqualTo(2);
        assertThat(info.getUncompressedSize()).isEqualTo(182);

        try (OrcDataSource dataSource = fileOrcDataSource(newFile)) {
            OrcRecordReader reader = createReader(dataSource, columnIds, columnTypes);

            assertThat(reader.getReaderRowCount()).isEqualTo(2);
            assertThat(reader.getFileRowCount()).isEqualTo(2);
            assertThat(reader.getSplitLength()).isEqualTo(newFile.length());

            Page page = reader.nextPage();
            assertThat(page.getPositionCount()).isEqualTo(2);

            Block column0 = page.getBlock(0);
            assertThat(column0.getPositionCount()).isEqualTo(2);
            for (int i = 0; i < 2; i++) {
                assertThat(column0.isNull(i)).isEqualTo(false);
            }
            assertThat(BIGINT.getLong(column0, 0)).isEqualTo(123L);
            assertThat(BIGINT.getLong(column0, 1)).isEqualTo(456L);

            Block column1 = page.getBlock(1);
            assertThat(column1.getPositionCount()).isEqualTo(2);
            for (int i = 0; i < 2; i++) {
                assertThat(column1.isNull(i)).isEqualTo(false);
            }
            assertThat(createVarcharType(20).getSlice(column1, 0)).isEqualTo(utf8Slice("hello"));
            assertThat(createVarcharType(20).getSlice(column1, 1)).isEqualTo(utf8Slice("bye"));

            Block column2 = page.getBlock(2);
            assertThat(column2.getPositionCount()).isEqualTo(2);
            for (int i = 0; i < 2; i++) {
                assertThat(column2.isNull(i)).isEqualTo(false);
            }
            assertThat(arrayBlocksEqual(BIGINT, arrayType.getObject(column2, 0), arrayBlockOf(BIGINT, 1, 2))).isTrue();
            assertThat(arrayBlocksEqual(BIGINT, arrayType.getObject(column2, 1), arrayBlockOf(BIGINT, 5, 6))).isTrue();

            Block column3 = page.getBlock(3);
            assertThat(column3.getPositionCount()).isEqualTo(2);
            for (int i = 0; i < 2; i++) {
                assertThat(column3.isNull(i)).isEqualTo(false);
            }
            assertThat(sqlMapEqual(createVarcharType(5), BOOLEAN, (SqlMap) mapType.getObject(column3, 0), sqlMapOf(createVarcharType(5), BOOLEAN, "k1", true))).isTrue();
            assertThat(sqlMapEqual(createVarcharType(5), BOOLEAN, (SqlMap) mapType.getObject(column3, 1), sqlMapOf(createVarcharType(5), BOOLEAN, "k3", true))).isTrue();

            Block column4 = page.getBlock(4);
            assertThat(column4.getPositionCount()).isEqualTo(2);
            for (int i = 0; i < 2; i++) {
                assertThat(column4.isNull(i)).isEqualTo(false);
            }
            assertThat(arrayBlocksEqual(arrayType, arrayOfArrayType.getObject(column4, 0), arrayBlockOf(arrayType, arrayBlockOf(BIGINT, 5)))).isTrue();
            assertThat(arrayBlocksEqual(arrayType, arrayOfArrayType.getObject(column4, 1), arrayBlockOf(arrayType, arrayBlockOf(BIGINT, 7)))).isTrue();

            assertThat(reader.nextPage()).isEqualTo(null);

            OrcFileMetadata orcFileMetadata = METADATA_CODEC.fromJson(reader.getUserMetadata().get(OrcFileMetadata.KEY).getBytes());
            assertThat(orcFileMetadata).isEqualTo(new OrcFileMetadata(ImmutableMap.<Long, TypeId>builder()
                    .put(3L, BIGINT.getTypeId())
                    .put(7L, createVarcharType(20).getTypeId())
                    .put(9L, arrayType.getTypeId())
                    .put(10L, mapType.getTypeId())
                    .put(11L, arrayOfArrayType.getTypeId())
                    .put(12L, decimalType.getTypeId())
                    .buildOrThrow()));
        }
    }

    @Test
    public void testRewriteAllRowsDeleted()
            throws Exception
    {
        List<Long> columnIds = ImmutableList.of(3L);
        List<Type> columnTypes = ImmutableList.of(BIGINT);

        File file = temporary.resolve(randomUUID().toString()).toFile();
        try (OrcFileWriter writer = new OrcFileWriter(TESTING_TYPE_MANAGER, columnIds, columnTypes, file)) {
            writer.appendPages(rowPagesBuilder(columnTypes).row(123L).row(456L).build());
        }

        BitSet rowsToDelete = new BitSet();
        rowsToDelete.set(0);
        rowsToDelete.set(1);

        File newFile = temporary.resolve(randomUUID().toString()).toFile();
        OrcFileInfo info = OrcFileRewriter.rewrite(TESTING_TYPE_MANAGER, file, newFile, rowsToDelete);
        assertThat(info.getRowCount()).isEqualTo(0);
        assertThat(info.getUncompressedSize()).isEqualTo(0);

        assertThat(newFile.exists()).isFalse();
    }

    @Test
    public void testRewriteNoRowsDeleted()
            throws Exception
    {
        List<Long> columnIds = ImmutableList.of(3L);
        List<Type> columnTypes = ImmutableList.of(BIGINT);

        File file = temporary.resolve(randomUUID().toString()).toFile();
        try (OrcFileWriter writer = new OrcFileWriter(TESTING_TYPE_MANAGER, columnIds, columnTypes, file)) {
            writer.appendPages(rowPagesBuilder(columnTypes).row(123L).row(456L).build());
        }

        BitSet rowsToDelete = new BitSet();

        File newFile = temporary.resolve(randomUUID().toString()).toFile();
        OrcFileInfo info = OrcFileRewriter.rewrite(TESTING_TYPE_MANAGER, file, newFile, rowsToDelete);
        assertThat(info.getRowCount()).isEqualTo(2);
        assertThat(info.getUncompressedSize()).isEqualTo(18);

        assertThat(readAllBytes(newFile.toPath())).isEqualTo(readAllBytes(file.toPath()));
    }

    @Test
    public void testUncompressedSize()
            throws Exception
    {
        List<Long> columnIds = ImmutableList.of(1L, 2L, 3L, 4L, 5L);
        List<Type> columnTypes = ImmutableList.of(BOOLEAN, BIGINT, DOUBLE, createVarcharType(10), VARBINARY);

        File file = temporary.resolve(randomUUID().toString()).toFile();
        try (OrcFileWriter writer = new OrcFileWriter(TESTING_TYPE_MANAGER, columnIds, columnTypes, file)) {
            List<Page> pages = rowPagesBuilder(columnTypes)
                    .row(true, 123L, 98.7, "hello", utf8Slice("abc"))
                    .row(false, 456L, 65.4, "world", utf8Slice("xyz"))
                    .row(null, null, null, null, null)
                    .build();
            writer.appendPages(pages);
        }

        File newFile = temporary.resolve(randomUUID().toString()).toFile();
        OrcFileInfo info = OrcFileRewriter.rewrite(TESTING_TYPE_MANAGER, file, newFile, new BitSet());
        assertThat(info.getRowCount()).isEqualTo(3);
        assertThat(info.getUncompressedSize()).isEqualTo(106);
    }
}
