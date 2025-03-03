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
package io.trino.plugin.iceberg.delete;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInput;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.IcebergTestUtils;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.LongBitmapDataProvider;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.List;

import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.plugin.iceberg.delete.DeletionVectors.readDeletionVector;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.apache.iceberg.FileFormat.PUFFIN;
import static org.assertj.core.api.Assertions.assertThat;

final class TestDeletionVectors
        extends AbstractTestQueryFramework
{
    private HiveMetastore metastore;
    private TrinoFileSystemFactory fileSystemFactory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = IcebergQueryRunner.builder()
                .addIcebergProperty("iceberg.format-version", "3")
                .build();
        metastore = getHiveMetastore(queryRunner);
        fileSystemFactory = getFileSystemFactory(queryRunner);
        return queryRunner;
    }

    @Test
    void testReadDeletionVector()
            throws Exception
    {
        try (TestTable table = newTrinoTable("test_dv", "(x int)", List.of("1", "2", "3", "4", "5"))) {
            BaseTable icebergTable = loadTable(table.getName());
            String filePath = (String) computeScalar("SELECT file_path FROM \"" + table.getName() + "$files\"");

            // Write deletion vectors in the Puffin format
            OutputFileFactory fileFactory = OutputFileFactory.builderFor(icebergTable, 1, 1).format(PUFFIN).build();
            BaseDVFileWriter writer = new BaseDVFileWriter(fileFactory, _ -> null);
            writer.delete(filePath, 0, icebergTable.spec(), null);
            writer.delete(filePath, 2, icebergTable.spec(), null);
            writer.delete(filePath, 4, icebergTable.spec(), null);
            writer.close();

            DeleteWriteResult result = writer.result();
            List<DeleteFile> deleteFiles = result.deleteFiles();
            assertThat(deleteFiles).hasSize(1);
            DeleteFile deleteFile = deleteFiles.getFirst();

            // Verify deletion vectors
            LongBitmapDataProvider deletedRows = new Roaring64Bitmap();
            try (TrinoInput input = fileSystemFactory.create(SESSION).newInputFile(Location.of(deleteFile.location())).newInput()) {
                readDeletionVector(input, deleteFile.recordCount(), deleteFile.contentOffset(), deleteFile.contentSizeInBytes(), deletedRows);
            }
            assertThat(deletedRows.getLongCardinality()).isEqualTo(3);
            assertThat(deletedRows.contains(0)).isTrue();
            assertThat(deletedRows.contains(1)).isFalse();
            assertThat(deletedRows.contains(2)).isTrue();
            assertThat(deletedRows.contains(3)).isFalse();
            assertThat(deletedRows.contains(4)).isTrue();
        }
    }

    private BaseTable loadTable(String tableName)
    {
        return IcebergTestUtils.loadTable(tableName, metastore, fileSystemFactory, "iceberg", "tpch");
    }
}
