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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.apache.iceberg.BaseTable;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.plugin.iceberg.IcebergTestUtils.loadTable;
import static io.trino.plugin.iceberg.util.EqualityDeleteUtils.writeEqualityDeleteForTable;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergInputInfo
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setInitialTables(ImmutableList.of(TpchTable.NATION))
                .build();
    }

    @Test
    public void testInputWithPartitioning()
    {
        String tableName = "test_input_info_with_part_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (partitioning = ARRAY['regionkey', 'truncate(name, 1)']) AS SELECT * FROM nation WHERE nationkey < 10", 10);
        assertInputInfo(tableName, ImmutableList.of("regionkey: identity", "name_trunc: truncate[1]"), "PARQUET", 9, 0, 0);
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testInputWithoutPartitioning()
    {
        String tableName = "test_input_info_without_part_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM nation WHERE nationkey < 10", 10);
        assertInputInfo(tableName, ImmutableList.of(), "PARQUET", 1, 0, 0);
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testInputWithOrcFileFormat()
    {
        String tableName = "test_input_info_with_orc_file_format_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (format = 'ORC') AS SELECT * FROM nation WHERE nationkey < 10", 10);
        assertInputInfo(tableName, ImmutableList.of(), "ORC", 1, 0, 0);
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testInputWithPositionDeleteFile()
    {
        String tableName = "test_input_info_with_position_delete_file_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM nation WHERE nationkey < 10", 10);
        assertUpdate("DELETE FROM " + tableName + " WHERE nationkey = 1", 1);

        assertInputInfo(tableName, ImmutableList.of(), "PARQUET", 1, 1, 0);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testInputWithEqualityDeleteFile()
            throws Exception
    {
        String tableName = "test_input_info_with_equality_delete_file_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM nation WHERE nationkey < 10", 10);
        TrinoFileSystemFactory fileSystemFactory = getFileSystemFactory(getQueryRunner());
        BaseTable table = loadTable(tableName, getHiveMetastore(getQueryRunner()), fileSystemFactory, "iceberg", "tpch");
        writeEqualityDeleteForTable(table, fileSystemFactory, Optional.empty(), Optional.empty(), ImmutableMap.of("nationkey", 1L), Optional.empty());

        assertInputInfo(tableName, ImmutableList.of(), "PARQUET", 1, 0, 1);

        assertUpdate("DROP TABLE " + tableName);
    }

    private void assertInputInfo(String tableName, List<String> partitionFields, String expectedFileFormat, long dataFiles, long positionDeleteFiles, long equalityDeleteFiles)
    {
        inTransaction(session -> {
            Metadata metadata = getQueryRunner().getPlannerContext().getMetadata();
            QualifiedObjectName qualifiedObjectName = new QualifiedObjectName(
                    session.getCatalog().orElse(ICEBERG_CATALOG),
                    session.getSchema().orElse("tpch"),
                    tableName);
            Optional<TableHandle> tableHandle = metadata.getTableHandle(session, qualifiedObjectName);
            assertThat(tableHandle).isPresent();
            Optional<Object> tableInfo = metadata.getInfo(session, tableHandle.get());
            assertThat(tableInfo).isPresent();
            IcebergInputInfo icebergInputInfo = (IcebergInputInfo) tableInfo.get();
            assertThat(icebergInputInfo).isEqualTo(new IcebergInputInfo(
                    2,
                    icebergInputInfo.snapshotId(),
                    partitionFields,
                    expectedFileFormat,
                    Optional.of("10"),
                    Optional.empty(),
                    Optional.of(String.valueOf(dataFiles)),
                    Optional.of(String.valueOf(positionDeleteFiles + equalityDeleteFiles)),
                    Optional.of(String.valueOf(positionDeleteFiles)),
                    Optional.of(String.valueOf(equalityDeleteFiles))));
        });
    }
}
