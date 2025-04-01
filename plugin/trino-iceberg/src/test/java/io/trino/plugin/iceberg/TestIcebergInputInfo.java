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
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
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
        assertInputInfo(tableName, ImmutableList.of("regionkey: identity", "name_trunc: truncate[1]"), "PARQUET", 9);
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testInputWithoutPartitioning()
    {
        String tableName = "test_input_info_without_part_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM nation WHERE nationkey < 10", 10);
        assertInputInfo(tableName, ImmutableList.of(), "PARQUET", 1);
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testInputWithOrcFileFormat()
    {
        String tableName = "test_input_info_with_orc_file_format_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " WITH (format = 'ORC') AS SELECT * FROM nation WHERE nationkey < 10", 10);
        assertInputInfo(tableName, ImmutableList.of(), "ORC", 1);
        assertUpdate("DROP TABLE " + tableName);
    }

    private void assertInputInfo(String tableName, List<String> partitionFields, String expectedFileFormat, long dataFiles)
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
                    icebergInputInfo.snapshotId(),
                    partitionFields,
                    expectedFileFormat,
                    Optional.of("10"),
                    Optional.empty(),
                    Optional.of(String.valueOf(dataFiles)),
                    Optional.of("0")));
        });
    }
}
