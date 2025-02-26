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

import io.trino.testing.sql.TestTable;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.iceberg.IcebergFileFormat.ORC;
import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergOrcSystemTables
        extends BaseIcebergSystemTables
{
    public TestIcebergOrcSystemTables()
    {
        super(ORC);
    }

    @Test
    @Override
    public void testPropertiesTable()
    {
        try (TestTable table = newTrinoTable("test_properties", "(x BIGINT,y DOUBLE) WITH (sorted_by = ARRAY['y'])")) {
            Table icebergTable = loadTable(table.getName());
            assertThat(getTableProperties(table.getName()))
                    .containsExactly(
                            entry("format", "iceberg/" + ORC.name()),
                            entry("provider", "iceberg"),
                            entry("current-snapshot-id", Long.toString(icebergTable.currentSnapshot().snapshotId())),
                            entry("location", icebergTable.location()),
                            entry("format-version", "2"),
                            entry("sort-order", "y ASC NULLS FIRST"),
                            entry("write.orc.compression-codec", "zstd"),
                            entry("commit.retry.num-retries", "4"),
                            entry("write.format.default", ORC.name()),
                            entry("write.parquet.compression-codec", "zstd"));
        }
    }
}
