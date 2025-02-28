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

import io.trino.Session;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TrinoSqlExecutor;
import org.apache.parquet.format.CompressionCodec;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.iceberg.IcebergFileFormat.PARQUET;
import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergParquetSystemTables
        extends BaseIcebergSystemTables
{
    public TestIcebergParquetSystemTables()
    {
        super(PARQUET);
    }

    @Test
    public void testPropertiesTable()
    {
        Session snappySession = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "compression_codec", CompressionCodec.SNAPPY.name())
                .build();
        try (TestTable defaultTable = newTrinoTable("test_properties_table", "AS SELECT 1 x");
                TestTable snappyTable = new TestTable(new TrinoSqlExecutor(getQueryRunner(), snappySession), "test_table_codec_", "AS SELECT 1 x")) {
            assertThat(getTableProperties(defaultTable.getName()))
                    .contains(
                            entry("write.format.default", "PARQUET"),
                            entry("write.parquet.compression-codec", "zstd"));
            assertThat(getTableProperties(snappyTable.getName()))
                    .contains(
                            entry("write.format.default", "PARQUET"),
                            entry("write.parquet.compression-codec", "snappy"));
        }
    }
}
