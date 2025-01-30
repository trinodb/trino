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
import org.apache.parquet.format.CompressionCodec;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.iceberg.IcebergFileFormat.PARQUET;
import static io.trino.testing.TestingNames.randomNameSuffix;
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
        try (TestTable table = newTrinoTable("test_properties_table", "AS SELECT 1 x")) {
            assertThat(query("SELECT * FROM \"%s$properties\"".formatted(table.getName())))
                    .matches("""
                            VALUES (VARCHAR 'write.format.default', VARCHAR 'PARQUET'),
                            (VARCHAR 'commit.retry.num-retries', VARCHAR '4'),
                            (VARCHAR 'write.parquet.compression-codec', VARCHAR 'zstd')""");
        }
        String tableName = "test_table_codec" + randomNameSuffix();
        Session snappySession = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "compression_codec", CompressionCodec.SNAPPY.name())
                .build();
        try {
            assertUpdate(snappySession, "CREATE TABLE test_schema.%s (_varchar VARCHAR, _date DATE) WITH (partitioning = ARRAY['_date'])".formatted(tableName));
            assertThat(query("SELECT * FROM test_schema.\"%s$properties\"".formatted(tableName)))
                    .matches("""
                            VALUES (VARCHAR 'write.format.default', VARCHAR 'PARQUET'),
                            (VARCHAR 'commit.retry.num-retries', VARCHAR '4'),
                            (VARCHAR 'write.parquet.compression-codec', VARCHAR 'snappy')""");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_schema.%s".formatted(tableName));
        }
    }
}
