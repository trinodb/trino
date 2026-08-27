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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TestIcebergV3ParquetConnectorTest
        extends BaseIcebergParquetConnectorTest
{
    TestIcebergV3ParquetConnectorTest()
    {
        super(3);
    }

    @Test
    void testNullNestedRowWithVariantRoundTrip()
    {
        try (TestTable table = newTrinoTable(
                "test_null_nested_row_with_variant_",
                "AS SELECT CAST(NULL AS ROW(b ROW(c VARCHAR, d VARIANT))) AS a")) {
            assertThat(query("SELECT a IS NULL FROM " + table.getName()))
                    .matches("VALUES true");
        }
    }

    @Test
    void testNullMapWithVariantValueRoundTrip()
    {
        try (TestTable table = newTrinoTable(
                "test_null_map_with_variant_value_",
                "AS SELECT CAST(NULL AS MAP(VARCHAR, VARIANT)) AS a")) {
            assertThat(query("SELECT a IS NULL FROM " + table.getName()))
                    .matches("VALUES true");
        }
    }
}
