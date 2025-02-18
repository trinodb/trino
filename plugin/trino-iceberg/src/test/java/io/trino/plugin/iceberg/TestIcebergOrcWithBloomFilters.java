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

import io.trino.testing.BaseOrcWithBloomFiltersTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergOrcWithBloomFilters
        extends BaseOrcWithBloomFiltersTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .addIcebergProperty("iceberg.file-format", "ORC")
                .addIcebergProperty("hive.orc.bloom-filters.enabled", "true")
                .addIcebergProperty("hive.orc.default-bloom-filter-fpp", "0.001")
                .build();
    }

    @Override
    protected String getTableProperties(String bloomFilterColumnName, String bucketingColumnName)
    {
        return format(
                "format = 'ORC', orc_bloom_filter_columns = ARRAY['%s'], partitioning = ARRAY['bucket(%s, 1)']",
                bloomFilterColumnName,
                bucketingColumnName);
    }

    @Test
    public void testBloomFilterPropertiesArePersistedDuringCreate()
    {
        String tableName = "test_metadata_write_properties_" + randomNameSuffix();
        assertQuerySucceeds("CREATE TABLE " + tableName + " (a bigint, b bigint, c bigint) WITH (" +
                "format = 'orc'," +
                "orc_bloom_filter_columns = array['a','b']," +
                "orc_bloom_filter_fpp = 0.1)");

        assertThat(getTableProperties(tableName))
                .containsEntry("write.orc.bloom.filter.columns", "a,b")
                .containsEntry("write.orc.bloom.filter.fpp", "0.1");

        assertThat((String) computeScalar("SHOW CREATE TABLE " + tableName))
                .contains("orc_bloom_filter_columns", "orc_bloom_filter_fpp");
    }

    @Test
    void testBloomFilterPropertiesArePersistedDuringSetProperties()
    {
        String tableName = "test_metadata_write_properties_" + randomNameSuffix();
        assertQuerySucceeds("CREATE TABLE " + tableName + "(A bigint, b bigint, c bigint)");

        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES orc_bloom_filter_columns = ARRAY['a','B']");
        assertThat(getTableProperties(tableName))
                .containsEntry("write.orc.bloom.filter.columns", "a,b");

        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES orc_bloom_filter_columns = ARRAY['a']");
        assertThat(getTableProperties(tableName))
                .containsEntry("write.orc.bloom.filter.columns", "a");

        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES orc_bloom_filter_columns = ARRAY[]");
        assertThat(getTableProperties(tableName))
                .doesNotContainKey("write.orc.bloom.filter.columns");
    }

    @Test
    void testInvalidBloomFilterProperties()
    {
        String tableName = "test_invalid_bloom_filter_properties_" + randomNameSuffix();
        assertQueryFails(
                "CREATE TABLE " + tableName + "(x int) WITH (orc_bloom_filter_columns = ARRAY['missing_column'])",
                "\\QOrc bloom filter columns [missing_column] not present in schema");

        assertQuerySucceeds("CREATE TABLE " + tableName + "(x array(integer))");
        assertQueryFails(
                "ALTER TABLE " + tableName + " SET PROPERTIES orc_bloom_filter_columns = ARRAY['missing_column']",
                "\\QOrc bloom filter columns [missing_column] not present in schema");
    }

    @Test
    void testInvalidOrcBloomFilterPropertiesOnParquet()
    {
        try (TestTable table = newTrinoTable("test_orc_bloom_filter", "(x int) WITH (format = 'PARQUET')")) {
            assertQueryFails(
                    "ALTER TABLE " + table.getName() + " SET PROPERTIES orc_bloom_filter_columns = ARRAY['x']",
                    "Cannot specify orc_bloom_filter_columns table property for storage format: PARQUET");
        }
    }

    private Map<String, String> getTableProperties(String tableName)
    {
        return computeActual("SELECT key, value FROM \"" + tableName + "$properties\"").getMaterializedRows().stream()
                .collect(toImmutableMap(row -> (String) row.getField(0), row -> (String) row.getField(1)));
    }
}
