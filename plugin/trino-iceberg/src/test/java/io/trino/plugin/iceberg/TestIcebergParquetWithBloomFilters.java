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

import com.google.common.base.Joiner;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.BaseTestParquetWithBloomFilters;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.QueryAssertions.assertContains;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergParquetWithBloomFilters
        extends BaseTestParquetWithBloomFilters
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder().build();
    }

    @Override
    protected CatalogSchemaTableName createParquetTableWithBloomFilter(String columnName, List<Integer> testValues)
    {
        // create the managed table
        String tableName = "parquet_with_bloom_filters_" + randomNameSuffix();
        CatalogSchemaTableName catalogSchemaTableName = new CatalogSchemaTableName("iceberg", new SchemaTableName("tpch", tableName));
        assertUpdate(format("CREATE TABLE %s WITH (format = 'PARQUET', parquet_bloom_filter_columns = ARRAY['%s']) AS SELECT * FROM (VALUES %s) t(%s)", catalogSchemaTableName, columnName, Joiner.on(", ").join(testValues), columnName), testValues.size());

        return catalogSchemaTableName;
    }

    @Test
    public void testBloomFilterPropertiesArePersistedDuringCreate()
    {
        String tableName = "test_metadata_write_properties_" + randomNameSuffix();
        assertQuerySucceeds("CREATE TABLE " + tableName + " (A bigint, b bigint, c bigint) WITH (" +
                "format = 'parquet'," +
                "parquet_bloom_filter_columns = array['a','B'])");

        verifyTableProperties(tableName);
    }

    @Test
    void testBloomFilterPropertiesArePersistedDuringSetProperties()
    {
        String tableName = "test_metadata_write_properties_" + randomNameSuffix();
        assertQuerySucceeds("CREATE TABLE " + tableName + "(A bigint, b bigint, c bigint)");

        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES parquet_bloom_filter_columns = ARRAY['a','B']");
        verifyTableProperties(tableName);

        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES parquet_bloom_filter_columns = ARRAY['a']");
        assertThat((String) computeScalar("SHOW CREATE TABLE " + tableName))
                .contains("parquet_bloom_filter_columns = ARRAY['a']");

        assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES parquet_bloom_filter_columns = ARRAY[]");
        assertThat((String) computeScalar("SHOW CREATE TABLE " + tableName))
                .doesNotContain("parquet_bloom_filter_columns");
    }

    @Test
    void testInvalidBloomFilterProperties()
    {
        String tableName = "test_invalid_bloom_filter_properties_" + randomNameSuffix();
        assertQueryFails(
                "CREATE TABLE " + tableName + "(x int) WITH (parquet_bloom_filter_columns = ARRAY['missing_column'])",
                "Parquet Bloom filter column missing_column not present in schema");
        assertQueryFails(
                "CREATE TABLE " + tableName + "(x array(int)) WITH (parquet_bloom_filter_columns = ARRAY['x'])",
                "\\QParquet Bloom filter column x has unsupported type array(integer)");

        assertQuerySucceeds("CREATE TABLE " + tableName + "(x array(integer))");
        assertQueryFails(
                "ALTER TABLE " + tableName + " SET PROPERTIES parquet_bloom_filter_columns = ARRAY['missing_column']",
                "Parquet Bloom filter column missing_column not present in schema");
        assertQueryFails(
                "ALTER TABLE " + tableName + " SET PROPERTIES parquet_bloom_filter_columns = ARRAY['x']",
                "\\QParquet Bloom filter column x has unsupported type array(integer)");
    }

    private void verifyTableProperties(String tableName)
    {
        MaterializedResult actualProperties = computeActual("SELECT * FROM \"" + tableName + "$properties\"");
        assertThat(actualProperties).isNotNull();
        MaterializedResult expectedProperties = resultBuilder(getSession())
                .row("write.parquet.bloom-filter-enabled.column.a", "true")
                .row("write.parquet.bloom-filter-enabled.column.b", "true")
                .build();
        assertContains(actualProperties, expectedProperties);

        assertThat((String) computeScalar("SHOW CREATE TABLE " + tableName))
                .contains("parquet_bloom_filter_columns");
    }
}
