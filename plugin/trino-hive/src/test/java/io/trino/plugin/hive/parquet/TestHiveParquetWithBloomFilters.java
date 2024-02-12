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
package io.trino.plugin.hive.parquet;

import com.google.common.base.Joiner;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.BaseTestParquetWithBloomFilters;
import io.trino.testing.QueryRunner;

import java.util.List;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;

public class TestHiveParquetWithBloomFilters
        extends BaseTestParquetWithBloomFilters
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder().build();
    }

    @Override
    protected CatalogSchemaTableName createParquetTableWithBloomFilter(String columnName, List<Integer> testValues)
    {
        // create the managed table
        String tableName = "parquet_with_bloom_filters_" + randomNameSuffix();
        CatalogSchemaTableName catalogSchemaTableName = new CatalogSchemaTableName("hive", new SchemaTableName("tpch", tableName));
        assertUpdate(format("CREATE TABLE %s WITH (format = 'PARQUET', parquet_bloom_filter_columns = ARRAY['%s']) AS SELECT * FROM (VALUES %s) t(%s)", catalogSchemaTableName, columnName, Joiner.on(", ").join(testValues), columnName), testValues.size());

        return catalogSchemaTableName;
    }
}
