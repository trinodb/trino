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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.BaseDynamicPartitionPruningTest;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;

import java.util.List;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public class TestHiveDynamicPartitionPruningTest
        extends BaseDynamicPartitionPruningTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                .setExtraProperties(EXTRA_PROPERTIES)
                .setHiveProperties(ImmutableMap.of("hive.dynamic-filtering.wait-timeout", "1h"))
                .setInitialTables(REQUIRED_TABLES)
                .build();
    }

    @Override
    protected void createLineitemTable(String tableName, List<String> columns, List<String> partitionColumns)
    {
        @Language("SQL") String sql = format(
                "CREATE TABLE %s WITH (format = 'TEXTFILE', partitioned_by=array[%s]) AS SELECT %s FROM tpch.tiny.lineitem",
                tableName,
                partitionColumns.stream().map(column -> "'" + column + "'").collect(joining(",")),
                String.join(",", columns));
        getQueryRunner().execute(sql);
    }

    @Override
    protected void createPartitionedTable(String tableName, List<String> columns, List<String> partitionColumns)
    {
        @Language("SQL") String sql = format(
                "CREATE TABLE %s (%s) WITH (partitioned_by=array[%s])",
                tableName,
                String.join(",", columns),
                partitionColumns.stream().map(column -> "'" + column + "'").collect(joining(",")));
        getQueryRunner().execute(sql);
    }

    @Override
    protected void createPartitionedAndBucketedTable(String tableName, List<String> columns, List<String> partitionColumns, List<String> bucketColumns)
    {
        @Language("SQL") String sql = format(
                "CREATE TABLE %s (%s) WITH (partitioned_by=array[%s], bucketed_by=array[%s], bucket_count=10)",
                tableName,
                String.join(",", columns),
                partitionColumns.stream().map(column -> "'" + column + "'").collect(joining(",")),
                bucketColumns.stream().map(column -> "'" + column + "'").collect(joining(",")));
        getQueryRunner().execute(sql);
    }
}
