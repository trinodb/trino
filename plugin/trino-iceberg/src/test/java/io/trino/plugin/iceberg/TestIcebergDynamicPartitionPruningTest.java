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

import com.google.common.collect.ImmutableMap;
import io.trino.FeaturesConfig.JoinDistributionType;
import io.trino.testing.BaseDynamicPartitionPruningTest;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.SkipException;

import java.util.List;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public class TestIcebergDynamicPartitionPruningTest
        extends BaseDynamicPartitionPruningTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.createIcebergQueryRunner(
                EXTRA_PROPERTIES,
                ImmutableMap.of("iceberg.dynamic-filtering.wait-timeout", "1h"),
                REQUIRED_TABLES);
    }

    @Override
    public void testJoinDynamicFilteringMultiJoinOnBucketedTables(JoinDistributionType joinDistributionType)
    {
        throw new SkipException("Iceberg does not support bucketing");
    }

    @Override
    protected void createLineitemTable(String tableName, List<String> columns, List<String> partitionColumns)
    {
        @Language("SQL") String sql = format(
                "CREATE TABLE %s WITH (partitioning=array[%s]) AS SELECT %s FROM tpch.tiny.lineitem",
                tableName,
                partitionColumns.stream().map(column -> "'" + column + "'").collect(joining(",")),
                String.join(",", columns));
        getQueryRunner().execute(sql);
    }

    @Override
    protected void createPartitionedTable(String tableName, List<String> columns, List<String> partitionColumns)
    {
        @Language("SQL") String sql = format(
                "CREATE TABLE %s (%s) WITH (partitioning=array[%s])",
                tableName,
                String.join(",", columns),
                partitionColumns.stream().map(column -> "'" + column + "'").collect(joining(",")));
        getQueryRunner().execute(sql);
    }

    @Override
    protected void createPartitionedAndBucketedTable(String tableName, List<String> columns, List<String> partitionColumns, List<String> bucketColumns)
    {
        throw new UnsupportedOperationException();
    }
}
