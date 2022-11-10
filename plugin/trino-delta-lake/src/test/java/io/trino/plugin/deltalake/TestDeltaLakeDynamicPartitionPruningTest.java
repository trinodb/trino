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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.BaseDynamicPartitionPruningTest;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.testng.SkipException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.List;

import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.createDeltaLakeQueryRunner;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public class TestDeltaLakeDynamicPartitionPruningTest
        extends BaseDynamicPartitionPruningTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = createDeltaLakeQueryRunner(DELTA_CATALOG, EXTRA_PROPERTIES, ImmutableMap.of(
                "delta.dynamic-filtering.wait-timeout", "1h",
                "delta.enable-non-concurrent-writes", "true"));
        for (TpchTable<?> table : REQUIRED_TABLES) {
            queryRunner.execute(format("CREATE TABLE %1$s.tpch.%2$s AS SELECT * FROM tpch.tiny.%2$s", DELTA_CATALOG, table.getTableName()));
        }
        return queryRunner;
    }

    @Override
    public void testJoinDynamicFilteringMultiJoinOnBucketedTables()
    {
        throw new SkipException("Delta Lake does not support bucketing");
    }

    @Override
    protected void createLineitemTable(String tableName, List<String> columns, List<String> partitionColumns)
    {
        String sql = format(
                "CREATE TABLE %s WITH (partitioned_by=ARRAY[%s]) AS SELECT %s FROM tpch.tiny.lineitem",
                tableName,
                partitionColumns.stream().map(column -> "'" + column + "'").collect(joining(",")),
                String.join(",", columns));
        getQueryRunner().execute(sql);
    }

    @Override
    protected void createPartitionedTable(String tableName, List<String> columns, List<String> partitionColumns)
    {
        String sql = format(
                "CREATE TABLE %s (%s) WITH (location='%s', partitioned_by=ARRAY[%s])",
                tableName,
                String.join(",", columns),
                createTableLocation(tableName),
                partitionColumns.stream().map(column -> "'" + column + "'").collect(joining(",")));
        getQueryRunner().execute(sql);
    }

    @Override
    protected void createPartitionedAndBucketedTable(String tableName, List<String> columns, List<String> partitionColumns, List<String> bucketColumns)
    {
        throw new UnsupportedOperationException();
    }

    private static URI createTableLocation(String tableName)
    {
        try {
            return Files.createTempDirectory(tableName).toFile().toURI();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
