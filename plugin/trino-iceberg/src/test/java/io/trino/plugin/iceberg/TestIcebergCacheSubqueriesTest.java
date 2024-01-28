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

import com.google.common.collect.ImmutableList;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.testing.BaseCacheSubqueriesTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import io.trino.testing.sql.TestTable;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
public class TestIcebergCacheSubqueriesTest
        extends BaseCacheSubqueriesTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setExtraProperties(EXTRA_PROPERTIES)
                .setInitialTables(REQUIRED_TABLES)
                .build();
    }

    @Test
    public void testDoUsePartiallyCachedResultsWhenDataWasDeletedFromUnpartitionedTable()
    {
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "iceberg_do_not_cache",
                "(name VARCHAR)",
                ImmutableList.of("'value1'", "'value2'"))) {
            // multi insert to place 2 values in single split
            assertUpdate("insert into %s(name) values ('value3'), ('value4')".formatted(testTable.getName()), 2);
            @Language("SQL") String selectQuery = "select name from %s union all select name from %s".formatted(testTable.getName(), testTable.getName());
            MaterializedResultWithPlan result = executeWithPlan(withCacheEnabled(), selectQuery);
            assertThat(result.result().getRowCount()).isEqualTo(8);
            assertThat(getScanOperatorInputPositions(result.queryId())).isPositive();

            assertUpdate("delete from %s where name='value3'".formatted(testTable.getName()), 1);
            result = executeWithPlan(withCacheEnabled(), selectQuery);

            assertThat(result.result().getRowCount()).isEqualTo(6);
            assertThat(result.result().getMaterializedRows().stream().noneMatch(row -> row.getField(0).equals("value3"))).isTrue();
            // split with deleted file should trigger table scan
            assertThat(getScanOperatorInputPositions(result.queryId())).isPositive();
            // after deletion cached data is partially reused
            assertThat(getLoadCachedDataOperatorInputPositions(result.queryId())).isPositive();

            result = executeWithPlan(withCacheEnabled(), selectQuery);
            assertThat(getLoadCachedDataOperatorInputPositions(result.queryId())).isEqualTo(6);
            assertThat(getScanOperatorInputPositions(result.queryId())).isZero();
        }
    }

    @Test
    public void testTimeTravelQueryCache()
    {
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "iceberg_timetravel",
                "(year INT, name VARCHAR) with (partitioning = ARRAY['year'])",
                ImmutableList.of("2000, 'value1'", "2001, 'value2'"))) {
            Optional<TableHandle> tableHandler = withTransaction(session -> getDistributedQueryRunner().getCoordinator()
                    .getPlannerContext().getMetadata()
                    .getTableHandle(session, new QualifiedObjectName(ICEBERG_CATALOG, session.getSchema().get(), testTable.getName())));
            IcebergTableHandle icebergTableHandle = (IcebergTableHandle) tableHandler.get().getConnectorHandle();

            @Language("SQL") String selectQuery = """
            select name from %s where year = 2000
            union all
            select name from %s FOR VERSION AS OF %s where year = 2000
            """.formatted(testTable.getName(), testTable.getName(), icebergTableHandle.getSnapshotId().get());

            assertUpdate("insert into %s(year, name) values (2000, 'value3'), (2001, 'value4')".formatted(testTable.getName()), 2);
            MaterializedResultWithPlan result = executeWithPlan(withCacheEnabled(), selectQuery);
            // two rows from current snapshot and one row from previous
            assertThat(result.result().getRowCount()).isEqualTo(3);
            assertThat(getCacheDataOperatorInputPositions(result.queryId())).isEqualTo(2);
            assertThat(getScanOperatorInputPositions(result.queryId())).isPositive();

            assertUpdate("delete from %s where year = 2000".formatted(testTable.getName()), 2);
            result = executeWithPlan(withCacheEnabled(), selectQuery);
            assertThat(result.result().getRowCount()).isEqualTo(1);
            assertThat(getLoadCachedDataOperatorInputPositions(result.queryId())).isEqualTo(1);
        }
    }

    @Test
    public void testChangeWhenSchemaEvolved()
    {
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "iceberg_do_not_cache",
                "(year INT, name VARCHAR) with (partitioning = ARRAY['year'])",
                ImmutableList.of("2000, 'value1'", "2001, 'value2'"))) {
            @Language("SQL") String selectQuery = "select name from %s where year = 2001 and name ='value2' union all select name from %s where year = 2000 and name='value1'".formatted(testTable.getName(), testTable.getName());
            executeWithPlan(withCacheEnabled(), selectQuery);
            assertUpdate("insert into %s(year, name) values (2000, 'value3'), (2001, 'value4')".formatted(testTable.getName()), 2);
            assertUpdate("ALTER TABLE %s SET PROPERTIES partitioning = ARRAY['name']".formatted(testTable.getName()));
            MaterializedResultWithPlan result = executeWithPlan(withCacheEnabled(), "select name from %s where year=2000".formatted(testTable.getName()));
            assertThat(result.result().getRowCount()).isEqualTo(2);
            assertThat(getLoadCachedDataOperatorInputPositions(result.queryId())).isZero();
            assertThat(getScanOperatorInputPositions(result.queryId())).isPositive();

            result = executeWithPlan(withCacheEnabled(), selectQuery);
            assertThat(getLoadCachedDataOperatorInputPositions(result.queryId())).isEqualTo(2);
            assertThat(getScanOperatorInputPositions(result.queryId())).isEqualTo(0);
        }
    }

    @Override
    protected void createPartitionedTableAsSelect(String tableName, List<String> partitionColumns, String asSelect)
    {

        @Language("SQL") String sql = format(
                "CREATE TABLE %s WITH (partitioning=array[%s]) as %s",
                tableName,
                partitionColumns.stream().map(column -> "'" + column + "'").collect(joining(",")),
                asSelect);

        getQueryRunner().execute(sql);
    }
}
