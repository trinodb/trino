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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.operator.TableScanOperator;
import io.trino.testing.BaseCacheSubqueriesTest;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import io.trino.testing.sql.TestTable;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.createDeltaLakeQueryRunner;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static java.lang.String.format;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestDeltaLakeCacheSubqueriesTest
        extends BaseCacheSubqueriesTest
{
    private Path deletionVectorTablePath;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = createDeltaLakeQueryRunner(DELTA_CATALOG, EXTRA_PROPERTIES,
                ImmutableMap.of("delta.register-table-procedure.enabled", "true",
                        "delta.enable-non-concurrent-writes", "true"));
        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, queryRunner.getDefaultSession(), REQUIRED_TABLES);
        return queryRunner;
    }

    @BeforeAll
    public void registerTables()
            throws IOException
    {
        String deletionVectors = "deletion_vectors";
        Path tempDirectory = Files.createTempDirectory(deletionVectors);
        deletionVectorTablePath = tempDirectory.resolve("deltalake/cache/table_with_deletion_vector");
        copyResources(Path.of(getClass().getResource("/deltalake/cache/table_with_deletion_vector").getPath()), tempDirectory);
        getQueryRunner().execute(format("CALL system.register_table('%s', '%s', '%s')", getSession().getSchema().orElseThrow(), deletionVectors, deletionVectorTablePath.toString()));
    }

    @Test
    public void testDoNotUseCacheAfterInsert()
    {
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_do_not_use_cache",
                "(name VARCHAR)",
                ImmutableList.of("'value1'", "'value2'"))) {
            @Language("SQL") String selectQuery = "select name from %s union all select name from %s".formatted(testTable.getName(), testTable.getName());
            MaterializedResultWithPlan result = executeWithPlan(withCacheEnabled(), selectQuery);
            assertEqualsIgnoreOrder(result.result().getMaterializedRows().stream().map(row -> row.getField(0)).toList(), ImmutableList.of("value1", "value2", "value1", "value2"));
            assertThat(getOperatorInputPositions(result.queryId(), TableScanOperator.class.getSimpleName())).isPositive();

            assertUpdate("insert into %s(name) values ('value3')".formatted(testTable.getName()), 1);
            result = executeWithPlan(withCacheEnabled(), selectQuery);

            assertThat(result.result().getRowCount()).isEqualTo(6);
            assertThat(getScanOperatorInputPositions(result.queryId())).isPositive();
            assertThat(getLoadCachedDataOperatorInputPositions(result.queryId())).isPositive();

            result = executeWithPlan(withCacheEnabled(), selectQuery);
            assertThat(getLoadCachedDataOperatorInputPositions(result.queryId())).isEqualTo(6);
            assertThat(getScanOperatorInputPositions(result.queryId())).isZero();
        }
    }

    @Test
    public void testCacheWhenSchemaEvolved()
    {
        computeActual("create table orders2 with (column_mapping_mode='name') as select orderkey, orderdate, orderpriority from orders limit 100");
        @Language("SQL") String query = "select * from orders2 union all select * from orders2";

        MaterializedResultWithPlan result = executeWithPlan(withCacheEnabled(), query);
        assertThat(result.result().getMaterializedRows().get(0).getFieldCount()).isEqualTo(3);
        assertThat(getCacheDataOperatorInputPositions(result.queryId())).isPositive();

        // add a nullable column - schema will be evolved
        assertUpdate("alter table orders2 add column c varchar");

        // should not use cache because of schema was evolved
        result = executeWithPlan(withCacheEnabled(), query);
        assertThat(result.result().getMaterializedRows().get(0).getFieldCount()).isEqualTo(4);
        assertThat(result.result().getMaterializedRows().stream().map(row -> row.getField(3))).allMatch(Objects::isNull);

        // drop a column - schema will be evolved
        assertUpdate("alter table orders2 drop column c");

        // should not use cache because of schema was evolved
        result = executeWithPlan(withCacheEnabled(), query);
        assertThat(result.result().getMaterializedRows().get(0).getFieldCount()).isEqualTo(3);

        assertUpdate("drop table orders2");
    }

    @Test
    public void testDeletionVectors()
            throws IOException
    {
        @Language("SQL") String query = "select * from (select * from deletion_vectors union all select * from deletion_vectors) order by 1";
        MaterializedResultWithPlan result = executeWithPlan(withCacheEnabled(), query);
        assertThat(result.result().getRowCount()).isEqualTo(4);
        assertThat(getCacheDataOperatorInputPositions(result.queryId())).isPositive();

        // should use only cache
        result = executeWithPlan(withCacheEnabled(), query);
        assertThat(result.result().getRowCount()).isEqualTo(4);
        assertThat(getScanOperatorInputPositions(result.queryId())).isZero();
        assertThat(getLoadCachedDataOperatorInputPositions(result.queryId())).isPositive();

        // simulate that row was deleted with deletion vector
        Files.copy(
                Path.of(getClass().getResource("/deltalake/cache/00000000000000000002.json").getPath()),
                Path.of(deletionVectorTablePath.toString(), "_delta_log", "00000000000000000002.json"),
                REPLACE_EXISTING);

        // cache should not be used because of deletion vector presence
        result = executeWithPlan(withCacheEnabled(), query);
        assertThat(result.result().getRowCount()).isEqualTo(2);
        assertThat(result.result().getMaterializedRows()).map(row -> row.getField(0)).matches(values -> !values.contains(2));
        assertThat(getScanOperatorInputPositions(result.queryId())).isPositive();
    }

    @Override
    protected void createPartitionedTableAsSelect(String tableName, List<String> partitionColumns, String asSelect)
    {
        @Language("SQL") String sql = format(
                "CREATE TABLE %s WITH (partitioned_by=array[%s]) as %s",
                tableName,
                partitionColumns.stream().map(column -> "'" + column + "'").collect(joining(",")),
                asSelect);

        getQueryRunner().execute(sql);
    }

    @Override
    protected Session withProjectionPushdownEnabled(Session session, boolean projectionPushdownEnabled)
    {
        return Session.builder(session)
                .setSystemProperty("delta.projection_pushdown_enabled", String.valueOf(projectionPushdownEnabled))
                .build();
    }

    private void copyResources(Path resourceDirectory, Path destinationDirectory)
            throws IOException
    {
        try (Stream<Path> files = Files.walk(resourceDirectory)) {
            files.forEach(input -> {
                try {
                    Path destination = destinationDirectory.resolve(input.subpath(resourceDirectory.getNameCount() - 3, input.getNameCount()).toString());
                    if (Files.isDirectory(input)) {
                        Files.createDirectories(destination);
                    }
                    else {
                        Files.copy(input, destination);
                    }
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }
}
