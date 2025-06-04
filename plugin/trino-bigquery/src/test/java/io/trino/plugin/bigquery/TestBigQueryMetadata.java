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
package io.trino.plugin.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.google.cloud.bigquery.Field.Mode.NULLABLE;
import static com.google.cloud.bigquery.StandardSQLTypeName.BIGNUMERIC;
import static io.trino.plugin.bigquery.BigQueryMetadata.projectParentColumns;
import static io.trino.plugin.bigquery.BigQueryQueryRunner.BigQuerySqlExecutor;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class TestBigQueryMetadata
{
    @Test
    public void testDatasetNotFoundMessage()
    {
        // Update the catch block condition in 'BigQueryMetadata.listTables()' method if this test failed
        BigQuery bigQuery = new BigQuerySqlExecutor().getBigQuery();
        assertThatExceptionOfType(BigQueryException.class)
                .isThrownBy(() -> bigQuery.listTables("test_dataset_not_found"))
                .matches(e -> e.getCode() == 404 && e.getMessage().contains("Not found: Dataset"));
    }

    @Test
    public void testProjectParentColumnsSingleParent()
    {
        BigQueryColumnHandle parentColumn = testingColumn("a", ImmutableList.of());
        List<BigQueryColumnHandle> columns = ImmutableList.of(
                parentColumn,
                testingColumn("a", ImmutableList.of("b")),
                testingColumn("a", ImmutableList.of("b", "c", "d")),
                testingColumn("a", ImmutableList.of("b", "c")));

        List<BigQueryColumnHandle> parentColumns = projectParentColumns(columns);
        assertThat(parentColumns).size().isEqualTo(1);
        assertThat(parentColumns.getFirst()).isEqualTo(parentColumn);
    }

    @Test
    public void testProjectParentColumnsSingleParentDifferentOrder()
    {
        BigQueryColumnHandle parentColumn = testingColumn("a", ImmutableList.of());
        List<BigQueryColumnHandle> columns = ImmutableList.of(
                parentColumn,
                testingColumn("a", ImmutableList.of("b")),
                testingColumn("a", ImmutableList.of("d", "c", "b")),
                testingColumn("a", ImmutableList.of("b", "c", "d")),
                testingColumn("a", ImmutableList.of("b", "c")));

        List<BigQueryColumnHandle> parentColumns = projectParentColumns(columns);
        assertThat(parentColumns).size().isEqualTo(1);
        assertThat(parentColumns.getFirst()).isEqualTo(parentColumn);
    }

    @Test
    public void testProjectParentColumnsNoParentDifferentOrder()
    {
        List<BigQueryColumnHandle> columns = ImmutableList.of(
                testingColumn("a", ImmutableList.of("b", "c", "d")),
                testingColumn("a", ImmutableList.of("d", "c", "b")));

        List<BigQueryColumnHandle> parentColumns = projectParentColumns(columns);
        assertThat(parentColumns).size().isEqualTo(2);
    }

    @Test
    public void testProjectParentColumnsSingleParentSuddenJump()
    {
        BigQueryColumnHandle parentColumn = testingColumn("a", ImmutableList.of());
        List<BigQueryColumnHandle> columns = ImmutableList.of(
                parentColumn,
                testingColumn("a", ImmutableList.of("d", "c", "b")));

        List<BigQueryColumnHandle> parentColumns = projectParentColumns(columns);
        assertThat(parentColumns).size().isEqualTo(1);
        assertThat(parentColumns.getFirst()).isEqualTo(parentColumn);
    }

    @Test
    public void testProjectParentColumnsMultipleParent()
    {
        BigQueryColumnHandle parentColumn = testingColumn("a", ImmutableList.of());
        BigQueryColumnHandle anotherParentColumn = testingColumn("a1", ImmutableList.of());
        List<BigQueryColumnHandle> columns = ImmutableList.of(
                parentColumn,
                anotherParentColumn,
                testingColumn("a", ImmutableList.of("b", "c", "d")),
                testingColumn("a", ImmutableList.of("b", "c")));

        List<BigQueryColumnHandle> parentColumns = projectParentColumns(columns);
        assertThat(parentColumns).size().isEqualTo(2);
        assertThat(parentColumns).containsExactlyInAnyOrder(parentColumn, anotherParentColumn);
    }

    private static BigQueryColumnHandle testingColumn(String name, List<String> dereferenceNames)
    {
        return new BigQueryColumnHandle(
                name,
                dereferenceNames,
                BIGINT,
                BIGNUMERIC,
                false,
                NULLABLE,
                ImmutableList.of(),
                "description",
                false);
    }
}
