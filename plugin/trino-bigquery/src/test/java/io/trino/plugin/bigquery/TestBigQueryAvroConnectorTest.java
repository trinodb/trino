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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;

import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBigQueryAvroConnectorTest
        extends BaseBigQueryConnectorTest
{
    private static final Set<String> UNSUPPORTED_COLUMN_NAMES = ImmutableSet.<String>builder()
            .add("a-hyphen-minus")
            .add("a space")
            .add("atrailingspace ")
            .add(" aleadingspace")
            .add("a:colon")
            .add("an'apostrophe")
            .add("0startwithdigit")
            .add("カラム")
            .build();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return BigQueryQueryRunner.builder()
                .setConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("bigquery.arrow-serialization.enabled", "false")
                        .put("bigquery.job.label-name", "trino_query")
                        .put("bigquery.job.label-format", "q_$QUERY_ID__t_$TRACE_TOKEN")
                        .buildOrThrow())
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    protected Optional<String> filterColumnNameTestData(String columnName)
    {
        if (UNSUPPORTED_COLUMN_NAMES.contains(columnName)) {
            return Optional.empty();
        }
        return Optional.of(columnName);
    }

    // TODO: Disable all operations for unsupported column names
    @Test
    public void testSelectFailsForColumnName()
    {
        for (String columnName : UNSUPPORTED_COLUMN_NAMES) {
            String tableName = "test.test_unsupported_column_name" + randomNameSuffix();

            assertUpdate("CREATE TABLE " + tableName + "(\"" + columnName + "\" varchar(50))");
            try {
                assertUpdate("INSERT INTO " + tableName + " VALUES ('test value')", 1);
                // The storage API can't read the table, but query based API can read it
                assertThat(query("SELECT * FROM " + tableName))
                        .failure().hasMessageMatching("(Cannot create read|Invalid Avro schema).*(Illegal initial character|Invalid name).*");
                assertThat(bigQuerySqlExecutor.executeQuery("SELECT * FROM " + tableName).getValues())
                        .extracting(field -> field.get(0).getStringValue())
                        .containsExactly("test value");
            }
            finally {
                assertUpdate("DROP TABLE " + tableName);
            }
        }
    }

    @Override
    @Test
    public void testProjectionPushdown()
    {
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_projection_pushdown_",
                "(id BIGINT, root ROW(f1 BIGINT, f2 BIGINT))",
                ImmutableList.of("(1, ROW(1, 2))", "(2, NULl)", "(3, ROW(NULL, 4))"))) {
            String selectQuery = "SELECT id, root.f1 FROM " + testTable.getName();
            String expectedResult = "VALUES (BIGINT '1', BIGINT '1'), (BIGINT '2', NULL), (BIGINT '3', NULL)";

            // With Projection Pushdown enabled
            assertThat(query(selectQuery))
                    .matches(expectedResult)
                    .isFullyPushedDown();

            // With Projection Pushdown disabled
            Session sessionWithoutPushdown = sessionWithProjectionPushdownDisabled(getSession());
            assertThat(query(sessionWithoutPushdown, selectQuery))
                    .matches(expectedResult)
                    .isNotFullyPushedDown(ProjectNode.class);
        }
    }

    @Override
    @Test
    public void testProjectionWithCaseSensitiveField()
    {
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_projection_with_case_sensitive_field_",
                "(id INT, a ROW(\"UPPER_CASE\" INT, \"lower_case\" INT, \"MiXeD_cAsE\" INT))",
                ImmutableList.of("(1, ROW(2, 3, 4))", "(5, ROW(6, 7, 8))"))) {
            // shippriority column is bigint (not integer) in BigQuery connector
            String expected = "VALUES (BIGINT '2', BIGINT '3', BIGINT '4'), (BIGINT '6', BIGINT '7', BIGINT '8')";
            assertThat(query("SELECT \"a\".\"UPPER_CASE\", \"a\".\"lower_case\", \"a\".\"MiXeD_cAsE\" FROM " + testTable.getName()))
                    .matches(expected)
                    .isFullyPushedDown();
            assertThat(query("SELECT \"a\".\"upper_case\", \"a\".\"lower_case\", \"a\".\"mixed_case\" FROM " + testTable.getName()))
                    .matches(expected)
                    .isFullyPushedDown();
            assertThat(query("SELECT \"a\".\"UPPER_CASE\", \"a\".\"LOWER_CASE\", \"a\".\"MIXED_CASE\" FROM " + testTable.getName()))
                    .matches(expected)
                    .isFullyPushedDown();
        }
    }

    @Override
    @Test
    public void testProjectionPushdownMultipleRows()
    {
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_projection_pushdown_multiple_rows_",
                "(id INT, nested1 ROW(child1 INT, child2 VARCHAR, child3 INT), nested2 ROW(child1 DOUBLE, child2 BOOLEAN, child3 DATE))",
                ImmutableList.of(
                        "(1, ROW(10, 'a', 100), ROW(10.10, true, DATE '2023-04-19'))",
                        "(2, ROW(20, 'b', 200), ROW(20.20, false, DATE '1990-04-20'))",
                        "(4, ROW(40, NULL, 400), NULL)",
                        "(5, NULL, ROW(NULL, true, NULL))"))) {
            // Select one field from one row field
            assertThat(query("SELECT id, nested1.child1 FROM " + testTable.getName()))
                    .matches("VALUES (BIGINT '1', BIGINT '10'), (BIGINT '2', BIGINT '20'), (BIGINT '4', BIGINT '40'), (BIGINT '5', NULL)")
                    .isFullyPushedDown();
            assertThat(query("SELECT nested2.child3, id FROM " + testTable.getName()))
                    .matches("VALUES (DATE '2023-04-19', BIGINT '1'), (DATE '1990-04-20', BIGINT '2'), (NULL, BIGINT '4'), (NULL, BIGINT '5')")
                    .isFullyPushedDown();

            // Select one field each from multiple row fields
            assertThat(query("SELECT nested2.child1, id, nested1.child2 FROM " + testTable.getName()))
                    .skippingTypesCheck()
                    .matches("VALUES (DOUBLE '10.10', BIGINT '1', 'a'), (DOUBLE '20.20', BIGINT '2', 'b'), (NULL, BIGINT '4', NULL), (NULL, BIGINT '5', NULL)")
                    .isFullyPushedDown();

            // Select multiple fields from one row field
            assertThat(query("SELECT nested1.child3, id, nested1.child2 FROM " + testTable.getName()))
                    .skippingTypesCheck()
                    .matches("VALUES (BIGINT '100', BIGINT '1', 'a'), (BIGINT '200', BIGINT '2', 'b'), (BIGINT '400', BIGINT '4', NULL), (NULL, BIGINT '5', NULL)")
                    .isFullyPushedDown();
            assertThat(query("SELECT nested2.child2, nested2.child3, id FROM " + testTable.getName()))
                    .matches("VALUES (true, DATE '2023-04-19' , BIGINT '1'), (false, DATE '1990-04-20', BIGINT '2'), (NULL, NULL, BIGINT '4'), (true, NULL, BIGINT '5')")
                    .isFullyPushedDown();

            // Select multiple fields from multiple row fields
            assertThat(query("SELECT id, nested2.child1, nested1.child3, nested2.child2, nested1.child1 FROM " + testTable.getName()))
                    .matches("VALUES (BIGINT '1', DOUBLE '10.10', BIGINT '100', true, BIGINT '10'), (BIGINT '2', DOUBLE '20.20', BIGINT '200', false, BIGINT '20'), (BIGINT '4', NULL, BIGINT '400', NULL, BIGINT '40'), (BIGINT '5', NULL, NULL, true, NULL)")
                    .isFullyPushedDown();

            // Select only nested fields
            assertThat(query("SELECT nested2.child2, nested1.child3 FROM " + testTable.getName()))
                    .matches("VALUES (true, BIGINT '100'), (false, BIGINT '200'), (NULL, BIGINT '400'), (true, NULL)")
                    .isFullyPushedDown();
        }
    }

    @Override
    @Test
    public void testProjectionPushdownReadsLessData()
    {
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_projection_pushdown_reads_less_data_",
                "AS SELECT val AS id, CAST(ROW(val + 1, val + 2) AS ROW(leaf1 BIGINT, leaf2 BIGINT)) AS root FROM UNNEST(SEQUENCE(1, 10)) AS t(val)")) {
            MaterializedResult expectedResult = computeActual("SELECT val + 2 FROM UNNEST(SEQUENCE(1, 10)) AS t(val)");
            String selectQuery = "SELECT root.leaf2 FROM " + testTable.getName();
            Session sessionWithoutPushdown = sessionWithProjectionPushdownDisabled(getSession());

            assertQueryStats(
                    getSession(),
                    selectQuery,
                    statsWithPushdown -> {
                        DataSize physicalInputDataSizeWithPushdown = statsWithPushdown.getPhysicalInputDataSize();
                        DataSize processedDataSizeWithPushdown = statsWithPushdown.getProcessedInputDataSize();
                        assertQueryStats(
                                sessionWithoutPushdown,
                                selectQuery,
                                statsWithoutPushdown -> {
                                    if (supportsPhysicalPushdown()) {
                                        assertThat(statsWithoutPushdown.getPhysicalInputDataSize()).isGreaterThan(physicalInputDataSizeWithPushdown);
                                    }
                                    else {
                                        // TODO https://github.com/trinodb/trino/issues/17201
                                        assertThat(statsWithoutPushdown.getPhysicalInputDataSize()).isEqualTo(physicalInputDataSizeWithPushdown);
                                    }
                                    assertThat(statsWithoutPushdown.getProcessedInputDataSize()).isGreaterThan(processedDataSizeWithPushdown);
                                },
                                results -> assertThat(results.getOnlyColumnAsSet()).isEqualTo(expectedResult.getOnlyColumnAsSet()));
                    },
                    results -> assertThat(results.getOnlyColumnAsSet()).isEqualTo(expectedResult.getOnlyColumnAsSet()));
        }
    }

    @Override
    @Test
    public void testProjectionPushdownPhysicalInputSize()
    {
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_projection_pushdown_physical_input_size_",
                "AS SELECT val AS id, CAST(ROW(val + 1, val + 2) AS ROW(leaf1 BIGINT, leaf2 BIGINT)) AS root FROM UNNEST(SEQUENCE(1, 10)) AS t(val)")) {
            // Verify that the physical input size is smaller when reading the root.leaf1 field compared to reading the root field
            assertQueryStats(
                    getSession(),
                    "SELECT root FROM " + testTable.getName(),
                    statsWithSelectRootField -> {
                        assertQueryStats(
                                getSession(),
                                "SELECT root.leaf1 FROM " + testTable.getName(),
                                statsWithSelectLeafField -> {
                                    if (supportsPhysicalPushdown()) {
                                        assertThat(statsWithSelectLeafField.getPhysicalInputDataSize()).isLessThan(statsWithSelectRootField.getPhysicalInputDataSize());
                                    }
                                    else {
                                        // TODO https://github.com/trinodb/trino/issues/17201
                                        assertThat(statsWithSelectLeafField.getPhysicalInputDataSize()).isEqualTo(statsWithSelectRootField.getPhysicalInputDataSize());
                                    }
                                },
                                results -> assertThat(results.getOnlyColumnAsSet()).isEqualTo(computeActual("SELECT val + 1 FROM UNNEST(SEQUENCE(1, 10)) AS t(val)").getOnlyColumnAsSet()));
                    },
                    results -> assertThat(results.getOnlyColumnAsSet()).isEqualTo(computeActual("SELECT ROW(val + 1, val + 2) FROM UNNEST(SEQUENCE(1, 10)) AS t(val)").getOnlyColumnAsSet()));

            // Verify that the physical input size is the same when reading the root field compared to reading both the root and root.leaf1 fields
            assertQueryStats(
                    getSession(),
                    "SELECT root FROM " + testTable.getName(),
                    statsWithSelectRootField -> {
                        assertQueryStats(
                                getSession(),
                                "SELECT root, root.leaf1 FROM " + testTable.getName(),
                                statsWithSelectRootAndLeafField -> {
                                    assertThat(statsWithSelectRootAndLeafField.getPhysicalInputDataSize()).isEqualTo(statsWithSelectRootField.getPhysicalInputDataSize());
                                },
                                results -> assertEqualsIgnoreOrder(results.getMaterializedRows(), computeActual("SELECT ROW(val + 1, val + 2), val + 1 FROM UNNEST(SEQUENCE(1, 10)) AS t(val)").getMaterializedRows()));
                    },
                    results -> assertThat(results.getOnlyColumnAsSet()).isEqualTo(computeActual("SELECT ROW(val + 1, val + 2) FROM UNNEST(SEQUENCE(1, 10)) AS t(val)").getOnlyColumnAsSet()));
        }
    }
}
