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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.trino.Session;
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
