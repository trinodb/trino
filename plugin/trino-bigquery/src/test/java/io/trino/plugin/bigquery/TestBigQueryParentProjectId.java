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
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tpch.TpchTable.NATION;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

class TestBigQueryParentProjectId
        extends BaseBigQueryProjectIdResolution
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        queryRunner = BigQueryQueryRunner.builder()
                .setConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("bigquery.parent-project-id", parentProjectId)
                        .buildOrThrow())
                .setInitialTables(ImmutableList.of(NATION))
                .build();
        return queryRunner;
    }

    @Test
    void testQueriesWithParentProjectId()
            throws Exception
    {
        // tpch schema is available in both projects
        assertThat(computeScalar("SELECT name FROM bigquery.tpch.nation WHERE nationkey = 0")).isEqualTo("ALGERIA");
        assertThat(computeScalar("SELECT * FROM TABLE(bigquery.system.query(query => 'SELECT name FROM tpch.nation WHERE nationkey = 0'))")).isEqualTo("ALGERIA");
        assertThat(computeScalar(format("SELECT * FROM TABLE(bigquery.system.query(query => 'SELECT name FROM %s.tpch.nation WHERE nationkey = 0'))", projectId)))
                .isEqualTo("ALGERIA");
        assertThat(computeScalar(format("SELECT * FROM TABLE(bigquery.system.query(query => 'SELECT name FROM %s.tpch.nation WHERE nationkey = 0'))", parentProjectId)))
                .isEqualTo("ALGERIA");

        String trinoSchema = "someschema_" + randomNameSuffix();
        try (AutoCloseable ignored = withSchema(trinoSchema); TestTable table = newTrinoTable("%s.table".formatted(trinoSchema), "(col1 INT)")) {
            String tableName = table.getName().split("\\.")[1];
            // schema created in parentProjectId by default
            assertThat(computeActual(format(
                    "SELECT * FROM TABLE(bigquery.system.query(query => 'SELECT schema_name FROM `%s.region-us.INFORMATION_SCHEMA.SCHEMATA`'))",
                    projectId)))
                    .doesNotContain(row(trinoSchema));
            // confusion point: this implicitly points to project
            assertThat(computeActual(format("SELECT * FROM TABLE(bigquery.system.query(query => 'SELECT schema_name FROM INFORMATION_SCHEMA.SCHEMATA'))")))
                    .contains(row(trinoSchema));
            assertThat(computeActual(format(
                    "SELECT * FROM TABLE(bigquery.system.query(query => 'SELECT schema_name FROM `%s.region-us.INFORMATION_SCHEMA.SCHEMATA`'))",
                    parentProjectId)))
                    .contains(row(trinoSchema));
            // table created in parentProjectId by default
            assertThat(computeActual("SHOW TABLES FROM " + trinoSchema).getOnlyColumn()).contains(tableName);
            assertThat(computeActual(format(
                    "SELECT * FROM TABLE(bigquery.system.query(query => 'SELECT table_name FROM `%s.region-us.INFORMATION_SCHEMA.TABLES` WHERE table_schema = \"%s\"'))",
                    projectId,
                    trinoSchema)))
                    .doesNotContain(row(tableName));
            assertThat(query(format(
                    "SELECT * FROM TABLE(bigquery.system.query(query => 'SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = \"%s\"'))",
                    trinoSchema)))
                    .failure()
                    .hasMessageContaining("Table \"INFORMATION_SCHEMA.TABLES\" must be qualified with a dataset (e.g. dataset.table)");
            assertThat(computeActual(format(
                    "SELECT * FROM TABLE(bigquery.system.query(query => 'SELECT table_name FROM `%s.region-us.INFORMATION_SCHEMA.TABLES` WHERE table_schema = \"%s\"'))",
                    parentProjectId,
                    trinoSchema)))
                    .contains(row(tableName));
            assertThat(query("SELECT * FROM " + table.getName())).returnsEmptyResult();
            assertThat(query("SELECT * FROM TABLE(bigquery.system.query(query => 'SELECT * FROM `%s.%s`'))".formatted(parentProjectId, table.getName()))).returnsEmptyResult();
            assertThat(query("SELECT * FROM TABLE(bigquery.system.query(query => 'SELECT * FROM `%s.%s`'))".formatted(projectId, table.getName())))
                    .failure()
                    .hasMessageContaining("Failed to get destination table for query");
        }
    }
}
