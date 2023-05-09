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
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import java.util.Map;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBigQueryWithDifferentProjectIdConnectorSmokeTest
        extends BaseBigQueryConnectorSmokeTest
{
    private static final String ALTERNATE_PROJECT_CATALOG = "bigquery";
    private static final String SERVICE_ACCOUNT_CATALOG = "service_account_bigquery";

    protected String alternateProjectId;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.alternateProjectId = requireNonNull(System.getProperty("testing.alternate-bq-project-id"), "testing.alternate-bq-project-id system property not set");

        QueryRunner queryRunner = BigQueryQueryRunner.createQueryRunner(
                ImmutableMap.of(),
                ImmutableMap.of("bigquery.project-id", alternateProjectId),
                REQUIRED_TPCH_TABLES);
        queryRunner.createCatalog(SERVICE_ACCOUNT_CATALOG, "bigquery", Map.of());
        return queryRunner;
    }

    @Test
    public void testCreateSchemasInDifferentProjectIdCatalog()
    {
        // This test case would fail without the bug fix https://github.com/trinodb/trino/issues/14083
        // It would create a schema in the wrong project, not the one defined in the catalog properties
        String serviceAccountSchema = "service_account_schema" + randomNameSuffix();
        String projectIdSchema = "project_id_schema" + randomNameSuffix();
        try {
            assertThat(computeActual("SHOW CATALOGS").getOnlyColumnAsSet())
                    .contains(ALTERNATE_PROJECT_CATALOG, SERVICE_ACCOUNT_CATALOG);

            assertUpdate("CREATE SCHEMA " + SERVICE_ACCOUNT_CATALOG + "." + serviceAccountSchema);
            assertQuery("SHOW SCHEMAS FROM " + SERVICE_ACCOUNT_CATALOG + " LIKE '" + serviceAccountSchema + "'", "VALUES '" + serviceAccountSchema + "'");

            assertUpdate("CREATE SCHEMA " + ALTERNATE_PROJECT_CATALOG + "." + projectIdSchema);
            assertQuery("SHOW SCHEMAS FROM " + ALTERNATE_PROJECT_CATALOG + " LIKE '" + projectIdSchema + "'", "VALUES '" + projectIdSchema + "'");
        }
        finally {
            assertUpdate("DROP SCHEMA IF EXISTS " + SERVICE_ACCOUNT_CATALOG + "." + serviceAccountSchema);
            assertUpdate("DROP SCHEMA IF EXISTS " + ALTERNATE_PROJECT_CATALOG + "." + projectIdSchema);
        }
    }

    @Test
    public void testNativeQuerySelectFromTestTable()
    {
        String suffix = randomNameSuffix();
        String tableName = ALTERNATE_PROJECT_CATALOG + ".test.test_select" + suffix;
        String bigQueryTableName = "`" + alternateProjectId + "`.test.test_select" + suffix;
        try {
            assertUpdate("CREATE TABLE " + tableName + " (col BIGINT)");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1), (2)", 2);
            assertQuery(
                    "SELECT * FROM TABLE(bigquery.system.query(query => 'SELECT * FROM " + bigQueryTableName + "'))",
                    "VALUES 1, 2");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }
}
