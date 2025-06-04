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

import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.plugin.bigquery.BigQueryQueryRunner.BIGQUERY_CREDENTIALS_KEY;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingProperties.requiredNonEmptySystemProperty;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBigQueryWithDifferentProjectIdConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    private static final String ALTERNATE_PROJECT_CATALOG = "bigquery";
    private static final String SERVICE_ACCOUNT_CATALOG = "service_account_bigquery";

    protected String alternateProjectId;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.alternateProjectId = requiredNonEmptySystemProperty("testing.alternate-bq-project-id");

        QueryRunner queryRunner = BigQueryQueryRunner.builder()
                .setConnectorProperties(Map.of("bigquery.project-id", alternateProjectId))
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
        queryRunner.createCatalog(SERVICE_ACCOUNT_CATALOG, "bigquery", Map.of("bigquery.credentials-key", BIGQUERY_CREDENTIALS_KEY));
        return queryRunner;
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_TRUNCATE -> true;
            case SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_CREATE_VIEW,
                 SUPPORTS_MERGE,
                 SUPPORTS_RENAME_SCHEMA,
                 SUPPORTS_RENAME_TABLE,
                 SUPPORTS_UPDATE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
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
