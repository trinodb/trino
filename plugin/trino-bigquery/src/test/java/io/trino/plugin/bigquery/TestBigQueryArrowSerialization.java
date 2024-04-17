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

import io.trino.plugin.bigquery.BigQueryQueryRunner.BigQuerySqlExecutor;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.testing.TestingNames.randomNameSuffix;

public class TestBigQueryArrowSerialization
        extends AbstractTestQueryFramework
{
    private final BigQuerySqlExecutor bigQuerySqlExecutor;

    public TestBigQueryArrowSerialization()
    {
        this.bigQuerySqlExecutor = new BigQuerySqlExecutor();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return BigQueryQueryRunner.builder()
                .setConnectorProperties(Map.of("bigquery.arrow-serialization.enabled", "true"))
                .build();
    }

    @Test
    void testViewProjectionPredicates()
    {
        // Run query that columns in WHERE clause don't exist in projections.
        // Columns in ReadSession.setRowRestriction in ReadSessionCreator must exist in selected fields.
        String viewName = "test_projection_predicates_" + randomNameSuffix();

        onBigQuery("CREATE VIEW test." + viewName + " AS SELECT 1 AS id, 'test' AS data");
        try {
            assertQuery("SELECT data FROM test." + viewName + " WHERE id = 1", "VALUES 'test'");
            assertQuery("SELECT id FROM test." + viewName + " WHERE data = 'test'", "VALUES 1");
        }
        finally {
            onBigQuery("DROP VIEW test." + viewName);
        }
    }

    private void onBigQuery(@Language("SQL") String sql)
    {
        bigQuerySqlExecutor.execute(sql);
    }
}
