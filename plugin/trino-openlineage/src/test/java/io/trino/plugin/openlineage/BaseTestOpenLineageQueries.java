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
package io.trino.plugin.openlineage;

import io.airlift.units.Duration;
import io.trino.testing.AbstractTestQueryFramework;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static io.trino.testing.assertions.Assert.assertEventually;
import static java.lang.String.format;

public abstract class BaseTestOpenLineageQueries
        extends AbstractTestQueryFramework
{
    private static final Duration TIMEOUT = Duration.valueOf("10s");
    protected static final String TRINO_URI = "http://trino-integration-test:1337";
    protected static final String OPEN_LINEAGE_NAMESPACE = format("trino://%s:%s", URI.create(TRINO_URI).getHost(), URI.create(TRINO_URI).getPort());

    @Test
    void testCreateTableAsSelectFromTable()
            throws Exception
    {
        String outputTable = "test_create_table_as_select_from_table";

        @Language("SQL") String createTableQuery = format(
                "CREATE TABLE %s AS SELECT * FROM tpch.tiny.nation",
                outputTable);

        String queryId = this.getQueryRunner()
                .executeWithPlan(this.getSession(), createTableQuery)
                .queryId()
                .toString();

        assertEventually(TIMEOUT, () -> assertCreateTableAsSelectFromTable(queryId, createTableQuery));
    }

    public abstract void assertCreateTableAsSelectFromTable(String queryId, String query)
            throws Exception;

    @Test
    void testCreateTableAsSelectFromView()
            throws Exception
    {
        String viewName = "test_view";
        String outputTable = "test_create_table_as_select_from_view";

        @Language("SQL") String createViewQuery = format(
                "CREATE VIEW %s AS SELECT * FROM tpch.tiny.nation",
                viewName);

        String createViewQueryId = this.getQueryRunner()
                .executeWithPlan(this.getSession(), createViewQuery)
                .queryId()
                .toString();

        @Language("SQL") String createTableQuery = format(
                "CREATE TABLE %s AS SELECT * FROM %s",
                outputTable, viewName);

        String createTableQueryId = this.getQueryRunner()
                .executeWithPlan(this.getSession(), createTableQuery)
                .queryId()
                .toString();

        assertEventually(TIMEOUT, () -> assertCreateTableAsSelectFromView(
                createViewQueryId,
                createViewQuery,
                createTableQueryId,
                createTableQuery));
    }

    public abstract void assertCreateTableAsSelectFromView(String createViewQueryId, String createViewQuery, String createTableQueryId, String createTableQuery)
            throws Exception;
}
