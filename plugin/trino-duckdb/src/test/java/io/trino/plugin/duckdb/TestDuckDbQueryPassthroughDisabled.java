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
package io.trino.plugin.duckdb;

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.duckdb.TestingDuckDb.TPCH_SCHEMA;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

final class TestDuckDbQueryPassthroughDisabled
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DuckDbQueryRunner.builder(closeAfterClass(new TestingDuckDb())).build();
    }

    @Test
    void testQueryTableFunctionNotRegistered()
    {
        assertQueryFails(
                "SELECT * FROM TABLE(system.query(query => 'SELECT * FROM read_text(''/etc/passwd'')'))",
                "line 1:21: Table function 'system.query' not registered");
    }

    @Test
    void testExecuteProcedureDenied()
    {
        String tableName = "test_execute_denied" + randomNameSuffix();
        assertQueryFails(
                "CALL system.execute('CREATE TABLE %s.%s(x int)')".formatted(TPCH_SCHEMA, tableName),
                "Access Denied: Cannot execute procedure system.execute: query pass-through is disabled; set duckdb.query-passthrough.enabled=true");
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
    }

    @Test
    void testRegularOperationsAllowed()
    {
        String tableName = "test_regular_operations" + randomNameSuffix();
        assertUpdate("CREATE TABLE %s(x int)".formatted(tableName));
        assertUpdate("INSERT INTO %s VALUES 1, 2".formatted(tableName), 2);
        assertQuery("SELECT * FROM " + tableName, "VALUES 1, 2");
        assertQuery("SHOW TABLES LIKE '%s'".formatted(tableName), "VALUES '%s'".formatted(tableName));
        assertUpdate("CALL system.flush_metadata_cache()");
        assertUpdate("DROP TABLE " + tableName);
    }
}
