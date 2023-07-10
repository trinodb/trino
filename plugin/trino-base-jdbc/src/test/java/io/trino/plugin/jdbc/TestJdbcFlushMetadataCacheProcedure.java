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
package io.trino.plugin.jdbc;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.JdbcSqlExecutor;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.trino.plugin.jdbc.H2QueryRunner.createH2QueryRunner;
import static io.trino.tpch.TpchTable.NATION;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Test(singleThreaded = true) // some test assertions rely on `flush_metadata_cache()` being not executed yet, so cannot run concurrently
public class TestJdbcFlushMetadataCacheProcedure
        extends AbstractTestQueryFramework
{
    private JdbcSqlExecutor h2SqlExecutor;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .putAll(TestingH2JdbcModule.createProperties())
                .put("metadata.cache-ttl", "10m")
                .put("metadata.cache-missing", "true")
                .put("case-insensitive-name-matching", "true")
                .buildOrThrow();
        this.h2SqlExecutor = new JdbcSqlExecutor(properties.get("connection-url"), new Properties());
        return createH2QueryRunner(List.of(NATION), properties);
    }

    @Test
    public void testFlushMetadataCacheProcedureFlushMetadata()
    {
        h2SqlExecutor.execute("CREATE SCHEMA cached");
        assertUpdate("CREATE TABLE cached.cached AS SELECT * FROM tpch.nation", 25);

        // Verify that column cache is flushed
        // Fill caches
        assertQuerySucceeds("SELECT name, regionkey FROM cached.cached");

        // Rename column outside Trino
        h2SqlExecutor.execute("ALTER TABLE cached.cached ALTER COLUMN regionkey RENAME TO renamed");

        String renamedColumnQuery = "SELECT name, renamed FROM cached.cached";
        // Should fail as Trino has old metadata cached
        assertThatThrownBy(() -> getQueryRunner().execute(renamedColumnQuery))
                .hasMessageMatching(".*Column 'renamed' cannot be resolved");

        // Should succeed after flushing Trino JDBC metadata cache
        getQueryRunner().execute("CALL system.flush_metadata_cache()");
        assertQuerySucceeds(renamedColumnQuery);

        // Verify that table cache is flushed
        String showTablesSql = "SHOW TABLES FROM cached";
        // Fill caches
        assertQuery(showTablesSql, "VALUES ('cached')");

        // Rename table outside Trino
        h2SqlExecutor.execute("ALTER TABLE cached.cached RENAME TO cached.renamed");

        // Should still return old table name from cache
        assertQuery(showTablesSql, "VALUES ('cached')");

        // Should return new table name after cache flush
        getQueryRunner().execute("CALL system.flush_metadata_cache()");
        assertQuery(showTablesSql, "VALUES ('renamed')");

        // Verify that schema cache is flushed
        String showSchemasSql = "SHOW SCHEMAS from jdbc";
        // Fill caches
        assertQuery(showSchemasSql, "VALUES ('cached'), ('information_schema'), ('public'), ('tpch')");

        // Rename schema outside Trino
        h2SqlExecutor.execute("ALTER SCHEMA cached RENAME TO renamed");

        // Should still return old schemas from cache
        assertQuery(showSchemasSql, "VALUES ('cached'), ('information_schema'), ('public'), ('tpch')");

        // Should return new schema name after cache flush
        getQueryRunner().execute("CALL system.flush_metadata_cache()");
        assertQuery(showSchemasSql, "VALUES ('information_schema'), ('renamed'), ('public'), ('tpch')");
    }

    @Test
    public void testFlushMetadataCacheProcedureFlushIdentifierMapping()
    {
        assertUpdate("CREATE TABLE cached_name AS SELECT * FROM nation", 25);

        // Should succeed. Trino will cache lowercase identifier mapping to uppercase
        String query = "SELECT name, regionkey FROM cached_name";
        assertQuerySucceeds(query);

        // H2 stores unquoted names as uppercase. So this query should fail
        assertThatThrownBy(() -> h2SqlExecutor.execute("SELECT * FROM tpch.\"cached_name\""))
                .hasRootCauseMessage("Table \"cached_name\" not found (candidates are: \"CACHED_NAME\"); SQL statement:\n" +
                        "SELECT * FROM tpch.\"cached_name\" [42103-214]");
        // H2 stores unquoted names as uppercase. So this query should succeed
        h2SqlExecutor.execute("SELECT * FROM tpch.\"CACHED_NAME\"");

        // Rename to lowercase name outside Trino
        h2SqlExecutor.execute("ALTER TABLE tpch.\"CACHED_NAME\" RENAME TO tpch.\"cached_name\"");

        // Should fail as Trino has old lowercase identifier mapping to uppercase cached
        assertThatThrownBy(() -> getQueryRunner().execute(query))
                .hasMessageMatching("(?s)Table \"CACHED_NAME\" not found.*");

        // Should succeed after flushing Trino cache
        getQueryRunner().execute(getSession(), "CALL system.flush_metadata_cache()");
        assertQuerySucceeds(query);
    }
}
