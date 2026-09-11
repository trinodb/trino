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
package io.trino.plugin.session.db;

import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.Isolated;
import org.testcontainers.containers.JdbcDatabaseContainer;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
@Isolated
public abstract class BaseTestDbSessionPropertyManagerFlywayMigration
{
    protected final JdbcDatabaseContainer<?> container = startContainer();
    protected final Jdbi jdbi = Jdbi.create(container.getJdbcUrl(), container.getUsername(), container.getPassword());

    protected abstract JdbcDatabaseContainer<?> startContainer();

    protected abstract boolean tableExists(String tableName);

    @AfterAll
    public final void close()
    {
        container.close();
    }

    @Test
    public void testMigrationWithEmptyDatabase()
    {
        DbSessionPropertyManagerConfig config = new DbSessionPropertyManagerConfig()
                .setConfigDbUrl(container.getJdbcUrl())
                .setConfigDbUser(container.getUsername())
                .setConfigDbPassword(container.getPassword());
        new FlywayMigration(config).migrate();
        verifySessionPropertiesSchema();

        dropAllTables();
    }

    @Test
    public void testMigrationWithNonEmptyDatabase()
    {
        Handle jdbiHandle = jdbi.open();
        jdbiHandle.execute("CREATE TABLE t1 (id INT)");
        jdbiHandle.execute("CREATE TABLE t2 (id INT)");
        DbSessionPropertyManagerConfig config = new DbSessionPropertyManagerConfig()
                .setConfigDbUrl(container.getJdbcUrl())
                .setConfigDbUser(container.getUsername())
                .setConfigDbPassword(container.getPassword());
        new FlywayMigration(config).migrate();
        verifySessionPropertiesSchema();
        jdbiHandle.execute("DROP TABLE t1");
        jdbiHandle.execute("DROP TABLE t2");
        jdbiHandle.close();

        dropAllTables();
    }

    @Test
    public void testMigrationDisabled()
    {
        DbSessionPropertyManagerConfig config = new DbSessionPropertyManagerConfig()
                .setConfigDbUrl(container.getJdbcUrl())
                .setConfigDbUser(container.getUsername())
                .setConfigDbPassword(container.getPassword())
                .setRunMigrationsEnabled(false);
        new FlywayMigration(config).migrate();
        assertThat(tableExists("session_specs")).isFalse();
        assertThat(tableExists("session_client_tags")).isFalse();
        assertThat(tableExists("session_property_values")).isFalse();
    }

    protected void verifySessionPropertiesSchema()
    {
        verifyResultSetCount("SELECT user_regex FROM session_specs", 0);
        verifyResultSetCount("SELECT client_tag FROM session_client_tags", 0);
        verifyResultSetCount("SELECT session_property_name FROM session_property_values", 0);
    }

    private void verifyResultSetCount(String sql, int expectedCount)
    {
        List<String> results = jdbi.withHandle(handle ->
                handle.createQuery(sql).mapTo(String.class).list());
        assertThat(results).hasSize(expectedCount);
    }

    protected void dropAllTables()
    {
        Handle jdbiHandle = jdbi.open();
        jdbiHandle.execute("DROP TABLE IF EXISTS session_property_values");
        jdbiHandle.execute("DROP TABLE IF EXISTS session_client_tags");
        jdbiHandle.execute("DROP TABLE IF EXISTS session_specs");
        jdbiHandle.execute("DROP TABLE IF EXISTS flyway_schema_history");
        jdbiHandle.close();
    }
}
