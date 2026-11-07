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

import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;

public class TestDbSessionPropertyManagerMysqlFlywayMigration
        extends BaseTestDbSessionPropertyManagerFlywayMigration
{
    @Override
    protected final JdbcDatabaseContainer<?> startContainer()
    {
        JdbcDatabaseContainer<?> container = new TestingMySqlContainer();
        container.start();
        return container;
    }

    /**
     * Deployments that predate the migrations have tables created by the DAO's original DDL, with foreign keys that
     * don't cascade. Migrating them must keep their rules and make deletes cascade.
     */
    @Test
    public void testMigrationAddsCascadeToExistingSchema()
    {
        SessionPropertiesDao dao = jdbi.installPlugin(new SqlObjectPlugin()).onDemand(SessionPropertiesDao.class);
        dao.createSessionSpecsTable();
        dao.createSessionClientTagsTable();
        dao.createSessionPropertiesTable();
        dao.insertSpecRow(1, "alice.*", null, null, null, 0);
        dao.insertClientTag(1, "etl");
        dao.insertSessionProperty(1, "query_priority", "8");

        DbSessionPropertyManagerConfig config = new DbSessionPropertyManagerConfig()
                .setConfigDbUrl(container.getJdbcUrl())
                .setConfigDbUser(container.getUsername())
                .setConfigDbPassword(container.getPassword());
        new FlywayMigration(config).migrate();

        verifyResultSetCount("SELECT user_regex FROM session_specs", 1);
        verifyResultSetCount("SELECT client_tag FROM session_client_tags", 1);
        verifyResultSetCount("SELECT session_property_name FROM session_property_values", 1);

        jdbi.useHandle(handle -> handle.execute("DELETE FROM session_specs WHERE spec_id = 1"));
        verifySessionPropertiesSchema();

        dropAllTables();
    }

    @Override
    protected final boolean tableExists(String tableName)
    {
        return jdbi.withHandle(handle ->
                handle.createQuery("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = :tableName")
                        .bind("tableName", tableName)
                        .mapTo(Long.class)
                        .one()) > 0;
    }
}
