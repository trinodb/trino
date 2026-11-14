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
package io.trino.plugin.resourcegroups.db;

import com.google.common.collect.ImmutableMap;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

public final class TestingResourceGroupsDatabase
{
    private static final AtomicInteger SCHEMA_COUNTER = new AtomicInteger();
    private static final JdbcDatabaseContainer<?> CONTAINER = new PostgreSQLContainer("postgres:18");

    static {
        CONTAINER.start();
    }

    private final String jdbcUrl;
    private final String user;
    private final String password;
    private final Jdbi jdbi;
    private final TestingResourceGroupsDao dao;

    private TestingResourceGroupsDatabase(String jdbcUrl, String user, String password)
    {
        this.jdbcUrl = requireNonNull(jdbcUrl, "jdbcUrl is null");
        this.user = requireNonNull(user, "user is null");
        this.password = requireNonNull(password, "password is null");
        this.jdbi = Jdbi.create(ClassLoaderAwareConnectionFactory.create(jdbcUrl, user, password));
        this.dao = jdbi.installPlugin(new SqlObjectPlugin()).onDemand(TestingResourceGroupsDao.class);
    }

    public static TestingResourceGroupsDatabase create()
    {
        String schema = "resource_groups_test_" + SCHEMA_COUNTER.incrementAndGet();
        try (Connection connection = ClassLoaderAwareConnectionFactory.create(CONTAINER.getJdbcUrl(), CONTAINER.getUsername(), CONTAINER.getPassword()).openConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA " + schema);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to create schema " + schema, e);
        }

        String jdbcUrl = CONTAINER.getJdbcUrl() + (CONTAINER.getJdbcUrl().contains("?") ? "&" : "?") + "currentSchema=" + schema;
        new FlywayDatabaseMigrator(jdbcUrl, CONTAINER.getUsername(), CONTAINER.getPassword(), SupportedDatabase.requireSupported(jdbcUrl).getMigrationLocation()).migrate();
        return new TestingResourceGroupsDatabase(jdbcUrl, CONTAINER.getUsername(), CONTAINER.getPassword());
    }

    public String getJdbcUrl()
    {
        return jdbcUrl;
    }

    public String getContainerId()
    {
        return CONTAINER.getContainerId();
    }

    public String getUser()
    {
        return user;
    }

    public String getPassword()
    {
        return password;
    }

    public TestingResourceGroupsDao getDao()
    {
        return dao;
    }

    public Map<String, String> getConfigurationProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("resource-groups.config-db-url", jdbcUrl)
                .put("resource-groups.config-db-user", user)
                .put("resource-groups.config-db-password", password)
                .buildOrThrow();
    }
}
