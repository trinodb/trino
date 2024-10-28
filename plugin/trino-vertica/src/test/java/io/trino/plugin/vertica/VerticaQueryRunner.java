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
package io.trino.plugin.vertica;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.plugin.jmx.JmxPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.security.Identity;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

public final class VerticaQueryRunner
{
    private VerticaQueryRunner() {}

    public static final String GRANTED_USER = "alice";
    public static final String NON_GRANTED_USER = "bob";
    public static final String TPCH_SCHEMA = "tpch";

    private static DistributedQueryRunner createVerticaQueryRunner(
            TestingVerticaServer server,
            Map<String, String> extraProperties,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> tables)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            DistributedQueryRunner.Builder<?> builder = DistributedQueryRunner.builder(createSession(GRANTED_USER, "vertica"));
            extraProperties.forEach(builder::addExtraProperty);
            queryRunner = builder.build();

            queryRunner.installPlugin(new JmxPlugin());
            queryRunner.createCatalog("jmx", "jmx");

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog(TPCH_SCHEMA, TPCH_SCHEMA);

            // Create two users, one of which will have access to the TPCH database/schema
            executeAsAdmin(server, "CREATE SCHEMA IF NOT EXISTS tpch");
            executeAsAdmin(server, "CREATE ROLE " + GRANTED_USER);
            executeAsAdmin(server, "CREATE ROLE " + NON_GRANTED_USER);
            executeAsAdmin(server, "GRANT ALL PRIVILEGES ON DATABASE tpch TO " + GRANTED_USER);
            executeAsAdmin(server, "GRANT ALL PRIVILEGES ON SCHEMA tpch TO " + GRANTED_USER);

            // Allow the user to set the roles
            executeAsAdmin(server, "GRANT " + GRANTED_USER + " TO " + server.getUsername());
            executeAsAdmin(server, "GRANT " + NON_GRANTED_USER + " TO " + server.getUsername());

            queryRunner.installPlugin(new VerticaPlugin());
            queryRunner.createCatalog("vertica", "vertica", connectorProperties);

            copyTpchTables(queryRunner, TPCH_SCHEMA, TINY_SCHEMA_NAME, createSession(GRANTED_USER, "vertica"), tables);

            // Revoke all access to the database for the server's user if impersonation is enabled
            // This will allow the impersonation to work as intended for testing as Vertica roles add to the user's existing permissions
            // Running queries with the NON_GRANTED_USER user/role will succeed because the user in the JDBC connection has access to the tables
            if (Boolean.parseBoolean(connectorProperties.getOrDefault("vertica.impersonation.enabled", "false"))) {
                executeAsAdmin(server, "REVOKE ALL ON SCHEMA tpch FROM " + server.getUsername());
                executeAsAdmin(server, "REVOKE ALL ON DATABASE tpch FROM " + server.getUsername());
            }

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    public static Session createSession(String user, String catalogName)
    {
        return testSessionBuilder()
                .setCatalog(catalogName)
                .setSchema(TPCH_SCHEMA)
                .setIdentity(Identity.ofUser(user))
                .build();
    }

    public static Builder builder(TestingVerticaServer server)
    {
        return new Builder(server);
    }

    public static class Builder
    {
        private final TestingVerticaServer server;
        private Iterable<TpchTable<?>> tables = ImmutableList.of();
        private Map<String, String> connectorProperties;
        private Map<String, String> extraProperties;

        public Builder(TestingVerticaServer server)
        {
            this.server = requireNonNull(server, "server is null");
            connectorProperties = ImmutableMap.<String, String>builder()
                    .put("connection-url", requireNonNull(server.getJdbcUrl(), "jdbcUrl is null"))
                    .put("connection-user", requireNonNull(server.getUsername(), "user is null"))
                    .put("connection-password", requireNonNull(server.getPassword(), "password is null"))
                    .buildOrThrow();
            extraProperties = ImmutableMap.of();
        }

        public Builder addConnectorProperties(Map<String, String> properties)
        {
            connectorProperties = updateProperties(connectorProperties, properties);
            return this;
        }

        public Builder addExtraProperties(Map<String, String> properties)
        {
            extraProperties = updateProperties(extraProperties, properties);
            return this;
        }

        public Builder setTables(Iterable<TpchTable<?>> tables)
        {
            this.tables = ImmutableList.copyOf(requireNonNull(tables, "tables is null"));
            return this;
        }

        public QueryRunner build()
                throws Exception
        {
            return createVerticaQueryRunner(
                    server,
                    extraProperties,
                    connectorProperties,
                    tables);
        }
    }

    private static Map<String, String> updateProperties(Map<String, String> properties, Map<String, String> update)
    {
        return ImmutableMap.<String, String>builder()
                .putAll(requireNonNull(properties, "properties is null"))
                .putAll(requireNonNull(update, "update is null"))
                .buildOrThrow();
    }

    private static void executeAsAdmin(TestingVerticaServer server, String sql)
    {
        server.execute(sql, "dbadmin", null);
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();

        DistributedQueryRunner queryRunner = (DistributedQueryRunner) builder(new TestingVerticaServer())
                .addExtraProperties(ImmutableMap.of("http-server.http.port", "8080"))
                .setTables(TpchTable.getTables())
                .build();

        Logger log = Logger.get(VerticaQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
