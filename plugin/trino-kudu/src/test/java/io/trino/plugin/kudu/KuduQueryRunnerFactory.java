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
package io.trino.plugin.kudu;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.apache.kudu.client.KuduClient;

import java.util.Map;
import java.util.Optional;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.Session.SessionBuilder;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;

public final class KuduQueryRunnerFactory
{
    private KuduQueryRunnerFactory() {}

    public static QueryRunner createKuduQueryRunner(TestingKuduServer kuduServer, Session session)
            throws Exception
    {
        QueryRunner runner = null;
        try {
            runner = DistributedQueryRunner.builder(session).build();

            installKuduConnector(kuduServer.getMasterAddress(), runner, session.getSchema().orElse("kudu_smoke_test"), Optional.of(""), ImmutableMap.of());

            return runner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, runner);
            throw e;
        }
    }

    public static QueryRunner createKuduQueryRunner(TestingKuduServer kuduServer, String kuduSchema)
            throws Exception
    {
        QueryRunner runner = null;
        try {
            runner = DistributedQueryRunner.builder(createSession(kuduSchema)).build();

            installKuduConnector(kuduServer.getMasterAddress(), runner, kuduSchema, Optional.of(""), ImmutableMap.of());

            return runner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, runner);
            throw e;
        }
    }

    public static QueryRunner createKuduQueryRunner(String kuduSchema, Map<String, String> connectorProperties)
            throws Exception
    {
        QueryRunner runner = null;
        try {
            runner = DistributedQueryRunner.builder(createSession(kuduSchema)).build();
            installKuduConnector(runner, connectorProperties);
            return runner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, runner);
            throw e;
        }
    }

    public static QueryRunner createKuduQueryRunnerTpch(TestingKuduServer kuduServer, Optional<String> kuduSchemaEmulationPrefix, TpchTable<?>... tables)
            throws Exception
    {
        return createKuduQueryRunnerTpch(kuduServer, kuduSchemaEmulationPrefix, ImmutableList.copyOf(tables));
    }

    public static QueryRunner createKuduQueryRunnerTpch(TestingKuduServer kuduServer, Optional<String> kuduSchemaEmulationPrefix, Iterable<TpchTable<?>> tables)
            throws Exception
    {
        return createKuduQueryRunnerTpch(kuduServer, kuduSchemaEmulationPrefix, ImmutableMap.of(), ImmutableMap.of(), tables);
    }

    public static QueryRunner createKuduQueryRunnerTpch(HostAndPort kuduServerMasterAddress, Optional<String> kuduSchemaEmulationPrefix, Iterable<TpchTable<?>> tables)
            throws Exception
    {
        return createKuduQueryRunnerTpch(kuduServerMasterAddress, kuduSchemaEmulationPrefix, ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of(), tables);
    }

    public static QueryRunner createKuduQueryRunnerTpch(
            TestingKuduServer kuduServer,
            Optional<String> kuduSchemaEmulationPrefix,
            Map<String, String> kuduSessionProperties,
            Map<String, String> extraProperties,
            Iterable<TpchTable<?>> tables)
            throws Exception
    {
        return createKuduQueryRunnerTpch(kuduServer.getMasterAddress(), kuduSchemaEmulationPrefix, kuduSessionProperties, ImmutableMap.of(), extraProperties, tables);
    }

    public static QueryRunner createKuduQueryRunnerTpch(
            HostAndPort masterAddress,
            Optional<String> kuduSchemaEmulationPrefix,
            Map<String, String> kuduSessionProperties,
            Map<String, String> kuduExtraProperties,
            Map<String, String> extraProperties,
            Iterable<TpchTable<?>> tables)
            throws Exception
    {
        QueryRunner runner = null;
        try {
            String kuduSchema = kuduSchemaEmulationPrefix.isPresent() ? "tpch" : "default";
            Session session = createSession(kuduSchema, kuduSessionProperties);
            runner = DistributedQueryRunner.builder(session)
                    .setExtraProperties(extraProperties)
                    .build();

            runner.installPlugin(new TpchPlugin());
            runner.createCatalog("tpch", "tpch");

            installKuduConnector(masterAddress, runner, kuduSchema, kuduSchemaEmulationPrefix, kuduExtraProperties);

            copyTpchTables(runner, "tpch", TINY_SCHEMA_NAME, session, tables);

            return runner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, runner);
            throw e;
        }
    }

    public static KuduClient createKuduClient(TestingKuduServer kuduServer)
    {
        KuduClient.KuduClientBuilder builder = new KuduClient.KuduClientBuilder(kuduServer.getMasterAddress().toString());
        return builder.build();
    }

    private static void installKuduConnector(
            QueryRunner runner,
            Map<String, String> connectorProperties)
    {
        runner.installPlugin(new KuduPlugin());
        runner.createCatalog("kudu", "kudu", connectorProperties);
    }

    private static void installKuduConnector(
            HostAndPort masterAddress,
            QueryRunner runner,
            String kuduSchema,
            Optional<String> kuduSchemaEmulationPrefix,
            Map<String, String> kuduExtraProperties)
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .putAll(kuduExtraProperties);
        if (kuduSchemaEmulationPrefix.isPresent()) {
            properties.put("kudu.schema-emulation.enabled", "true");
            properties.put("kudu.schema-emulation.prefix", kuduSchemaEmulationPrefix.get());
            properties.put("kudu.client.master-addresses", masterAddress.toString());
        }
        else {
            properties.put("kudu.schema-emulation.enabled", "false");
            properties.put("kudu.client.master-addresses", masterAddress.toString());
        }

        runner.installPlugin(new KuduPlugin());
        runner.createCatalog("kudu", "kudu", properties.buildOrThrow());

        if (kuduSchemaEmulationPrefix.isPresent()) {
            runner.execute("DROP SCHEMA IF EXISTS " + kuduSchema);
            runner.execute("CREATE SCHEMA " + kuduSchema);
        }
    }

    public static Session createSession(String schema, Map<String, String> kuduSessionProperties)
    {
        SessionBuilder builder = testSessionBuilder()
                .setCatalog("kudu")
                .setSchema(schema);
        kuduSessionProperties.forEach((k, v) -> builder.setCatalogSessionProperty("kudu", k, v));
        return builder.build();
    }

    public static Session createSession(String schema)
    {
        return testSessionBuilder()
                .setCatalog("kudu")
                .setSchema(schema)
                .build();
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        QueryRunner queryRunner = createKuduQueryRunnerTpch(
                new TestingKuduServer(),
                Optional.empty(),
                ImmutableMap.of(),
                ImmutableMap.of("http-server.http.port", "8080"),
                TpchTable.getTables());
        Logger log = Logger.get(KuduQueryRunnerFactory.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
