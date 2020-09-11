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
package io.prestosql.plugin.kudu;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.prestosql.Session;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tpch.TpchTable;

import java.util.Map;
import java.util.Optional;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.prestosql.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.prestosql.testing.QueryAssertions.copyTpchTables;
import static io.prestosql.testing.TestingSession.testSessionBuilder;

public final class KuduQueryRunnerFactory
{
    private KuduQueryRunnerFactory() {}

    public static QueryRunner createKuduQueryRunner(TestingKuduServer kuduServer, Session session)
            throws Exception
    {
        QueryRunner runner = null;
        try {
            runner = DistributedQueryRunner.builder(session).setNodeCount(3).build();

            installKuduConnector(kuduServer.getMasterAddress(), runner, session.getSchema().orElse("kudu_smoke_test"), Optional.of(""));

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
            runner = DistributedQueryRunner.builder(createSession(kuduSchema)).setNodeCount(3).build();

            installKuduConnector(kuduServer.getMasterAddress(), runner, kuduSchema, Optional.of(""));

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
        DistributedQueryRunner runner = null;
        try {
            String kuduSchema = kuduSchemaEmulationPrefix.isPresent() ? "tpch" : "default";
            runner = DistributedQueryRunner.builder(createSession(kuduSchema)).setNodeCount(3).build();

            runner.installPlugin(new TpchPlugin());
            runner.createCatalog("tpch", "tpch");

            installKuduConnector(kuduServer.getMasterAddress(), runner, kuduSchema, kuduSchemaEmulationPrefix);

            copyTpchTables(runner, "tpch", TINY_SCHEMA_NAME, createSession(kuduSchema), tables);

            return runner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, runner);
            throw e;
        }
    }

    private static void installKuduConnector(HostAndPort masterAddress, QueryRunner runner, String kuduSchema, Optional<String> kuduSchemaEmulationPrefix)
    {
        Map<String, String> properties;
        if (kuduSchemaEmulationPrefix.isPresent()) {
            properties = ImmutableMap.of(
                    "kudu.schema-emulation.enabled", "true",
                    "kudu.schema-emulation.prefix", kuduSchemaEmulationPrefix.get(),
                    "kudu.client.master-addresses", masterAddress.toString());
        }
        else {
            properties = ImmutableMap.of(
                    "kudu.schema-emulation.enabled", "false",
                    "kudu.client.master-addresses", masterAddress.toString());
        }

        runner.installPlugin(new KuduPlugin());
        runner.createCatalog("kudu", "kudu", properties);

        if (kuduSchemaEmulationPrefix.isPresent()) {
            runner.execute("DROP SCHEMA IF EXISTS " + kuduSchema);
            runner.execute("CREATE SCHEMA " + kuduSchema);
        }
    }

    public static Session createSession(String schema)
    {
        return testSessionBuilder()
                .setCatalog("kudu")
                .setSchema(schema)
                .build();
    }
}
