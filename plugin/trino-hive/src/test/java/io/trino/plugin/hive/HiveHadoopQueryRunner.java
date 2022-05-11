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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.plugin.hive.containers.HiveHadoop;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SelectedRole;
import io.trino.testing.DistributedQueryRunner;
import io.trino.tpch.TpchTable;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.spi.security.SelectedRole.Type.ROLE;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;

public final class HiveHadoopQueryRunner
{
    public static final SelectedRole ADMIN_ROLE = new SelectedRole(ROLE, Optional.of("admin"));
    public static final Identity ADMIN_IDENTITY = Identity.forUser("hive")
            .withConnectorRoles(ImmutableMap.of(
                    "hive", ADMIN_ROLE,
                    "tpch", ADMIN_ROLE))
            .build();

    private HiveHadoopQueryRunner() {}

    public static DistributedQueryRunner createHadoopQueryRunner(
            HiveHadoop server,
            Identity identity,
            Map<String, String> extraProperties,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> tables)
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(createSession(identity))
                .setExtraProperties(extraProperties)
                .build();
        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.installPlugin(new TestingHivePlugin());
            queryRunner.createCatalog("tpch", "tpch");

            String hostAddress = InetAddress.getLocalHost().getHostAddress();
            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("hive.metastore.uri", format("thrift://%s:%d", hostAddress, server.getHiveMetastorePort()));
            connectorProperties.putIfAbsent("hive.metastore.thrift.client.socks-proxy", format("%s:%d", hostAddress, server.getSocksProxyPort()));
            connectorProperties.putIfAbsent("hive.hdfs.socks-proxy", format("%s:%d", hostAddress, server.getSocksProxyPort()));

            queryRunner.createCatalog("hive", "hive", connectorProperties);

            queryRunner.execute(createSession(identity), "CREATE SCHEMA tpch");
            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(identity), tables);
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    private static Session createSession(Identity identity)
    {
        return testSessionBuilder()
                .setIdentity(identity)
                .setCatalog("hive")
                .setSchema("tpch")
                .build();
    }

    public static void main(String[] args)
            throws Exception
    {
        // Set "--user hive" to your CLI
        HiveHadoop hadoopServer = HiveHadoop.builder().build();
        hadoopServer.start();
        DistributedQueryRunner queryRunner = createHadoopQueryRunner(
                hadoopServer,
                ADMIN_IDENTITY,
                ImmutableMap.of("http-server.http.port", "8080"),
                ImmutableMap.of("hive.security", "sql-standard"),
                TpchTable.getTables());

        Logger log = Logger.get(HiveHadoopQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
