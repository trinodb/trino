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
package io.trino.plugin.mongodb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;

public final class MongoQueryRunner
{
    private static final String TPCH_SCHEMA = "tpch";

    private MongoQueryRunner() {}

    public static DistributedQueryRunner createMongoQueryRunner(MongoServer server, TpchTable<?>... tables)
            throws Exception
    {
        return createMongoQueryRunner(server, ImmutableMap.of(), ImmutableList.copyOf(tables));
    }

    public static DistributedQueryRunner createMongoQueryRunner(MongoServer server, Map<String, String> extraProperties, Iterable<TpchTable<?>> tables)
            throws Exception
    {
        return createMongoQueryRunner(server, extraProperties, ImmutableMap.of(), ImmutableMap.of(), tables, runner -> {});
    }

    public static DistributedQueryRunner createMongoQueryRunner(
            MongoServer server,
            Map<String, String> extraProperties,
            Map<String, String> coordinatorProperties,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> tables,
            Consumer<QueryRunner> moreSetup)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner.builder(createSession())
                    .setExtraProperties(extraProperties)
                    .setCoordinatorProperties(coordinatorProperties)
                    .setAdditionalSetup(moreSetup)
                    .build();

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            connectorProperties = new HashMap(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("mongodb.connection-url", server.getConnectionString().toString());

            queryRunner.installPlugin(new MongoPlugin());
            queryRunner.createCatalog("mongodb", "mongodb", connectorProperties);
            queryRunner.execute("CREATE SCHEMA mongodb." + TPCH_SCHEMA);

            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(), tables);
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    public static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("mongodb")
                .setSchema(TPCH_SCHEMA)
                .build();
    }

    public static MongoClient createMongoClient(MongoServer server)
    {
        return MongoClients.create(server.getConnectionString());
    }

    public static void main(String[] args)
            throws Exception
    {
        DistributedQueryRunner queryRunner = createMongoQueryRunner(
                new MongoServer(),
                ImmutableMap.of("http-server.http.port", "8080"),
                TpchTable.getTables());
        Thread.sleep(10);
        Logger log = Logger.get(MongoQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
