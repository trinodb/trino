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

import com.google.common.collect.ImmutableMap;
import com.mongodb.ConnectionString;
import io.airlift.log.Logger;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.tpch.TpchTable;

import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.mongodb.TestingMongoAtlasInfoProvider.getConnectionString;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;

public final class MongoAtlasQueryRunner
{
    private static final String TPCH_SCHEMA = "tpch";

    private MongoAtlasQueryRunner() {}

    public static DistributedQueryRunner createMongoAtlasQueryRunner(
            ConnectionString connectionString,
            Map<String, String> extraProperties,
            Iterable<TpchTable<?>> tables)
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(MongoQueryRunner.createSession())
                .setExtraProperties(extraProperties)
                .build();
        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            Map<String, String> connectorProperties = Map.of("mongodb.connection-url", connectionString.toString());

            queryRunner.installPlugin(new MongoPlugin());
            queryRunner.createCatalog("mongodb", "mongodb", connectorProperties);
            queryRunner.execute("CREATE SCHEMA IF NOT EXISTS mongodb." + TPCH_SCHEMA);

            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, MongoQueryRunner.createSession(), tables);
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        DistributedQueryRunner atlasQueryRunner = createMongoAtlasQueryRunner(
                getConnectionString(),
                ImmutableMap.of("http-server.http.port", "8080"),
                TpchTable.getTables());
        Logger log = Logger.get(MongoAtlasQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", atlasQueryRunner.getCoordinator().getBaseUrl());
    }
}
