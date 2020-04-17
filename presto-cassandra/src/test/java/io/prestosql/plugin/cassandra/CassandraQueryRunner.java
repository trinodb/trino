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
package io.prestosql.plugin.cassandra;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.tpch.TpchTable;

import static io.prestosql.plugin.cassandra.CassandraTestingUtils.createKeyspace;
import static io.prestosql.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.prestosql.testing.QueryAssertions.copyTpchTables;
import static io.prestosql.testing.TestingSession.testSessionBuilder;

public final class CassandraQueryRunner
{
    private CassandraQueryRunner() {}

    public static DistributedQueryRunner createCassandraQueryRunner(CassandraServer server, TpchTable<?>... tables)
            throws Exception
    {
        return createCassandraQueryRunner(server, ImmutableList.copyOf(tables));
    }

    public static DistributedQueryRunner createCassandraQueryRunner(CassandraServer server, Iterable<TpchTable<?>> tables)
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(createCassandraSession("tpch")).build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        queryRunner.installPlugin(new CassandraPlugin());
        queryRunner.createCatalog("cassandra", "cassandra", ImmutableMap.of(
                "cassandra.contact-points", server.getHost(),
                "cassandra.native-protocol-port", Integer.toString(server.getPort()),
                "cassandra.allow-drop-table", "true"));

        createKeyspace(server.getSession(), "tpch");
        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createCassandraSession("tpch"), tables);
        for (TpchTable<?> table : tables) {
            server.refreshSizeEstimates("tpch", table.getTableName());
        }

        return queryRunner;
    }

    public static Session createCassandraSession(String schema)
    {
        return testSessionBuilder()
                .setCatalog("cassandra")
                .setSchema(schema)
                .build();
    }
}
