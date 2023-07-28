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
package io.trino.server.protocol;

import io.trino.Session;
import io.trino.client.ClientSession;
import io.trino.client.QueryDataDecoder;
import io.trino.client.QueryDataSerialization;
import io.trino.client.StatementClient;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.testing.AbstractTestEngineOnlyQueries;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingStatementClientFactory;
import io.trino.testing.TestingTrinoClient;
import io.trino.tpch.TpchTable;
import okhttp3.OkHttpClient;

import java.util.Optional;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.client.ProtocolSwitchingQueryDataDecoder.negotiateProtocol;
import static io.trino.client.StatementClientFactory.newStatementClient;

public class TestEncodedQueryDataDistributedQueries
        extends AbstractTestEngineOnlyQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = MemoryQueryRunner.builder()
                        .setInitialTables(TpchTable.getTables())
                        .setTestingTrinoClientFactory(TestEncodedQueryDataDistributedQueries::createClient)
                        .build();
        queryRunner.getCoordinator().getSessionPropertyManager().addSystemSessionProperties(TEST_SYSTEM_PROPERTIES);
        try {
            queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                    .withSessionProperties(TEST_CATALOG_PROPERTIES)
                    .build()));
            queryRunner.createCatalog(TESTING_CATALOG, "mock");
        }
        catch (RuntimeException e) {
            throw closeAllSuppress(e, queryRunner);
        }
        return queryRunner;
    }

    private static TestingTrinoClient createClient(TestingTrinoServer testingTrinoServer, Session session)
    {
        QueryDataDecoder decoder = negotiateProtocol()
                .withInlineEncoding(QueryDataSerialization.JSON)
                .build();

        return new TestingTrinoClient(testingTrinoServer, new TestingStatementClientFactory() {
            @Override
            public StatementClient create(OkHttpClient httpClient, Session session, ClientSession clientSession, String query)
            {
                return newStatementClient(httpClient, decoder, clientSession, query, Optional.empty());
            }
        }, session, new OkHttpClient());
    }
}
