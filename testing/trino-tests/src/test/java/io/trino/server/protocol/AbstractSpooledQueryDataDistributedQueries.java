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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.client.ClientSession;
import io.trino.client.StatementClient;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spooling.filesystem.FileSystemSpoolingPlugin;
import io.trino.testing.AbstractTestEngineOnlyQueries;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingStatementClientFactory;
import io.trino.testing.TestingTrinoClient;
import io.trino.testing.containers.Minio;
import io.trino.tpch.TpchTable;
import okhttp3.OkHttpClient;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.client.StatementClientFactory.newStatementClient;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static io.trino.util.Ciphers.createRandomAesEncryptionKey;
import static java.util.Base64.getEncoder;

public abstract class AbstractSpooledQueryDataDistributedQueries
        extends AbstractTestEngineOnlyQueries
{
    private Minio minio;

    protected abstract String encoding();

    protected Map<String, String> spoolingConfig()
    {
        return Map.of();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        minio = closeAfterClass(Minio.builder().build());
        minio.start();

        String bucketName = "segments" + UUID.randomUUID();
        minio.createBucket(bucketName, true);

        DistributedQueryRunner queryRunner = MemoryQueryRunner.builder()
                .setInitialTables(TpchTable.getTables())
                .setTestingTrinoClientFactory((trinoServer, session) -> createClient(trinoServer, session, encoding()))
                .addExtraProperty("experimental.protocol.spooling.enabled", "true")
                .addExtraProperty("protocol.spooling.shared-secret-key", randomAES256Key())
                .setAdditionalSetup(runner -> {
                    runner.installPlugin(new FileSystemSpoolingPlugin());
                    Map<String, String> spoolingConfig = ImmutableMap.<String, String>builder()
                            .put("fs.s3.enabled", "true")
                            .put("fs.location", "s3://" + bucketName + "/")
                            // Direct storage access with encryption requires SSE-c which is not yet implemented
                            .put("fs.segment.encryption", "false")
                            .put("s3.endpoint", minio.getMinioAddress())
                            .put("s3.region", MINIO_REGION)
                            .put("s3.aws-access-key", MINIO_ACCESS_KEY)
                            .put("s3.aws-secret-key", MINIO_SECRET_KEY)
                            .put("s3.path-style-access", "true")
                            .putAll(spoolingConfig())
                            .buildKeepingLast();
                    runner.loadSpoolingManager("filesystem", spoolingConfig);
                })
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

    private static TestingTrinoClient createClient(TestingTrinoServer testingTrinoServer, Session session, String encoding)
    {
        return new TestingTrinoClient(testingTrinoServer, new TestingStatementClientFactory() {
            @Override
            public StatementClient create(OkHttpClient httpClient, Session session, ClientSession clientSession, String query)
            {
                ClientSession clientSessionSpooled = ClientSession
                        .builder(clientSession)
                        .encoding(Optional.ofNullable(encoding))
                        .build();
                return newStatementClient(httpClient, clientSessionSpooled, query, Optional.empty());
            }
        }, session, new OkHttpClient());
    }

    private static String randomAES256Key()
    {
        return getEncoder().encodeToString(createRandomAesEncryptionKey().getEncoded());
    }
}
