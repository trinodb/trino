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
import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spooling.filesystem.FileSystemSpoolingPlugin;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.TestingStatementClientFactory;
import io.trino.testing.TestingTrinoClient;
import io.trino.tests.tpch.TpchQueryRunner;
import io.trino.tpch.TpchTable;
import okhttp3.OkHttpClient;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static io.trino.SystemSessionProperties.SPOOLING_ENABLED;
import static io.trino.client.StatementClientFactory.newStatementClient;
import static io.trino.server.protocol.spooling.SpoolingSessionProperties.INLINING_ENABLED;
import static io.trino.server.protocol.spooling.SpoolingSessionProperties.INLINING_MAX_ROWS;
import static io.trino.util.Ciphers.createRandomAesEncryptionKey;
import static java.util.Base64.getEncoder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public abstract class AbstractSpooledQueries
{
    private LocalStackContainer localstack;
    private DistributedQueryRunner spooledRunner;
    private DistributedQueryRunner nonSpooledRunner;
    private QueryAssertions assertions;

    private final String testBucket = "segments" + UUID.randomUUID();

    protected abstract String encoding();

    protected Map<String, String> spoolingFileSystemConfig()
    {
        return Map.of();
    }

    protected Map<String, String> spoolingConfig()
    {
        return Map.of();
    }

    @BeforeAll
    protected void createQueryRunner()
            throws Exception
    {
        localstack = new LocalStackContainer("s3-latest");
        localstack.start();

        try (S3Client client = createS3Client(localstack)) {
            client.createBucket(CreateBucketRequest.builder().bucket(testBucket).build());
        }

        spooledRunner = MemoryQueryRunner.builder()
                .setInitialTables(TpchTable.getTables())
                .setTestingTrinoClientFactory((trinoServer, session) -> createClient(trinoServer, session, encoding()))
                .addExtraProperty("protocol.spooling.enabled", "true")
                .addExtraProperty("protocol.spooling.shared-secret-key", randomAES256Key())
                .addExtraProperties(spoolingConfig())
                .setAdditionalSetup(runner -> {
                    runner.installPlugin(new FileSystemSpoolingPlugin());
                    Map<String, String> spoolingConfig = ImmutableMap.<String, String>builder()
                            .put("fs.s3.enabled", "true")
                            .put("fs.location", "s3://" + testBucket + "/")
                            .put("fs.segment.encryption", "true")
                            .put("fs.segment.pruning.enabled", "false") // We want to test whether all segments are acknowledged
                            .put("s3.endpoint", localstack.getEndpointOverride(Service.S3).toString())
                            .put("s3.region", localstack.getRegion())
                            .put("s3.aws-access-key", localstack.getAccessKey())
                            .put("s3.aws-secret-key", localstack.getSecretKey())
                            .putAll(spoolingFileSystemConfig())
                            .buildKeepingLast();
                    runner.loadSpoolingManager("filesystem", spoolingConfig);
                })
                .build();

        nonSpooledRunner = TpchQueryRunner.builder()
                .build();

        assertions = new QueryAssertions(spooledRunner);
    }

    @AfterAll
    public void destroy()
    {
        if (spooledRunner != null) {
            spooledRunner.close();
            spooledRunner = null;
            assertions = null;
        }

        if (nonSpooledRunner != null) {
            nonSpooledRunner.close();
            nonSpooledRunner = null;
        }

        if (localstack != null) {
            localstack.close();
        }
    }

    @Test
    public void testDifferentQueryShapes()
    {
        // Ensure that spooling is enabled for SELECT queries
        assertThat(assertions.query(spoolingDisabled(), "SELECT * FROM nation"))
                .result()
                .hasNoQueryDataEncoding();

        // Ensure that spooling can be disabled for SELECT queries
        assertThat(assertions.query("SELECT * FROM nation"))
                .result()
                .hasQueryDataEncoding(encoding());

        // The rest of the cases are not meant to cover all possible query shapes
        assertThat(assertions.query("EXPLAIN SELECT * FROM nation"))
                .result()
                .hasNoQueryDataEncoding();

        assertThat(assertions.query("CREATE TABLE spooling_test (col INT)"))
                .result()
                .hasNoQueryDataEncoding();

        assertThat(assertions.query("INSERT INTO spooling_test (col) VALUES (2137)"))
                .result()
                .hasNoQueryDataEncoding();

        assertThat(assertions.query("SHOW SESSION"))
                .result()
                .hasNoQueryDataEncoding();

        assertThat(assertions.query("DROP TABLE spooling_test"))
                .result()
                .hasNoQueryDataEncoding();
    }

    @Test
    public void testReadingNationTable()
    {
        assertSameResult("SELECT * FROM nation");
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

    public void assertSameResult(Session session, @Language("SQL") String query)
    {
        assertThat(assertions.query(session, query))
                .result()
                .matches(nonSpooledRunner.execute(query));
    }

    public void assertSameResult(@Language("SQL") String query)
    {
        assertSameResult(inliningEnabled(), query);
        assertSameResult(inliningDisabled(), query);
        assertSameResult(spoolingDisabled(), query);
    }

    private Session inliningDisabled()
    {
        return Session.builder(spooledRunner.getDefaultSession())
                .setSystemProperty(INLINING_ENABLED, "false")
                .build();
    }

    private Session inliningEnabled()
    {
        return Session.builder(spooledRunner.getDefaultSession())
                .setSystemProperty(INLINING_ENABLED, "true")
                .setSystemProperty(INLINING_MAX_ROWS, "1000")
                .build();
    }

    private Session spoolingDisabled()
    {
        return Session.builder(spooledRunner.getDefaultSession())
                .setSystemProperty(SPOOLING_ENABLED, "false")
                .build();
    }

    private static String randomAES256Key()
    {
        return getEncoder().encodeToString(createRandomAesEncryptionKey().getEncoded());
    }

    protected S3Client createS3Client(LocalStackContainer localstack)
    {
        return S3Client.builder()
                .endpointOverride(localstack.getEndpointOverride(Service.S3))
                .region(Region.of(localstack.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())))
                .build();
    }
}
