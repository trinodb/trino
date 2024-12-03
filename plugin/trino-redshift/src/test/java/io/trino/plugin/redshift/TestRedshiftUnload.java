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
package io.trino.plugin.redshift;

import com.amazon.redshift.Driver;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.filesystem.s3.S3FileSystemStats;
import io.trino.plugin.base.mapping.DefaultIdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.JdbcDynamicFilteringConfig;
import io.trino.plugin.jdbc.JdbcDynamicFilteringSessionProperties;
import io.trino.plugin.jdbc.JdbcNamedRelationHandle;
import io.trino.plugin.jdbc.JdbcSplit;
import io.trino.plugin.jdbc.JdbcSplitManager;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.credential.ExtraCredentialProvider;
import io.trino.plugin.jdbc.credential.StaticCredentialProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.jdbc.logging.RemoteQueryModifier.NONE;
import static io.trino.plugin.redshift.RedshiftQueryRunner.AWS_ACCESS_KEY;
import static io.trino.plugin.redshift.RedshiftQueryRunner.AWS_REGION;
import static io.trino.plugin.redshift.RedshiftQueryRunner.AWS_SECRET_KEY;
import static io.trino.plugin.redshift.RedshiftQueryRunner.IAM_ROLE;
import static io.trino.plugin.redshift.RedshiftQueryRunner.S3_UNLOAD_ROOT;
import static io.trino.plugin.redshift.TestingRedshiftServer.JDBC_PASSWORD;
import static io.trino.plugin.redshift.TestingRedshiftServer.JDBC_URL;
import static io.trino.plugin.redshift.TestingRedshiftServer.JDBC_USER;
import static io.trino.plugin.redshift.TestingRedshiftServer.TEST_DATABASE;
import static io.trino.plugin.redshift.TestingRedshiftServer.TEST_SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestRedshiftUnload
{
    private RedshiftConfig redshiftJdbcConfig;
    private RedshiftSplitManager redshiftSplitManager;

    @BeforeAll
    public void setup()
            throws Exception
    {
        redshiftJdbcConfig = new RedshiftConfig().setUnloadLocation(S3_UNLOAD_ROOT).setUnloadIamRole(IAM_ROLE);
        redshiftSplitManager = createSplitManager();
    }

    private RedshiftSplitManager createSplitManager()
    {
        DriverConnectionFactory driverConnectionFactory = DriverConnectionFactory.builder(new Driver(), JDBC_URL, new ExtraCredentialProvider(Optional.empty(), Optional.empty(), new StaticCredentialProvider(Optional.of(JDBC_USER), Optional.of(JDBC_PASSWORD))))
                .build();
        RedshiftClient redshiftClient = new RedshiftClient(
                new BaseJdbcConfig().setConnectionUrl(JDBC_URL),
                redshiftJdbcConfig,
                driverConnectionFactory,
                new JdbcStatisticsConfig(),
                new DefaultQueryBuilder(NONE),
                new DefaultIdentifierMapping(),
                NONE);

        return new RedshiftSplitManager(
                redshiftClient,
                new DefaultQueryBuilder(NONE),
                NONE,
                new JdbcSplitManager(redshiftClient),
                redshiftJdbcConfig,
                new S3FileSystemFactory(OpenTelemetry.noop(), new S3FileSystemConfig()
                        .setAwsAccessKey(AWS_ACCESS_KEY)
                        .setAwsSecretKey(AWS_SECRET_KEY)
                        .setRegion(AWS_REGION)
                        .setStreamingPartSize(DataSize.valueOf("5.5MB")), new S3FileSystemStats()),
                Executors.newFixedThreadPool(1));
    }

    @Test
    void testUnloadFlow()
            throws Exception
    {
        JdbcTableHandle table =
                new JdbcTableHandle(
                        new SchemaTableName(TEST_SCHEMA, "nation"),
                        new RemoteTableName(Optional.of(TEST_DATABASE), Optional.of(TEST_SCHEMA), "nation"),
                        Optional.empty());

        ConnectorSession session = createUnloadSession();
        ConnectorSplitSource splitSource = redshiftSplitManager.getSplits(null, session, table, DynamicFilter.EMPTY, Constraint.alwaysTrue());
        assertThat(splitSource).isInstanceOf(RedshiftUnloadSplitSource.class);

        ConnectorSplitSource.ConnectorSplitBatch connectorSplitBatch = splitSource.getNextBatch(2).get();
        assertThat(connectorSplitBatch.isNoMoreSplits()).isTrue();
        assertThat(connectorSplitBatch.getSplits()).hasSize(2);
        assertThat(connectorSplitBatch.getSplits()).allMatch(split -> split instanceof RedshiftUnloadSplit);
        List<RedshiftUnloadSplit> redshiftUnloadSplits = connectorSplitBatch.getSplits().stream().map(split -> ((RedshiftUnloadSplit) split)).collect(toImmutableList());
        verifyS3UnloadedFiles(redshiftUnloadSplits);
        removeS3UnloadedFiles(redshiftUnloadSplits);
    }

    @Test
    void testUnloadFlowFallbackToJdbc()
            throws Exception
    {
        // Fallback to JDBC as limit clause is not supported by UNLOAD
        JdbcTableHandle table = new JdbcTableHandle(
                new JdbcNamedRelationHandle(
                        new SchemaTableName(TEST_SCHEMA, "nation"),
                        new RemoteTableName(Optional.of(TEST_DATABASE),
                                Optional.of(TEST_SCHEMA), "nation"),
                        Optional.empty()),
                TupleDomain.all(),
                ImmutableList.of(),
                Optional.empty(),
                OptionalLong.of(2),
                Optional.empty(),
                Optional.of(ImmutableSet.of()),
                0,
                Optional.empty(),
                ImmutableList.of());

        ConnectorSession session = createUnloadSession();
        try (ConnectorSplitSource splitSource = redshiftSplitManager.getSplits(null, session, table, DynamicFilter.EMPTY, Constraint.alwaysTrue())) {
            assertThat(splitSource).isInstanceOf(FixedSplitSource.class);

            ConnectorSplitSource.ConnectorSplitBatch connectorSplitBatch = splitSource.getNextBatch(2).get();
            assertThat(connectorSplitBatch.isNoMoreSplits()).isTrue();
            assertThat(connectorSplitBatch.getSplits()).hasSize(1);
            connectorSplitBatch.getSplits().forEach(split -> assertThat(split).isInstanceOf(JdbcSplit.class));
        }
    }

    private ConnectorSession createUnloadSession()
    {
        return TestingConnectorSession.builder()
                .setPropertyMetadata(
                        Stream.concat(
                                        new JdbcDynamicFilteringSessionProperties(new JdbcDynamicFilteringConfig()).getSessionProperties().stream(),
                                        new RedshiftSessionProperties(redshiftJdbcConfig).getSessionProperties().stream())
                                .toList())
                .build();
    }

    private static void verifyS3UnloadedFiles(List<RedshiftUnloadSplit> redshiftUnloadSplits)
    {
        assertThat(redshiftUnloadSplits).allMatch(redshiftUnloadSplit -> redshiftUnloadSplit.path().startsWith(S3_UNLOAD_ROOT));
        try (S3Client s3 = S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(AWS_ACCESS_KEY, AWS_SECRET_KEY)))
                .region(Region.of(AWS_REGION))
                .build()) {
            redshiftUnloadSplits.forEach(
                    redshiftUnloadSplit -> {
                        URI s3Path = URI.create(redshiftUnloadSplit.path());
                        assertThat(s3.headObject(request -> request.bucket(s3Path.getHost()).key(s3Path.getPath().substring(1))).contentLength()).isEqualTo(redshiftUnloadSplit.length());
                    });
        }
    }

    private static void removeS3UnloadedFiles(List<RedshiftUnloadSplit> redshiftUnloadSplits)
    {
        try (S3Client s3 = S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(AWS_ACCESS_KEY, AWS_SECRET_KEY)))
                .region(Region.of(AWS_REGION))
                .build()) {
            redshiftUnloadSplits.forEach(
                    redshiftUnloadSplit -> {
                        URI s3Path = URI.create(redshiftUnloadSplit.path());
                        s3.deleteObject(request -> request.bucket(s3Path.getHost()).key(s3Path.getPath().substring(1)));
                    });
        }
    }
}
