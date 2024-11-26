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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.jdbc.JdbcNamedRelationHandle;
import io.trino.plugin.jdbc.JdbcSplit;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.redshift.RedshiftQueryRunner.AWS_ACCESS_KEY;
import static io.trino.plugin.redshift.RedshiftQueryRunner.AWS_REGION;
import static io.trino.plugin.redshift.RedshiftQueryRunner.AWS_SECRET_KEY;
import static io.trino.plugin.redshift.RedshiftQueryRunner.IAM_ROLE;
import static io.trino.plugin.redshift.RedshiftQueryRunner.S3_UNLOAD_ROOT;
import static io.trino.plugin.redshift.TestingRedshiftServer.TEST_DATABASE;
import static io.trino.plugin.redshift.TestingRedshiftServer.TEST_SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.abort;

public class TestRedshiftUnloadConnectorTest
        extends TestRedshiftConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        redshiftJdbcConfig = new RedshiftConfig().setUnloadLocation(S3_UNLOAD_ROOT).setUnloadIamRole(IAM_ROLE);
        redshiftSplitManager = createSplitManager();

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("redshift.unload-location", S3_UNLOAD_ROOT)
                .put("redshift.unload-iam-role", IAM_ROLE)
                .put("s3.region", AWS_REGION)
                .put("s3.endpoint", "https://s3.%s.amazonaws.com".formatted(AWS_REGION))
                .put("s3.aws-access-key", AWS_ACCESS_KEY)
                .put("s3.aws-secret-key", AWS_SECRET_KEY)
                .put("s3.path-style-access", "true")
                .put("join-pushdown.enabled", "true")
                .put("join-pushdown.strategy", "EAGER")
                .buildOrThrow();

        return RedshiftQueryRunner.builder()
                // NOTE this can cause tests to time-out if larger tables like
                //  lineitem and orders need to be re-created.
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .setConnectorProperties(properties)
                .build();
    }

    @Test
    @Override
    public void testCancellation()
    {
        abort("java.util.concurrent.TimeoutException: testCancellation() timed out after 60 seconds");
    }

    @Test
    @Override
    void testJdbcFlow()
    {
        abort("Not applicable for unload test");
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
