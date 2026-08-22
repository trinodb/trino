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
import io.trino.Session;
import io.trino.operator.OperatorInfo;
import io.trino.plugin.jdbc.RemoteDatabaseEvent.Status;
import io.trino.plugin.jdbc.RemoteLogTracingEvent;
import io.trino.spi.QueryId;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TestView;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.trino.plugin.jdbc.BaseJdbcConnectorTest.getQueryId;
import static io.trino.plugin.redshift.RedshiftQueryRunner.IAM_ROLE;
import static io.trino.plugin.redshift.TestingRedshiftServer.TEST_SCHEMA;
import static io.trino.plugin.redshift.TestingRedshiftServer.createSleepingView;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingProperties.requiredNonEmptySystemProperty;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.Assert.assertEventually;
import static io.trino.tpch.TpchTable.NATION;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestRedshiftUnload
        extends AbstractTestQueryFramework
{
    private static final String S3_UNLOAD_ROOT = requiredNonEmptySystemProperty("test.redshift.s3.unload.root");
    private static final String AWS_REGION = requiredNonEmptySystemProperty("test.redshift.aws.region");
    private static final String AWS_ACCESS_KEY = requiredNonEmptySystemProperty("test.redshift.aws.access-key");
    private static final String AWS_SECRET_KEY = requiredNonEmptySystemProperty("test.redshift.aws.secret-key");

    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getName() + "-%s"));
    private final RedshiftRemoteDatabaseEventMonitor remoteDatabaseEventMonitor = new RedshiftRemoteDatabaseEventMonitor("UNLOAD");

    @AfterAll
    void tearDown()
    {
        executor.shutdownNow();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return RedshiftQueryRunner.builder()
                .setConnectorProperties(
                        Map.of(
                                "redshift.unload-location", S3_UNLOAD_ROOT,
                                "redshift.unload-iam-role", IAM_ROLE,
                                "s3.region", AWS_REGION,
                                "s3.aws-access-key", AWS_ACCESS_KEY,
                                "s3.aws-secret-key", AWS_SECRET_KEY))
                .setInitialTables(List.of(NATION))
                .build();
    }

    @Test
    void testUnloadEnabled()
    {
        assertQuery(
                "SHOW SESSION LIKE 'redshift.unload_enabled'",
                "VALUES ('redshift.unload_enabled', 'true', 'true', 'boolean', 'Use UNLOAD for reading query results')");
    }

    @Test
    void testUnload()
    {
        assertQueryStats(
                getSession(),
                """
                SELECT nationkey, name FROM nation WHERE regionkey = 0
                UNION
                SELECT nationkey, name FROM nation WHERE regionkey = 1
                """,
                queryStats -> {
                    List<String> unloadedPaths =
                            queryStats.getOperatorSummaries()
                                    .stream()
                                    .filter(summary -> summary.getOperatorType().startsWith("TableScanOperator"))
                                    .map(operatorStat -> operatorStat.getConnectorMetrics().getMetrics())
                                    .flatMap(metrics -> metrics.keySet().stream())
                                    .filter(key -> !key.startsWith("ParquetReader"))
                                    .collect(toImmutableList());
                    unloadedPaths.forEach(path -> assertThat(path).matches("%s/.*/.*/.*.parquet.*".formatted(S3_UNLOAD_ROOT)));
                    String unloadedFilePath = unloadedPaths.getFirst();
                    assertThat(unloadedFilePath).matches("%s/.*/.*/.*.parquet.*".formatted(S3_UNLOAD_ROOT));
                    try (S3Client s3 = S3Client.builder()
                            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(AWS_ACCESS_KEY, AWS_SECRET_KEY)))
                            .region(Region.of(AWS_REGION))
                            .build()) {
                        URI s3Path = URI.create(unloadedFilePath.substring(0, unloadedFilePath.lastIndexOf("/", unloadedFilePath.lastIndexOf("/") - 1)));
                        assertThat(s3.listObjectsV2(request -> request.bucket(s3Path.getHost()).prefix(s3Path.getPath().substring(1))).contents()).isEmpty();
                    }
                },
                results -> assertThat(results.getRowCount()).isEqualTo(10));
    }

    @Test
    void testUnloadDisabled()
    {
        Session unloadDisabledSession = testSessionBuilder(getSession())
                .setCatalogSessionProperty("redshift", "unload_enabled", "false")
                .build();
        assertQueryStats(
                unloadDisabledSession,
                "SELECT nationkey, name FROM nation WHERE regionkey = 0",
                queryStats -> {
                    OperatorInfo operatorInfo = queryStats.getOperatorSummaries()
                            .stream()
                            .filter(summary -> summary.getOperatorType().startsWith("TableScanOperator"))
                            .collect(onlyElement())
                            .getInfo();
                    assertThat(operatorInfo).isNull();
                },
                results -> assertThat(results.getRowCount()).isEqualTo(5));
    }

    @Test
    void testUnloadProduceEmptyResults()
    {
        assertQueryStats(
                getSession(),
                "SELECT * FROM nation WHERE name = 'INVALID'",
                queryStats -> {
                    OperatorInfo operatorInfo = queryStats.getOperatorSummaries()
                            .stream()
                            .filter(summary -> summary.getOperatorType().startsWith("TableScanOperator"))
                            .collect(onlyElement())
                            .getInfo();
                    assertThat(operatorInfo).isNull();
                },
                results -> assertThat(results.getRowCount()).isEqualTo(0));
    }

    @Test
    void testUnloadWithDynamicFilter()
    {
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty("redshift", "join_pushdown_enabled", "false")
                .build();

        // Dynamic filter applied to the UNLOAD (probe) side of a join must not fail when attaching the predicate to the split
        String query = "SELECT n.name FROM nation n JOIN nation r ON n.nationkey = r.regionkey WHERE r.name = 'ALGERIA'";
        assertQuery(session, query, "VALUES ('ALGERIA')");

        // The dynamic filter must actually reduce the rows Redshift unloads from the probe side
        Session dynamicFilteringDisabled = Session.builder(session)
                .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "false")
                .build();
        assertThat(physicalInputPositions(session, query))
                .isLessThan(physicalInputPositions(dynamicFilteringDisabled, query));
    }

    private long physicalInputPositions(Session session, String query)
    {
        QueryRunner queryRunner = getDistributedQueryRunner();
        MaterializedResultWithPlan result = queryRunner.executeWithPlan(session, query);
        return queryRunner.getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(result.queryId())
                .getQueryStats()
                .getPhysicalInputPositions();
    }

    @Test
    void testUnloadFallbackToJdbc()
    {
        // Fallback to JDBC as limit clause is not supported by UNLOAD
        assertQueryStats(
                getSession(),
                "SELECT nationkey, name FROM nation WHERE regionkey = 0 LIMIT 1",
                queryStats -> {
                    OperatorInfo operatorInfo = queryStats.getOperatorSummaries()
                            .stream()
                            .filter(summary -> summary.getOperatorType().startsWith("TableScanOperator"))
                            .collect(onlyElement())
                            .getInfo();
                    assertThat(operatorInfo).isNull();
                },
                results -> assertThat(results.getRowCount()).isEqualTo(1));
    }

    @RepeatedTest(100)
    void testUnloadCancellation()
            throws Exception
    {
        try (TestView sleepingView = createSleepingView(MINUTES.toSeconds(1) + 1)) {
            String viewName = sleepingView.getName().toLowerCase(ENGLISH);

            RemoteLogTracingEvent runningEvent = new RemoteLogTracingEvent(event -> event.getQuery().toLowerCase(ENGLISH).contains(viewName) && event.getStatus() == Status.RUNNING);
            remoteDatabaseEventMonitor.startTracingDatabaseEvent(runningEvent);

            String query = "SELECT * FROM " + sleepingView.getName();
            Future<?> future = executor.submit(() -> assertQueryFails(query, "Query killed. Message: Killed by test"));
            QueryId queryId = getQueryId(getQueryRunner(), query);
            assertEventually(() -> assertThat(runningEvent.hasHappened()).isTrue());
            remoteDatabaseEventMonitor.stopTracingDatabaseEvent(runningEvent);

            RemoteLogTracingEvent cancelledEvent = new RemoteLogTracingEvent(event -> event.getQuery().toLowerCase(ENGLISH).contains(viewName) && event.getStatus() == Status.CANCELLED);
            remoteDatabaseEventMonitor.startTracingDatabaseEvent(cancelledEvent);
            assertUpdate(format("CALL system.runtime.kill_query(query_id => '%s', message => '%s')", queryId, "Killed by test"));
            future.get();
            assertEventually(() -> assertThat(cancelledEvent.hasHappened()).isTrue());
            remoteDatabaseEventMonitor.stopTracingDatabaseEvent(cancelledEvent);
        }
    }

    @Test
    void testColumnName()
    {
        List<String> columnNames = ImmutableList.<String>builder()
                .add("lowercase")
                .add("UPPERCASE")
                .add("MixedCase")
                .add("an_underscore")
                .add("a-hyphen-minus") // ASCII '-' is HYPHEN-MINUS in Unicode
                .add("a space")
                .add("atrailingspace ")
                .add(" aleadingspace")
                .add("a.dot")
                .add("a,comma")
                .add("a:colon")
                .add("a;semicolon")
                .add("an@at")
                // .add("a\"quote") // TODO escape "(double quotes) in UNLOAD manifest(manifest json contains unescaped double quotes in field value `"name": "a"quote"`)
                .add("an'apostrophe")
                .add("a`backtick`")
                .add("a/slash`")
                .add("a\\backslash`")
                .add("adigit0")
                .add("0startwithdigit")
                .add("カラム")
                .build();
        for (String columnName : columnNames) {
            testColumnName(columnName, requiresDelimiting(columnName));
        }
    }

    private void testColumnName(String columnName, boolean delimited)
    {
        String nameInSql = toColumnNameInSql(columnName, delimited);
        String tableNamePrefix = "tcn_" + nameInSql.toLowerCase(ENGLISH).replaceAll("[^a-z0-9]", "") + randomNameSuffix();

        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                TEST_SCHEMA + "." + tableNamePrefix,
                "(%s varchar(50))".formatted(nameInSql),
                ImmutableList.of("'abc'"))) {
            assertQuery("SELECT " + nameInSql + " FROM " + table.getName(), "VALUES ('abc')");
        }
    }

    private static String toColumnNameInSql(String columnName, boolean delimited)
    {
        String nameInSql = columnName;
        if (delimited) {
            nameInSql = "\"" + columnName.replace("\"", "\"\"") + "\"";
        }
        return nameInSql;
    }

    private static boolean requiresDelimiting(String identifierName)
    {
        return !identifierName.matches("[a-zA-Z][a-zA-Z0-9_]*");
    }

    private static SqlExecutor onRemoteDatabase()
    {
        return TestingRedshiftServer::executeInRedshiftWithRetry;
    }
}
