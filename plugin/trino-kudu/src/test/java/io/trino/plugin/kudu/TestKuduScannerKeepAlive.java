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
package io.trino.plugin.kudu;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Span;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.Split;
import io.trino.metadata.TableHandle;
import io.trino.plugin.base.mapping.DefaultIdentifierMapping;
import io.trino.plugin.kudu.schema.NoSchemaEmulation;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.split.SplitSource;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.transaction.TransactionId;
import io.trino.transaction.TransactionManager;
import org.apache.kudu.client.KuduClient;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.plugin.kudu.KuduQueryRunnerFactory.createKuduQueryRunnerTpch;
import static io.trino.plugin.kudu.TestingKuduServer.LATEST_TAG;
import static io.trino.spi.connector.Constraint.alwaysTrue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@ExtendWith(ProgressLoggingWatcher.class)
public class TestKuduScannerKeepAlive
        extends AbstractTestQueryFramework
{
    private HostAndPort masterAddress;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingKuduServer kuduServer = closeAfterClass(new TestingKuduServer(LATEST_TAG, ImmutableList.of("--scanner_ttl_ms=10000", "--scanner_gc_check_interval_us=1000000")));
        this.masterAddress = kuduServer.getMasterAddress();
        QueryRunner queryRunner = createQueryRunner(ImmutableMap.of(
                "kudu.scanner.batch-size", "128kB",
                "kudu.scanner.keepalive-interval", "5s"));

        queryRunner.execute("DROP TABLE IF EXISTS test_scanner_keep_alive");
        queryRunner.execute("CREATE TABLE test_scanner_keep_alive " + getCreateTableDefaultDefinition());
        String largeColumnValue = "a".repeat(65536);
        // Produce many rows so that one scan should have multiple batches.
        for (int i = 0; i < 500; i++) {
            queryRunner.execute("INSERT INTO test_scanner_keep_alive VALUES (%d, '%s')".formatted(i, largeColumnValue));
        }

        return queryRunner;
    }

    @AfterAll
    public void tearDown()
    {
        getQueryRunner().execute("DROP TABLE test_scanner_keep_alive");
    }

    @Test
    public void testScannerKeepAliveLessThanScannerTtlMs()
            throws Exception
    {
        // Sleep < scannerKeepAliveInterval < scanner_ttl_ms
        assertScannerKeepAliveSuccess(getQueryRunner(), Duration.valueOf("5s"), 2);
        // scannerKeepAliveInterval < Sleep < scanner_ttl_ms
        assertScannerKeepAliveSuccess(getQueryRunner(), Duration.valueOf("5s"), 8);
        // scannerKeepAliveInterval < scanner_ttl_ms < Sleep
        assertScannerKeepAliveSuccess(getQueryRunner(), Duration.valueOf("5s"), 14);
    }

    @Test
    public void testScannerKeepAliveGreaterThanScannerTtlMs()
            throws Exception
    {
        QueryRunner queryRunner = createQueryRunner(ImmutableMap.of(
                "kudu.scanner.batch-size", "128kB",
                "kudu.scanner.keepalive-interval", "15s"));

        // Sleep < scanner_ttl_ms < scannerKeepAliveInterval
        assertScannerKeepAliveSuccess(queryRunner, Duration.valueOf("15s"), 7);
        // scanner_ttl_ms < Sleep < scannerKeepAliveInterval
        assertScannerKeepAliveSuccess(queryRunner, Duration.valueOf("15s"), 12);
        // scanner_ttl_ms < scannerKeepAliveInterval < Sleep
        assertScannerKeepAliveFailure(queryRunner, Duration.valueOf("15s"), 20);
    }

    @Test
    public void testScannerKeepAliveLessThanScannerTtlMsConcurrently()
    {
        runTaskConcurrently(() -> {
            try {
                testScannerKeepAliveLessThanScannerTtlMs();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testScannerKeepAliveGreaterThanScannerTtlMsConcurrently()
    {
        runTaskConcurrently(() -> {
            try {
                testScannerKeepAliveGreaterThanScannerTtlMs();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testScannerKeepAliveLessThanScannerTtlMsConcurrentlyWithSameKuduSession()
    {
        Duration scannerKeepAliveInterval = Duration.valueOf("5s");
        KuduClientSession kuduClientSession = createKuduClientSession(scannerKeepAliveInterval);

        runTaskConcurrently(() -> {
            try {
                // Sleep < scannerKeepAliveInterval < scanner_ttl_ms
                assertScannerKeepAliveSuccess(getQueryRunner(), kuduClientSession, scannerKeepAliveInterval, 2);
                // scannerKeepAliveInterval < Sleep < scanner_ttl_ms
                assertScannerKeepAliveSuccess(getQueryRunner(), kuduClientSession, scannerKeepAliveInterval, 8);
                // scannerKeepAliveInterval < scanner_ttl_ms < Sleep
                assertScannerKeepAliveSuccess(getQueryRunner(), kuduClientSession, scannerKeepAliveInterval, 14);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testScannerKeepAliveGreaterThanScannerTtlMsConcurrentlyWithSameKuduSession()
            throws Exception
    {
        QueryRunner queryRunner = createQueryRunner(ImmutableMap.of(
                "kudu.scanner.batch-size", "128kB",
                "kudu.scanner.keepalive-interval", "15s"));
        Duration scannerKeepAliveInterval = Duration.valueOf("15s");
        KuduClientSession kuduClientSession = createKuduClientSession(Duration.valueOf("15s"));

        runTaskConcurrently(() -> {
            try {
                // Sleep < scanner_ttl_ms < scannerKeepAliveInterval
                assertScannerKeepAliveSuccess(queryRunner, kuduClientSession, scannerKeepAliveInterval, 7);
                // scanner_ttl_ms < Sleep < scannerKeepAliveInterval
                assertScannerKeepAliveSuccess(queryRunner, kuduClientSession, scannerKeepAliveInterval, 12);
                // scanner_ttl_ms < scannerKeepAliveInterval < Sleep
                assertScannerKeepAliveFailure(queryRunner, kuduClientSession, scannerKeepAliveInterval, 20);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void runTaskConcurrently(Runnable task)
    {
        int threads = 5;
        ExecutorService executor = newFixedThreadPool(threads);
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < threads; i++) {
            Future<?> future = executor.submit(task);
            futures.add(future);
        }

        for (Future<?> future : futures) {
            try {
                future.get();
            }
            catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        executor.shutdown();
    }

    private RecordSet getRecordSetForScannerKeepAliveTable(QueryRunner queryRunner, KuduClientSession kuduClientSession, Duration scannerKeepAliveInterval)
    {
        TransactionManager transactionManager = queryRunner.getTransactionManager();
        TransactionId transactionId = transactionManager.beginTransaction(false);
        Session session = Session.builder(queryRunner.getDefaultSession())
                .build()
                .beginTransactionId(transactionId, transactionManager, new AllowAllAccessControl());

        QualifiedObjectName qualifiedTableName = new QualifiedObjectName(session.getCatalog().orElseThrow(), session.getSchema().orElseThrow(), "test_scanner_keep_alive");
        Optional<TableHandle> tableHandle = queryRunner.getMetadata().getTableHandle(session, qualifiedTableName);
        assertTrue(tableHandle.isPresent());
        SplitSource splitSource = queryRunner.getSplitManager()
                .getSplits(session, Span.getInvalid(), tableHandle.get(), DynamicFilter.EMPTY, alwaysTrue());
        List<Split> splits = new ArrayList<>();
        while (!splitSource.isFinished()) {
            splits.addAll(getFutureValue(splitSource.getNextBatch(1000)).getSplits());
        }

        KuduClientConfig config = new KuduClientConfig()
                .setMasterAddresses(masterAddress.toString())
                .setScannerKeepAliveInterval(scannerKeepAliveInterval);

        return new KuduRecordSet(
                session.toConnectorSession(),
                kuduClientSession,
                (KuduSplit) splits.get(0).getConnectorSplit(),
                ImmutableList.of(
                        new KuduColumnHandle("key", 0, BIGINT),
                        new KuduColumnHandle("name", 1, VARCHAR)),
                new KuduScannerAliveKeeper(config));
    }

    private void assertScannerKeepAliveSuccess(QueryRunner queryRunner, Duration scannerKeepAliveInterval, long sleep)
            throws InterruptedException
    {
        assertScannerKeepAliveSuccess(queryRunner, createKuduClientSession(scannerKeepAliveInterval), scannerKeepAliveInterval, sleep);
    }

    private void assertScannerKeepAliveSuccess(QueryRunner queryRunner, KuduClientSession kuduClientSession, Duration scannerKeepAliveInterval, long sleep)
            throws InterruptedException
    {
        try (RecordCursor cursor = getRecordSetForScannerKeepAliveTable(queryRunner, kuduClientSession, scannerKeepAliveInterval).cursor()) {
            cursor.advanceNextPosition();

            // simulation of slow operation on result rows
            TimeUnit.SECONDS.sleep(sleep);

            cursor.advanceNextPosition();
            assertEquals(cursor.getObject(1), "a".repeat(65536));
        }
    }

    private void assertScannerKeepAliveFailure(QueryRunner queryRunner, Duration scannerKeepAliveInterval, long sleep)
            throws InterruptedException
    {
        assertScannerKeepAliveFailure(queryRunner, createKuduClientSession(scannerKeepAliveInterval), scannerKeepAliveInterval, sleep);
    }

    private void assertScannerKeepAliveFailure(QueryRunner queryRunner, KuduClientSession kuduClientSession, Duration scannerKeepAliveInterval, long sleep)
            throws InterruptedException
    {
        try (RecordCursor cursor = getRecordSetForScannerKeepAliveTable(queryRunner, kuduClientSession, scannerKeepAliveInterval).cursor()) {
            cursor.advanceNextPosition();

            // simulation of slow operation on result rows
            TimeUnit.SECONDS.sleep(sleep);

            Assertions.assertThatThrownBy(cursor::advanceNextPosition)
                    .hasMessageMatching("org\\.apache\\.kudu\\.client\\.NonRecoverableException: Scanner (.*) not found \\(it may have expired\\)");
        }
    }

    private String getCreateTableDefaultDefinition()
    {
        return "(key BIGINT WITH (primary_key=true), name VARCHAR) " +
                "WITH (partition_by_hash_columns = ARRAY['key'], partition_by_hash_buckets = 2, number_of_replicas = 1)";
    }

    private QueryRunner createQueryRunner(ImmutableMap<String, String> kuduExtraProperties)
            throws Exception
    {
        return createKuduQueryRunnerTpch(
                masterAddress,
                Optional.empty(),
                ImmutableMap.of(),
                kuduExtraProperties,
                ImmutableMap.of(),
                ImmutableList.of());
    }

    private KuduClientSession createKuduClientSession(Duration scannerKeepAliveInterval)
    {
        KuduClient.KuduClientBuilder builder = new KuduClient.KuduClientBuilder(ImmutableList.of(masterAddress.toString()));
        builder.defaultAdminOperationTimeoutMs(30000);
        builder.defaultOperationTimeoutMs(30000);
        KuduClientWrapper client = new PassthroughKuduClient(builder.build());
        return new KuduClientSession(
                client,
                new NoSchemaEmulation(),
                true,
                DataSize.ofBytes(128),
                scannerKeepAliveInterval,
                Duration.valueOf("30s"),
                new DefaultIdentifierMapping());
    }
}
