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
package io.trino.plugin.paimon;

import com.google.common.collect.ImmutableMap;
import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.Result;
import io.minio.messages.Item;
import io.trino.Session;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryStats;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import io.trino.testing.containers.Minio;
import org.apache.paimon.CoreOptions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_ROOT_PASSWORD;
import static io.trino.testing.containers.Minio.MINIO_ROOT_USER;
import static java.lang.Math.max;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Explicit native-S3 capacity benchmark; excluded from ordinary connector test runs by its name.
 */
public class BenchmarkPaimonNativeS3Capacity
{
    private static final String CATALOG = "paimon";
    private static final String SCHEMA = "capacity_benchmark";
    private static final String ENABLED = "PAIMON_NATIVE_S3_CAPACITY_BENCHMARK";
    private static final Pattern METRIC = Pattern.compile("^minio_.*(?:request|requests).*\\s+([0-9]+(?:\\.[0-9]+)?)$");

    @Test
    public void benchmarkNativeS3Capacity()
            throws Exception
    {
        assumeTrue(Boolean.parseBoolean(System.getenv().getOrDefault(ENABLED, "false")),
                "Set " + ENABLED + "=true to run the native S3 capacity benchmark");

        String bucketName = "test-paimon-capacity-" + randomNameSuffix();
        Path spillPath = Files.createTempDirectory("paimon-capacity-spill");
        try (Minio minio = Minio.builder()
                .withEnvVars(ImmutableMap.of(
                        "MINIO_ACCESS_KEY", MINIO_ROOT_USER,
                        "MINIO_SECRET_KEY", MINIO_ROOT_PASSWORD,
                        "MINIO_PROMETHEUS_AUTH_TYPE", "public"))
                .build();
                PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
                        .withDatabaseName("paimon")
                        .withUsername("paimon")
                        .withPassword("paimon")) {
            minio.start();
            minio.createBucket(bucketName);
            awaitMinioReady(minio, bucketName);
            postgres.start();

            for (int rowCount : List.of(10_000, 100_000)) {
                for (int assignerParallelism : List.of(1, 2)) {
                    runCase(minio, postgres, bucketName, spillPath, rowCount, assignerParallelism);
                }
            }
        }
        finally {
            deleteRecursively(spillPath);
        }
    }

    private static void runCase(
            Minio minio,
            PostgreSQLContainer<?> postgres,
            String bucketName,
            Path spillPath,
            int rowCount,
            int assignerParallelism)
            throws Exception
    {
        String table = "key_dynamic_" + rowCount + "_" + assignerParallelism + "_" + randomNameSuffix();
        String qualifiedSchema = CATALOG + "." + SCHEMA;
        String qualifiedTable = qualifiedSchema + "." + table;
        Map<String, String> catalogProperties = catalogProperties(minio, postgres, bucketName, spillPath);
        Session session = testSessionBuilder()
                .setCatalog(CATALOG)
                .setSchema(SCHEMA)
                .build();

        long beforeRequests = requestCounter(minio.getMinioAddress());
        try (DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setWorkerCount(assignerParallelism + 1)
                .build()) {
            queryRunner.installPlugin(new PaimonPlugin());
            queryRunner.createCatalog(CATALOG, CATALOG, catalogProperties);
            queryRunner.execute("CREATE SCHEMA " + qualifiedSchema);
            try {
                queryRunner.execute("CREATE TABLE " + qualifiedTable + " ("
                        + "dt integer, id integer, value varchar) WITH ("
                        + "partitioned_by = ARRAY['dt'], "
                        + "primary_key = ARRAY['id'], "
                        + "bucket = '-1', "
                        + "dynamic_bucket_assigner_parallelism = '" + assignerParallelism + "', "
                        + "dynamic_bucket_initial_buckets = '" + max(2, assignerParallelism) + "', "
                        + "cross_partition_upsert_bootstrap_parallelism = '" + assignerParallelism + "', "
                        + "write_buffer_for_append = 'true', "
                        + "write_buffer_spillable = 'true', "
                        + "write_max_writers_to_spill = '1')");

                MaterializedResultWithPlan initial = queryRunner.executeWithPlan(session,
                        "INSERT INTO " + qualifiedTable
                                + " " + sequenceRows(1, rowCount,
                                "CAST(n % 32 AS INTEGER), CAST(n AS INTEGER), "
                                        + "CAST('initial-' || CAST(n AS VARCHAR) AS VARCHAR)"));
                assertThat(initial.result().getUpdateCount()).hasValue(rowCount);

                ObjectMetrics beforeObjects = objectMetrics(minio, bucketName, "warehouse/" + SCHEMA + ".db/" + table);
                long spillBefore = directoryBytes(spillPath);
                long startNanos = System.nanoTime();

                String updateSql = "INSERT INTO " + qualifiedTable + " "
                        + sequenceRows(1, rowCount / 10,
                        "CAST((n + 7) % 64 AS INTEGER), CAST(n AS INTEGER), "
                                + "CAST('updated-' || CAST(n AS VARCHAR) AS VARCHAR)")
                        + " UNION ALL "
                        + sequenceRows(rowCount + 1, rowCount + rowCount / 10,
                        "CAST((n + 7) % 64 AS INTEGER), CAST(n AS INTEGER), "
                                + "CAST('new-' || CAST(n AS VARCHAR) AS VARCHAR)");
                ObjectMetrics peakObjects = beforeObjects;
                ExecutorService executor = Executors.newSingleThreadExecutor();
                Future<MaterializedResultWithPlan> updateFuture = executor.submit(
                        () -> queryRunner.executeWithPlan(session, updateSql));
                while (!updateFuture.isDone()) {
                    peakObjects = peakObjects.maxBootstrap(objectMetrics(
                            minio, bucketName, "warehouse/" + SCHEMA + ".db/" + table));
                    Thread.sleep(100);
                }
                MaterializedResultWithPlan update;
                try {
                    update = updateFuture.get();
                }
                finally {
                    executor.shutdownNow();
                    executor.awaitTermination(10, TimeUnit.SECONDS);
                }
                long elapsedNanos = System.nanoTime() - startNanos;
                assertThat(update.result().getUpdateCount()).hasValue(rowCount / 5);
                assertThat(queryRunner.execute("SELECT count(*) FROM " + qualifiedTable).getOnlyValue())
                        .isEqualTo((long) rowCount + rowCount / 10);
                assertThat(queryRunner.execute("SELECT count(DISTINCT id) FROM " + qualifiedTable).getOnlyValue())
                        .isEqualTo((long) rowCount + rowCount / 10);

                QueryInfo queryInfo = queryRunner.getCoordinator().getQueryManager().getFullQueryInfo(update.queryId());
                QueryStats queryStats = queryInfo.getQueryStats();
                ObjectMetrics afterObjects = objectMetrics(minio, bucketName, "warehouse/" + SCHEMA + ".db/" + table);
                long spillAfter = directoryBytes(spillPath);
                long afterRequests = requestCounter(minio.getMinioAddress());
                long throughputRows = rowCount / 5;
                long elapsedMillis = Duration.ofNanos(elapsedNanos).toMillis();
                long throughput = elapsedMillis == 0 ? 0 : throughputRows * 1000 / elapsedMillis;
                long nativeReservation = keyDynamicMemoryReservation();
                System.out.printf(
                        Locale.ROOT,
                        "PAIMON_CAPACITY_RESULT rows=%d assigners=%d source_snapshot_bytes=%d source_snapshot_files=%d "
                                + "bootstrap_bytes=%d bootstrap_files=%d object_bytes=%d object_files=%d "
                                + "spill_bytes=%d query_spill_bytes=%d peak_task_memory=%d native_reservation=%d "
                                + "raw_input_bytes=%d physical_written_bytes=%d requests=%d elapsed_ms=%d throughput_rows_per_sec=%d%n",
                        rowCount,
                        assignerParallelism,
                        beforeObjects.totalBytes(),
                        beforeObjects.totalFiles(),
                        peakObjects.bootstrapBytes(),
                        peakObjects.bootstrapFiles(),
                        afterObjects.totalBytes() - beforeObjects.totalBytes(),
                        afterObjects.totalFiles() - beforeObjects.totalFiles(),
                        Math.max(0, spillAfter - spillBefore),
                        queryStats.getSpilledDataSize().toBytes(),
                        queryStats.getPeakTaskTotalMemory().toBytes(),
                        nativeReservation,
                        queryStats.getPhysicalInputDataSize().toBytes(),
                        queryStats.getPhysicalWrittenDataSize().toBytes(),
                        afterRequests - beforeRequests,
                        elapsedMillis,
                        throughput);

                verifyConcurrentSameKeyWrites(
                        queryRunner, session, catalogProperties, qualifiedTable, assignerParallelism);
            }
            finally {
                queryRunner.execute("DROP TABLE IF EXISTS " + qualifiedTable);
                queryRunner.execute("DROP SCHEMA IF EXISTS " + qualifiedSchema);
            }
        }
    }

    private static void verifyConcurrentSameKeyWrites(
            DistributedQueryRunner firstCoordinator,
            Session session,
            Map<String, String> catalogProperties,
            String qualifiedTable,
            int assignerParallelism)
            throws Exception
    {
        PaimonKeyDynamicBootstrap.ValidationScanMetrics validationBefore =
                PaimonKeyDynamicBootstrap.validationScanMetrics();
        try (DistributedQueryRunner secondCoordinator = DistributedQueryRunner.builder(session)
                .setWorkerCount(assignerParallelism + 1)
                .build()) {
            secondCoordinator.installPlugin(new PaimonPlugin());
            secondCoordinator.createCatalog(CATALOG, CATALOG, catalogProperties);

            CountDownLatch ready = new CountDownLatch(2);
            CountDownLatch start = new CountDownLatch(1);
            ExecutorService executor = Executors.newFixedThreadPool(2);
            Future<ConcurrentWriteResult> first = executor.submit(() -> concurrentWrite(
                    firstCoordinator, qualifiedTable, 41, "coordinator-1", ready, start));
            Future<ConcurrentWriteResult> second = executor.submit(() -> concurrentWrite(
                    secondCoordinator, qualifiedTable, 42, "coordinator-2", ready, start));
            try {
                assertThat(ready.await(30, TimeUnit.SECONDS)).isTrue();
                start.countDown();
                ConcurrentWriteResult firstResult = first.get(2, TimeUnit.MINUTES);
                ConcurrentWriteResult secondResult = second.get(2, TimeUnit.MINUTES);

                assertThat(List.of(firstResult, secondResult))
                        .anyMatch(ConcurrentWriteResult::success);
                assertThat(List.of(firstResult, secondResult))
                        .allMatch(result -> result.success() || result.sameKeyConflict());
                assertThat(firstCoordinator.execute(
                                "SELECT count(*) FROM " + qualifiedTable + " WHERE id = 2000000001")
                        .getOnlyValue())
                        .isEqualTo(1L);
                assertThat(firstCoordinator.execute(
                                "SELECT count(DISTINCT id) FROM " + qualifiedTable + " WHERE id = 2000000001")
                        .getOnlyValue())
                        .isEqualTo(1L);

                PaimonKeyDynamicBootstrap.ValidationScanMetrics validationAfter =
                        PaimonKeyDynamicBootstrap.validationScanMetrics();

                System.out.printf(
                        Locale.ROOT,
                        "PAIMON_CONCURRENT_RESULT first=%s second=%s final_rows=1 "
                                + "validation_scan_bytes=%d validation_scan_files=%d validation_elapsed_ms=%d%n",
                        firstResult.outcome(),
                        secondResult.outcome(),
                        validationAfter.bytes() - validationBefore.bytes(),
                        validationAfter.files() - validationBefore.files(),
                        Duration.ofNanos(validationAfter.nanos() - validationBefore.nanos()).toMillis());
            }
            finally {
                start.countDown();
                executor.shutdownNow();
                executor.awaitTermination(10, TimeUnit.SECONDS);
            }
        }
    }

    private static ConcurrentWriteResult concurrentWrite(
            DistributedQueryRunner coordinator,
            String qualifiedTable,
            int partition,
            String value,
            CountDownLatch ready,
            CountDownLatch start)
            throws InterruptedException
    {
        ready.countDown();
        if (!start.await(30, TimeUnit.SECONDS)) {
            return new ConcurrentWriteResult(false, false, "start-timeout");
        }
        try {
            coordinator.execute("INSERT INTO " + qualifiedTable + " VALUES ("
                    + partition + ", 2000000001, '" + value + "')");
            return new ConcurrentWriteResult(true, false, "success");
        }
        catch (RuntimeException e) {
            String messages = causalMessages(e);
            return new ConcurrentWriteResult(
                    false,
                    messages.contains("concurrent snapshot contains a primary key written by this query"),
                    messages);
        }
    }

    private static String causalMessages(Throwable failure)
    {
        StringBuilder messages = new StringBuilder();
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause.getMessage() != null) {
                if (!messages.isEmpty()) {
                    messages.append(" | ");
                }
                messages.append(cause.getMessage());
            }
        }
        return messages.toString();
    }

    private static Map<String, String> catalogProperties(
            Minio minio,
            PostgreSQLContainer<?> postgres,
            String bucketName,
            Path spillPath)
    {
        return ImmutableMap.<String, String>builder()
                .put("warehouse", "s3://" + bucketName + "/warehouse")
                .put("metastore", "jdbc")
                .put("uri", postgres.getJdbcUrl())
                .put("jdbc.user", postgres.getUsername())
                .put("jdbc.password", postgres.getPassword())
                .put("catalog-key", "trino-capacity")
                .put("lock.enabled", "true")
                .put("lock.type", "jdbc")
                .put("lock-acquire-timeout", "10 min")
                .put("lock-check-max-sleep", "1 s")
                .put("fs.hadoop.enabled", "false")
                .put("fs.native-s3.enabled", "true")
                .put("s3.endpoint", minio.getMinioAddress())
                .put("s3.access-key", MINIO_ROOT_USER)
                .put("s3.secret-key", MINIO_ROOT_PASSWORD)
                .put("s3.region", MINIO_REGION)
                .put("s3.path-style-access", "true")
                .put("write.spill-path", spillPath.toString())
                .buildOrThrow();
    }

    private static long keyDynamicMemoryReservation()
    {
        return CoreOptions.fromMap(Map.of()).lookupCacheMaxMemory().getBytes()
                + CoreOptions.fromMap(Map.of()).writeBufferSize();
    }

    private static String sequenceRows(int first, int last, String projection)
    {
        List<String> selects = new ArrayList<>();
        for (int start = first; start <= last; start += 10_000) {
            int end = Math.min(last, start + 9_999);
            selects.add("SELECT " + projection + " FROM UNNEST(sequence(" + start + ", " + end + ")) AS t(n)");
        }
        return String.join(" UNION ALL ", selects);
    }

    private static ObjectMetrics objectMetrics(Minio minio, String bucketName, String prefix)
    {
        MinioClient client = MinioClient.builder()
                .endpoint(minio.getMinioAddress())
                .credentials(MINIO_ROOT_USER, MINIO_ROOT_PASSWORD)
                .build();
        try {
            long totalBytes = 0;
            long totalFiles = 0;
            long bootstrapBytes = 0;
            long bootstrapFiles = 0;
            for (Result<Item> result : client.listObjects(
                    ListObjectsArgs.builder().bucket(bucketName).prefix(prefix).recursive(true).build())) {
                Item item = result.get();
                totalBytes += item.size();
                totalFiles++;
                if (item.objectName().contains("/.trino-key-dynamic-bootstrap/")) {
                    bootstrapBytes += item.size();
                    bootstrapFiles++;
                }
            }
            return new ObjectMetrics(totalBytes, totalFiles, bootstrapBytes, bootstrapFiles);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to inspect MinIO objects", e);
        }
    }

    private static long requestCounter(String endpoint)
            throws IOException
    {
        URL url = URI.create(endpoint + "/minio/v2/metrics/cluster").toURL();
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setConnectTimeout(5_000);
        connection.setReadTimeout(5_000);
        connection.setRequestMethod("GET");
        if (connection.getResponseCode() != 200) {
            throw new IOException("MinIO metrics endpoint returned HTTP " + connection.getResponseCode());
        }
        String body = new String(connection.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        long total = 0;
        boolean found = false;
        for (String line : body.split("\\R")) {
            Matcher matcher = METRIC.matcher(line);
            if (matcher.matches()) {
                total += (long) Double.parseDouble(matcher.group(1));
                found = true;
            }
        }
        if (!found) {
            throw new IOException("MinIO metrics endpoint did not expose request counters");
        }
        return total;
    }

    private static void awaitMinioReady(Minio minio, String bucketName)
            throws Exception
    {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        RuntimeException lastFailure = null;
        do {
            try {
                URL url = URI.create(minio.getMinioAddress() + "/minio/health/ready").toURL();
                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.setConnectTimeout(1_000);
                connection.setReadTimeout(1_000);
                if (connection.getResponseCode() == 200) {
                    objectMetrics(minio, bucketName, "");
                    return;
                }
            }
            catch (IOException | RuntimeException e) {
                lastFailure = new RuntimeException("MinIO is not ready", e);
            }
            Thread.sleep(200);
        }
        while (System.nanoTime() < deadline);
        throw new IllegalStateException("MinIO did not become ready within 30 seconds", lastFailure);
    }

    private static long directoryBytes(Path directory)
            throws IOException
    {
        if (!Files.exists(directory)) {
            return 0;
        }
        try (var paths = Files.walk(directory)) {
            return paths.filter(Files::isRegularFile).mapToLong(path -> {
                try {
                    return Files.size(path);
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }).sum();
        }
    }

    private static void deleteRecursively(Path path)
            throws IOException
    {
        if (!Files.exists(path)) {
            return;
        }
        try (var paths = Files.walk(path)) {
            paths.sorted((left, right) -> right.compareTo(left)).forEach(child -> {
                try {
                    Files.deleteIfExists(child);
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    private record ConcurrentWriteResult(boolean success, boolean sameKeyConflict, String outcome) {}

    private record ObjectMetrics(long totalBytes, long totalFiles, long bootstrapBytes, long bootstrapFiles)
    {
        private ObjectMetrics maxBootstrap(ObjectMetrics other)
        {
            return new ObjectMetrics(
                    totalBytes,
                    totalFiles,
                    Math.max(bootstrapBytes, other.bootstrapBytes),
                    Math.max(bootstrapFiles, other.bootstrapFiles));
        }
    }
}
