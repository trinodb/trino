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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryManager;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.local.LocalFileSystem;
import io.trino.server.BasicQueryInfo;
import io.trino.spi.QueryId;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.nio.file.Files.createTempDirectory;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
final class TestIcebergInsertFinishRetry
        extends AbstractTestQueryFramework
{
    private static final String METADATA_PATH_PROPERTY = "write.metadata.path";

    private final AtomicInteger statisticsWritesToFail = new AtomicInteger();
    private final AtomicInteger failedStatisticsWrites = new AtomicInteger();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Path baseDataDir = createTempDirectory("iceberg_insert_finish_retry");
        Path fileSystemRoot = baseDataDir.resolve("iceberg_data");
        Files.createDirectories(fileSystemRoot);
        File exchangeManagerDirectory = createTempDirectory("exchange_manager").toFile();
        exchangeManagerDirectory.deleteOnExit();

        TrinoFileSystemFactory flakyFileSystemFactory = _ -> flakyFileSystem(new LocalFileSystem(fileSystemRoot));
        return IcebergQueryRunner.builder()
                .setBaseDataDir(Optional.of(baseDataDir))
                .setExtraProperties(ImmutableMap.of(
                        "retry-policy", "TASK",
                        "fault-tolerant-execution-task-memory", "1GB"))
                .addIcebergProperty("iceberg.allowed-extra-properties", METADATA_PATH_PROPERTY)
                .setAdditionalOverrideModule(binder -> newMapBinder(binder, String.class, TrinoFileSystemFactory.class)
                        .addBinding("flaky")
                        .toInstance(flakyFileSystemFactory))
                .withExchange("filesystem", ImmutableMap.of("exchange.base-directories", exchangeManagerDirectory.getAbsolutePath()))
                .build();
    }

    @Test
    void testInsertFinishRetryDoesNotAppendRowsTwice()
            throws Exception
    {
        String tableName = "test_finish_retry_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (x integer) WITH (extra_properties = MAP(ARRAY['" + METADATA_PATH_PROPERTY + "'], ARRAY['flaky:///" + tableName + "']))");

        statisticsWritesToFail.set(1);

        String insert = "INSERT INTO " + tableName + " VALUES 1, 2, 3";
        ExecutorService executor = newSingleThreadExecutor();
        try {
            Future<?> insertFuture = executor.submit(() -> getQueryRunner().execute(insert));
            QueryId queryId = waitForQueryId(insert);
            insertFuture.get(120, SECONDS);

            QueryInfo queryInfo = queryManager().getFullQueryInfo(queryId);
            assertThat(failedStatisticsWrites.get()).isEqualTo(1);
            assertThat(queryInfo.getQueryStats().getFailedTasks()).isGreaterThanOrEqualTo(1);
            assertThat((long) computeScalar("SELECT count(*) FROM " + tableName)).isEqualTo(3);
        }
        finally {
            executor.shutdownNow();
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    private QueryId waitForQueryId(String sql)
            throws InterruptedException
    {
        for (int i = 0; i < 600; i++) {
            List<BasicQueryInfo> queries = queryManager().getQueries().stream()
                    .filter(query -> sql.equals(query.getQuery()))
                    .toList();
            if (!queries.isEmpty()) {
                return queries.getFirst().getQueryId();
            }
            Thread.sleep(100);
        }
        throw new AssertionError("query did not start: " + sql);
    }

    private QueryManager queryManager()
    {
        return getDistributedQueryRunner().getCoordinator().getQueryManager();
    }

    private TrinoFileSystem flakyFileSystem(TrinoFileSystem local)
    {
        return (TrinoFileSystem) Proxy.newProxyInstance(
                TrinoFileSystem.class.getClassLoader(),
                new Class<?>[] {TrinoFileSystem.class},
                (_, method, args) -> {
                    Object[] localArgs = args == null ? null : args.clone();
                    if (localArgs != null) {
                        for (int i = 0; i < localArgs.length; i++) {
                            localArgs[i] = toLocal(localArgs[i]);
                        }
                    }
                    if (method.getName().equals("newOutputFile") && args[0] instanceof Location location && location.path().endsWith(".stats")) {
                        if (statisticsWritesToFail.getAndUpdate(remaining -> Math.max(0, remaining - 1)) > 0) {
                            failedStatisticsWrites.incrementAndGet();
                            return failingOutputFile(location);
                        }
                    }
                    return invoke(method, local, localArgs);
                });
    }

    private static Object toLocal(Object argument)
    {
        if (argument instanceof Location location) {
            return location.scheme().filter("flaky"::equals).isPresent()
                    ? Location.of("local://" + location.toString().substring("flaky://".length()))
                    : location;
        }
        if (argument instanceof Collection<?> collection) {
            return collection.stream().map(TestIcebergInsertFinishRetry::toLocal).toList();
        }
        return argument;
    }

    private static TrinoOutputFile failingOutputFile(Location location)
    {
        return (TrinoOutputFile) Proxy.newProxyInstance(
                TrinoOutputFile.class.getClassLoader(),
                new Class<?>[] {TrinoOutputFile.class},
                (_, method, _) -> {
                    if (method.getName().equals("location")) {
                        return location;
                    }
                    throw new IOException("simulated statistics file write failure: " + location);
                });
    }

    private static Object invoke(Method method, Object target, Object[] args)
            throws Throwable
    {
        try {
            return method.invoke(target, args);
        }
        catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }
}
