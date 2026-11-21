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
package io.trino.exchange;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.secrets.SecretsResolver;
import io.airlift.units.Duration;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.dispatcher.DispatchManager;
import io.trino.execution.QueryState;
import io.trino.operator.RetryPolicy;
import io.trino.plugin.exchange.filesystem.FileSystemExchangeManagerFactory;
import io.trino.server.BasicQueryInfo;
import io.trino.server.BasicQueryStats;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.QueryId;
import io.trino.spi.exchange.ExtendedExchangeManager;
import io.trino.spi.resourcegroups.ResourceGroupId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.airlift.tracing.Tracing.noopTracer;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
class TestExchangeOrphanCleanupJob
{
    private static final DateTimeFormatter QUERY_ID_TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")
            .withZone(ZoneOffset.UTC);

    private TestingTrinoServer server;
    private DispatchManager dispatchManager;

    @BeforeAll
    void setUp()
    {
        server = TestingTrinoServer.create();
        dispatchManager = server.getDispatchManager();
    }

    @AfterAll
    void tearDown()
            throws Exception
    {
        if (server != null) {
            server.close();
        }
        server = null;
        dispatchManager = null;
    }

    @Test
    void testCleanupInactiveDirectories(@TempDir Path tempDir)
            throws IOException
    {
        ExchangeManagerRegistry exchangeManagerRegistry = createRegistry(tempDir);

        Instant now = Instant.now();
        String activeQueryId = newQueryId(now.minus(java.time.Duration.ofHours(1)), "00001", "abcde");
        String orphanQueryId = newQueryId(now.minus(java.time.Duration.ofHours(2)), "00002", "abcde");
        String recentQueryId = newQueryId(now.minus(java.time.Duration.ofMinutes(5)), "00003", "abcde");

        Path activeDirectory = createExchangeDirectory(tempDir, "aaaaaa." + activeQueryId + ".exchange-1.0");
        Path orphanDirectory = createExchangeDirectory(tempDir, "bbbbbb." + orphanQueryId + ".exchange-1.0");
        Path recentDirectory = createExchangeDirectory(tempDir, "cccccc." + recentQueryId + ".exchange-1.0");
        Path malformedDirectory = createExchangeDirectory(tempDir, "malformed-directory");

        BasicQueryInfo activeQuery = queryInfo(activeQueryId, QueryState.RUNNING);

        ExchangeOrphanCleanupJob job = new ExchangeOrphanCleanupJob(dispatchManager, exchangeManagerRegistry, cleanupConfig());
        job.cleanup(List.of(activeQuery), extendedExchangeManager(exchangeManagerRegistry));

        assertThat(activeDirectory).exists();
        assertThat(orphanDirectory).doesNotExist();
        assertThat(recentDirectory).exists();
        assertThat(malformedDirectory).exists();
    }

    @Test
    void testCleanupEmptyOrphanDirectory(@TempDir Path tempDir)
            throws IOException
    {
        ExchangeManagerRegistry exchangeManagerRegistry = createRegistry(tempDir);

        Instant now = Instant.now();
        String orphanQueryId = newQueryId(now.minus(java.time.Duration.ofHours(2)), "00002", "abcde");

        Path emptyOrphanDirectory = tempDir.resolve("4550a9." + orphanQueryId + ".external-exchange-2.2");
        Files.createDirectories(emptyOrphanDirectory);

        ExchangeOrphanCleanupJob job = new ExchangeOrphanCleanupJob(dispatchManager, exchangeManagerRegistry, cleanupConfig());
        job.cleanup(List.of(), extendedExchangeManager(exchangeManagerRegistry));

        assertThat(emptyOrphanDirectory).doesNotExist();
    }

    private static ExchangeManagerRegistry createRegistry(Path tempDir)
    {
        ExchangeManagerRegistry exchangeManagerRegistry = new ExchangeManagerRegistry(
                OpenTelemetry.noop(),
                noopTracer(),
                new SecretsResolver(Map.of()),
                new ExchangeManagerConfig());
        exchangeManagerRegistry.addExchangeManagerFactory(new FileSystemExchangeManagerFactory());
        exchangeManagerRegistry.loadExchangeManager("filesystem", ImmutableMap.of("exchange.base-directories", tempDir.toUri().toString()));
        return exchangeManagerRegistry;
    }

    private static ExtendedExchangeManager extendedExchangeManager(ExchangeManagerRegistry exchangeManagerRegistry)
    {
        return (ExtendedExchangeManager) exchangeManagerRegistry.getExchangeManager();
    }

    private static ExchangeManagerConfig cleanupConfig()
    {
        return new ExchangeManagerConfig()
                .setOrphanCleanupEnabled(true)
                .setOrphanCleanupInterval(new Duration(1, TimeUnit.MINUTES))
                .setOrphanCleanupRetention(new Duration(15, TimeUnit.MINUTES));
    }

    private static BasicQueryInfo queryInfo(String queryId, QueryState state)
    {
        return new BasicQueryInfo(
                QueryId.valueOf(queryId),
                TEST_SESSION.toSessionRepresentation(),
                Optional.of(new ResourceGroupId("global")),
                state,
                true,
                URI.create("http://localhost"),
                "SELECT 1",
                Optional.empty(),
                Optional.empty(),
                BasicQueryStats.immediateFailureQueryStats(),
                null,
                null,
                Optional.empty(),
                RetryPolicy.NONE);
    }

    private static Path createExchangeDirectory(Path baseDirectory, String directoryName)
            throws IOException
    {
        Path dataFile = baseDirectory.resolve(directoryName).resolve("0").resolve("0_0.data");
        Files.createDirectories(dataFile.getParent());
        Files.writeString(dataFile, "data");
        return baseDirectory.resolve(directoryName);
    }

    private static String newQueryId(Instant instant, String sequence, String coordinatorId)
    {
        return QUERY_ID_TIMESTAMP_FORMATTER.format(instant) + "_" + sequence + "_" + coordinatorId;
    }
}
