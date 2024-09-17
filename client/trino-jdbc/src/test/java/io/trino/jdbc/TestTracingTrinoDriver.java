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
package io.trino.jdbc;

import io.airlift.log.Logging;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.server.testing.TestingTrinoServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.opentelemetry.api.common.AttributeKey.longKey;
import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestTracingTrinoDriver
{
    private TestingTrinoServer server;

    @BeforeAll
    public void setupServer()
    {
        Logging.initialize();
        server = TestingTrinoServer.create();

        server.installPlugin(new MemoryPlugin());
        server.createCatalog("memory", "memory");
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        server.close();
        server = null;
    }

    @Test
    public void testInitialize()
            throws IOException, SQLException
    {
        InMemorySpanExporter exporter = InMemorySpanExporter.create();
        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(exporter))
                .build();

        OpenTelemetry telemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .build();

        try (TracingTrinoDriver tracingDriver = new TracingTrinoDriver(telemetry)) {
            try (Connection connection = createConnection(tracingDriver, "memory")) {
                try (Statement statement = connection.createStatement()) {
                    statement.execute("SHOW SCHEMAS FROM memory");
                }
            }
        }

        List<String> uris = exporter.getFinishedSpanItems().stream()
                .filter(span -> span.getAttributes().get(longKey("http.response.status_code")) != null)
                .map(span -> span.getAttributes().get(stringKey("url.full")))
                .map(value -> URI.create(value).getPath())
                .collect(toImmutableList());

        assertThat(uris)
                .hasSizeBetween(3, 10)  // POST, queued, executing
                .contains("/v1/statement");
    }

    private Connection createConnection(Driver driver, String catalog)
            throws SQLException
    {
        String url = format("jdbc:trino://%s/%s?explicitPrepare=true&user=test", server.getAddress(), catalog);
        return driver.connect(url, new Properties());
    }
}
