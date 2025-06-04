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
package io.trino.execution;

import com.google.common.collect.ImmutableMap;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestConnectorTracingContextPropagation
{
    private static final String CATALOG_NAME = "test_catalog";
    private static final String CONNECTOR_NAME = "test_connector";

    @Test
    public void testTracingContextCapture()
    {
        AtomicReference<Context> capturedContext = new AtomicReference<>();

        try (QueryRunner queryRunner = new StandaloneQueryRunner(testSessionBuilder().build())) {
            queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                    .withName(CONNECTOR_NAME)
                    .withData(table -> { // invoked in ConnectorPageSourceProvider
                        capturedContext.set(Context.current());
                        return List.of(List.of());
                    })
                    .build()));
            queryRunner.createCatalog(CATALOG_NAME, CONNECTOR_NAME, ImmutableMap.of());

            queryRunner.execute("SELECT COUNT(*) FROM %s.test.test".formatted(CATALOG_NAME));
        }

        assertThat(capturedContext.get())
                .matches(ctx -> Span.fromContext(ctx).getSpanContext().isValid(), "valid tracing context");
    }
}
