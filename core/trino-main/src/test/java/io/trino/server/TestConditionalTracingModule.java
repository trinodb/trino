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
package io.trino.server;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.node.NodeModule;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestConditionalTracingModule
{
    @BeforeAll
    public void setup()
    {
        // Ensure clean state
        GlobalOpenTelemetry.resetForTest();
    }

    @AfterAll
    public void cleanup()
    {
        // Reset after all tests
        GlobalOpenTelemetry.resetForTest();
        System.clearProperty("tracing.use-global");
    }

    @Test
    public void testUsesGlobalWhenAvailable()
    {
        try {
            // Initialize GlobalOpenTelemetry (simulating Java agent)
            OpenTelemetrySdk sdk = OpenTelemetrySdk.builder().build();
            GlobalOpenTelemetry.set(sdk);

            Bootstrap bootstrap = new Bootstrap(new ConditionalTracingModule("test-service", "1.0"));
            Injector injector = bootstrap
                    .doNotInitializeLogging()
                    .initialize();

            // Should use the global instance (which wraps the SDK)
            OpenTelemetry openTelemetry = injector.getInstance(OpenTelemetry.class);
            assertThat(openTelemetry).isSameAs(GlobalOpenTelemetry.get());

            Tracer tracer = injector.getInstance(Tracer.class);
            assertThat(tracer).isNotNull();
        }
        finally {
            GlobalOpenTelemetry.resetForTest();
        }
    }

    @Test
    public void testSystemPropertyForcesGlobal()
    {
        try {
            // Don't initialize GlobalOpenTelemetry, but force usage via system property
            System.setProperty("tracing.use-global", "true");

            Bootstrap bootstrap = new Bootstrap(new ConditionalTracingModule("test-service", "1.0"));
            Injector injector = bootstrap
                    .doNotInitializeLogging()
                    .initialize();

            // Should use GlobalOpenTelemetry even though it's noop
            OpenTelemetry openTelemetry = injector.getInstance(OpenTelemetry.class);
            assertThat(openTelemetry).isSameAs(GlobalOpenTelemetry.get());
        }
        finally {
            System.clearProperty("tracing.use-global");
            GlobalOpenTelemetry.resetForTest();
        }
    }

    @Test
    public void testSystemPropertyOverridesGlobalDetection()
    {
        try {
            // Initialize GlobalOpenTelemetry (simulating Java agent)
            OpenTelemetrySdk agentSdk = OpenTelemetrySdk.builder().build();
            GlobalOpenTelemetry.set(agentSdk);
            OpenTelemetry agentInstance = GlobalOpenTelemetry.get();

            // But explicitly configure to NOT use global via system property
            System.setProperty("tracing.use-global", "false");

            // TracingModule requires NodeInfo, so include NodeModule
            Bootstrap bootstrap = new Bootstrap(new NodeModule(), new ConditionalTracingModule("test-service", "1.0"));
            Injector injector = bootstrap
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperty("node.environment", "test")
                    .initialize();

            // Should NOT use the agent's global instance (system property overrides auto-detection)
            // Note: TracingModule will create its own SDK and register it globally,
            // so we verify we're NOT using the original agent SDK we set
            OpenTelemetry openTelemetry = injector.getInstance(OpenTelemetry.class);
            assertThat(openTelemetry).isNotSameAs(agentInstance);

            Tracer tracer = injector.getInstance(Tracer.class);
            assertThat(tracer).isNotNull();
        }
        finally {
            System.clearProperty("tracing.use-global");
            GlobalOpenTelemetry.resetForTest();
        }
    }
}
