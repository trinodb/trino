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

import com.google.inject.Binder;
import com.google.inject.Module;
import io.airlift.tracing.SpanSerialization.SpanDeserializer;
import io.airlift.tracing.SpanSerialization.SpanSerializer;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;

import static io.airlift.json.JsonBinder.jsonBinder;
import static java.util.Objects.requireNonNull;

/**
 * Binds the globally registered OpenTelemetry instance (typically from Java agent)
 * instead of creating a dedicated instance.
 * <p>
 * This allows Trino to share the same OpenTelemetry SDK instance as the Java agent,
 * which means:
 * <ul>
 * <li>All spans (auto-instrumented and manual) use the same exporter configuration</li>
 * <li>Resource attributes configured in the agent apply to Trino's manual spans</li>
 * <li>No duplicate OpenTelemetry instances or exporters</li>
 * <li>Consistent telemetry configuration across the application</li>
 * </ul>
 */
public class GlobalOpenTelemetryModule
        implements Module
{
    private final String serviceName;
    private final String serviceVersion;

    public GlobalOpenTelemetryModule(String serviceName, String serviceVersion)
    {
        this.serviceName = requireNonNull(serviceName, "serviceName is null");
        this.serviceVersion = requireNonNull(serviceVersion, "serviceVersion is null");
    }

    @Override
    public void configure(Binder binder)
    {
        // Bind the globally registered OpenTelemetry instance
        // This is typically set by the OpenTelemetry Java agent
        OpenTelemetry openTelemetry = GlobalOpenTelemetry.get();
        binder.bind(OpenTelemetry.class).toInstance(openTelemetry);

        // Create a tracer with Trino's service name and version
        // The tracer will inherit all configuration (exporters, processors, resources)
        // from the global OpenTelemetry instance
        Tracer tracer = openTelemetry.getTracer(serviceName, serviceVersion);
        binder.bind(Tracer.class).toInstance(tracer);

        // Register JSON serializers for Span (required for remote task communication)
        // This is normally done by TracingModule, but we need to do it here since
        // we're bypassing TracingModule when using GlobalOpenTelemetry
        jsonBinder(binder).addSerializerBinding(Span.class).to(SpanSerializer.class);
        jsonBinder(binder).addDeserializerBinding(Span.class).to(SpanDeserializer.class);
    }
}
