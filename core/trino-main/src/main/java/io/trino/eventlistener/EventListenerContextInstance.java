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
package io.trino.eventlistener;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.spi.eventlistener.EventListenerFactory;

import static java.util.Objects.requireNonNull;

public class EventListenerContextInstance
        implements EventListenerFactory.EventListenerContext
{
    private final OpenTelemetry openTelemetry;
    private final Tracer tracer;
    private final String version;

    public EventListenerContextInstance(String version, OpenTelemetry openTelemetry, Tracer tracer)
    {
        this.version = requireNonNull(version, "version is null");
        this.openTelemetry = requireNonNull(openTelemetry, "openTelemetry is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
    }

    @Override
    public String getVersion()
    {
        return version;
    }

    @Override
    public OpenTelemetry getOpenTelemetry()
    {
        return openTelemetry;
    }

    @Override
    public Tracer getTracer()
    {
        return tracer;
    }
}
