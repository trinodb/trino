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
package io.trino.plugin.base.evenlistener;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.spi.eventlistener.EventListenerFactory;

import static java.util.Objects.requireNonNull;

public class TestingEventListenerContext
        implements EventListenerFactory.EventListenerContext
{
    private final String trinoVersion;

    public TestingEventListenerContext()
    {
        this("trino-version");
    }

    public TestingEventListenerContext(String version)
    {
        this.trinoVersion = requireNonNull(version, "version is null");
    }

    @Override
    public String getVersion()
    {
        return this.trinoVersion;
    }

    @Override
    public OpenTelemetry getOpenTelemetry()
    {
        return OpenTelemetry.noop();
    }

    @Override
    public Tracer getTracer()
    {
        return OpenTelemetry.noop().getTracer("TEST_TRACER");
    }
}
