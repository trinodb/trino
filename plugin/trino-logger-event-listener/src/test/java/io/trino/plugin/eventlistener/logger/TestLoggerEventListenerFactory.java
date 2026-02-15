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
package io.trino.plugin.eventlistener.logger;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.spi.eventlistener.EventListenerFactory;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TestLoggerEventListenerFactory
{
    @Test
    public void testFactoryName()
    {
        LoggerEventListenerFactory factory = new LoggerEventListenerFactory();
        assertThat(factory.getName()).isEqualTo("logger");
    }

    @Test
    public void testFactoryCreatesListener()
    {
        LoggerEventListenerFactory factory = new LoggerEventListenerFactory();
        assertThat(factory.create(
                Map.of("logger-event-listener.log-created", "true"),
                new TestingEventListenerContext())).isNotNull();
    }

    private static class TestingEventListenerContext
            implements EventListenerFactory.EventListenerContext
    {
        @Override
        public String getVersion()
        {
            return "test";
        }

        @Override
        public OpenTelemetry getOpenTelemetry()
        {
            return OpenTelemetry.noop();
        }

        @Override
        public Tracer getTracer()
        {
            return OpenTelemetry.noop().getTracer("test");
        }
    }
}
