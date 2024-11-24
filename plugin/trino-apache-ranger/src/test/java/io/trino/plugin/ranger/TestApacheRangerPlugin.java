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
package io.trino.plugin.ranger;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.spi.Plugin;
import io.trino.spi.security.SystemAccessControlFactory;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.google.common.collect.Iterables.getOnlyElement;

final class TestApacheRangerPlugin
{
    @Test
    void testCreatePlugin()
    {
        Plugin plugin = new ApacheRangerPlugin();
        SystemAccessControlFactory factory = getOnlyElement(plugin.getSystemAccessControlFactories());
        factory.create(Map.of("apache-ranger.service.name", "trino"), new TestingSystemAccessControlContext()).shutdown();
    }

    private static final class TestingSystemAccessControlContext
            implements SystemAccessControlFactory.SystemAccessControlContext
    {
        @Override
        public String getVersion()
        {
            return "test-version";
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
}
