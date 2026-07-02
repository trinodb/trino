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
package io.trino.execution.admission;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

class TestResourceAwareAdmissionConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ResourceAwareAdmissionConfig.class)
                .setEnabled(false)
                .setRequiredFreeMemory(DataSize.of(0, BYTE))
                .setRequiredFreeVcpu(0)
                .setMaxWait(new Duration(5, MINUTES))
                .setPollInterval(new Duration(1, SECONDS))
                .setMemoryWindowSize(50)
                .setRequiredMemoryMultiplier(1.0));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("query-admission.enabled", "true")
                .put("query-admission.required-free-memory", "10GB")
                .put("query-admission.required-free-vcpu", "16")
                .put("query-admission.max-wait", "10m")
                .put("query-admission.poll-interval", "2s")
                .put("query-admission.memory-window-size", "100")
                .put("query-admission.required-memory-multiplier", "1.5")
                .buildOrThrow();

        ResourceAwareAdmissionConfig expected = new ResourceAwareAdmissionConfig()
                .setEnabled(true)
                .setRequiredFreeMemory(DataSize.of(10, GIGABYTE))
                .setRequiredFreeVcpu(16)
                .setMaxWait(new Duration(10, MINUTES))
                .setPollInterval(new Duration(2, SECONDS))
                .setMemoryWindowSize(100)
                .setRequiredMemoryMultiplier(1.5);

        assertFullMapping(properties, expected);
    }
}
