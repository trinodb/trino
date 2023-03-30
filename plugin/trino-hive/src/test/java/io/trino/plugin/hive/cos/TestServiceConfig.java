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
package io.trino.plugin.hive.cos;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.testing.TempFile;
import org.testng.annotations.Test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestServiceConfig
{
    @Test
    public void testInitializeConfiguration()
    {
        testInitializeConfiguration2("accessValue", "secretValue", Optional.of("endpointValue"));
        testInitializeConfiguration2("accessValue", "secretValue", Optional.empty());
    }

    private static void testInitializeConfiguration2(String accessValue, String secretValue, Optional<String> endpointValue)
    {
        ServiceConfig serviceConfig = new ServiceConfig("name", accessValue, secretValue, endpointValue);
        assertConfig(serviceConfig, accessValue, secretValue, endpointValue);
    }

    private static void assertConfig(
            ServiceConfig serviceConfig,
            String accessValue,
            String secretValue,
            Optional<String> endpointValue)
    {
        assertEquals(serviceConfig.getAccessKey(), accessValue);
        assertEquals(serviceConfig.getSecretKey(), secretValue);
        assertEquals(serviceConfig.getEndpoint(), endpointValue);
    }

    @Test
    public void testLoad()
            throws IOException
    {
        try (TempFile tempFile = new TempFile()) {
            assertTrue(ServiceConfig.loadServiceConfigs(tempFile.file()).isEmpty());

            writeProperties(tempFile, ImmutableMap.<String, String>builder()
                    .put("a.access-key", "a-accessValue")
                    .put("a.secret-key", "a-secretValue")
                    .put("a.endpoint", "a-endpointValue")

                    .put("b.access-key", "b-accessValue")
                    .put("b.secret-key", "b-secretValue")

                    .put("c.access-key", "c-accessValue")
                    .put("c.secret-key", "c-secretValue")
                    .put("c.endpoint", "c-endpointValue")
                    .buildOrThrow());

            Map<String, ServiceConfig> bucketConfigs = ServiceConfig.loadServiceConfigs(tempFile.file());
            assertEquals(bucketConfigs.keySet(), ImmutableSet.of("a", "b", "c"));

            assertConfig(bucketConfigs.get("a"), "a-accessValue", "a-secretValue", Optional.of("a-endpointValue"));
            assertConfig(bucketConfigs.get("b"), "b-accessValue", "b-secretValue", Optional.empty());
            assertConfig(bucketConfigs.get("c"), "c-accessValue", "c-secretValue", Optional.of("c-endpointValue"));
        }
    }

    @Test
    public void testLoadInvalid()
            throws IOException
    {
        assertInvalidLoad(
                "a.secret-key",
                ImmutableMap.of("a.access-key", "a-accessValue"));
        assertInvalidLoad(
                "a.unknown",
                ImmutableMap.<String, String>builder()
                        .put("a.access-key", "a-accessValue")
                        .put("a.secret-key", "a-secretValue")
                        .put("a.unknown", "value")
                        .buildOrThrow());
        assertInvalidLoad(
                "unknown",
                ImmutableMap.<String, String>builder()
                        .put("a.access-key", "a-accessValue")
                        .put("a.secret-key", "a-secretValue")
                        .put("unknown", "value")
                        .buildOrThrow());
    }

    private static void assertInvalidLoad(String message, ImmutableMap<String, String> properties)
            throws IOException
    {
        try (TempFile tempFile = new TempFile()) {
            writeProperties(tempFile, properties);

            assertThatThrownBy(() -> ServiceConfig.loadServiceConfigs(tempFile.file()))
                    .hasMessageContaining(message)
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    private static void writeProperties(TempFile tempFile, Map<String, String> map)
            throws IOException
    {
        try (FileOutputStream out = new FileOutputStream(tempFile.file())) {
            Properties properties = new Properties();
            properties.putAll(map);
            properties.store(out, "test");
        }
    }
}
