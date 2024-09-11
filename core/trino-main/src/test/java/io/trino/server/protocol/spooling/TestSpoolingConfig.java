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
package io.trino.server.protocol.spooling;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.util.Ciphers.createRandomAesEncryptionKey;

class TestSpoolingConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(SpoolingConfig.class)
                .setUseWorkers(false)
                .setDirectStorageAccess(true)
                .setDirectStorageFallback(false)
                .setInlineSegments(true)
                .setSharedEncryptionKey(null)
                .setInitialSegmentSize(DataSize.of(8, MEGABYTE))
                .setMaximumSegmentSize(DataSize.of(16, MEGABYTE)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        String randomAesEncryptionKey = Base64.getEncoder().encodeToString(createRandomAesEncryptionKey().getEncoded());

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("protocol.spooling.worker-access", "true")
                .put("protocol.spooling.direct-storage-access", "false")
                .put("protocol.spooling.direct-storage-fallback", "true")
                .put("protocol.spooling.inline-segments", "false")
                .put("protocol.spooling.shared-secret-key", randomAesEncryptionKey) // 256 bits
                .put("protocol.spooling.initial-segment-size", "2MB")
                .put("protocol.spooling.maximum-segment-size", "4MB")
                .buildOrThrow();

        SpoolingConfig expected = new SpoolingConfig()
                .setUseWorkers(true)
                .setDirectStorageAccess(false)
                .setDirectStorageFallback(true)
                .setInlineSegments(false)
                .setSharedEncryptionKey(randomAesEncryptionKey)
                .setInitialSegmentSize(DataSize.of(2, MEGABYTE))
                .setMaximumSegmentSize(DataSize.of(4, MEGABYTE));

        assertFullMapping(properties, expected);
    }
}
