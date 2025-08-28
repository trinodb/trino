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
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.server.protocol.spooling.SpoolingConfig.SegmentRetrievalMode.COORDINATOR_STORAGE_REDIRECT;
import static io.trino.server.protocol.spooling.SpoolingConfig.SegmentRetrievalMode.STORAGE;
import static io.trino.server.protocol.spooling.SpoolingConfig.SegmentRetrievalMode.WORKER_PROXY;
import static io.trino.util.Ciphers.createRandomAesEncryptionKey;

class TestSpoolingConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(SpoolingConfig.class)
                .setInliningEnabled(true)
                .setInliningMaxRows(50_000)
                .setInliningMaxSize(DataSize.of(3, MEGABYTE))
                .setSharedSecretKey(null)
                .setRetrievalMode(STORAGE)
                .setInitialSegmentSize(DataSize.of(8, MEGABYTE))
                .setMaximumSegmentSize(DataSize.of(16, MEGABYTE))
                .setMaxConcurrentSegmentSerialization(1)
                .setArrowMaxAllocation(DataSize.of(200, MEGABYTE)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        String randomAesEncryptionKey = Base64.getEncoder().encodeToString(createRandomAesEncryptionKey().getEncoded());

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("protocol.spooling.shared-secret-key", randomAesEncryptionKey) // 256 bits
                .put("protocol.spooling.retrieval-mode", "coordinator_storage_redirect")
                .put("protocol.spooling.inlining.enabled", "false")
                .put("protocol.spooling.initial-segment-size", "1kB")
                .put("protocol.spooling.max-segment-size", "8kB")
                .put("protocol.spooling.inlining.max-rows", "10000")
                .put("protocol.spooling.inlining.max-size", "1MB")
                .put("protocol.spooling.arrow.max-allocation", "512MB")
                .buildOrThrow();

        SpoolingConfig expected = new SpoolingConfig()
                .setRetrievalMode(COORDINATOR_STORAGE_REDIRECT)
                .setSharedSecretKey(randomAesEncryptionKey)
                .setInitialSegmentSize(DataSize.of(1, KILOBYTE))
                .setMaximumSegmentSize(DataSize.of(8, KILOBYTE))
                .setInliningMaxRows(10000)
                .setInliningMaxSize(DataSize.of(1, MEGABYTE))
                .setInliningEnabled(false)
                .setArrowMaxAllocation(DataSize.of(512, MEGABYTE));

        assertFullMapping(properties, expected);
    }
}
