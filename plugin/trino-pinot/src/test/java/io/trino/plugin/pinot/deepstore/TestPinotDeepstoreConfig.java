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
package io.trino.plugin.pinot.deepstore;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class TestPinotDeepstoreConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(
                ConfigAssertions.recordDefaults(PinotDeepStoreConfig.class)
                        .setDeepStoreUri(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("pinot.deep-store-uri", "http://deepstore.net")
                .buildOrThrow();

        PinotDeepStoreConfig expected = new PinotDeepStoreConfig()
                .setDeepStoreUri("http://deepstore.net");
        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
