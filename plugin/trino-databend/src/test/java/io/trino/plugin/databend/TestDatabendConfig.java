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
package io.trino.plugin.databend;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestDatabendConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(DatabendConfig.class)
                .setConnectionTimeout(Duration.valueOf("60s"))
                .setPresignedUrlDisabled(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("databend.presigned-url-disabled", "true")
                .put("databend.connection-timeout", "10s")
                .buildOrThrow();

        DatabendConfig expected = new DatabendConfig()
                .setPresignedUrlDisabled(true)
                .setConnectionTimeout(new Duration(10, SECONDS));

        assertFullMapping(properties, expected);
    }
}
