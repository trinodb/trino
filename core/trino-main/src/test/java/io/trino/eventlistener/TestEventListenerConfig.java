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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;

public class TestEventListenerConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(ConfigAssertions.recordDefaults(EventListenerConfig.class)
                .setEventListenerFiles(""));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Path config1 = Files.createTempFile(null, null);
        Path config2 = Files.createTempFile(null, null);

        Map<String, String> properties = ImmutableMap.of("event-listener.config-files", config1.toString() + "," + config2.toString());

        EventListenerConfig expected = new EventListenerConfig()
                .setEventListenerFiles(ImmutableList.of(config1.toFile(), config2.toFile()));

        assertFullMapping(properties, expected);
    }
}
