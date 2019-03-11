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
package io.prestosql.dispatcher;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static java.util.concurrent.TimeUnit.MINUTES;

public class TestRemoteDispatcherConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(RemoteDispatcherConfig.class)
                .setCoordinatorSelectionWaitTime(new Duration(5, MINUTES)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("dispatcher.coordinator-selection-max-wait", "33m")
                .build();

        RemoteDispatcherConfig expected = new RemoteDispatcherConfig()
                .setCoordinatorSelectionWaitTime(new Duration(33, MINUTES));

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
