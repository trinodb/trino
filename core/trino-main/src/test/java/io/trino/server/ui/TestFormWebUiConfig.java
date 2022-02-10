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
package io.trino.server.ui;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestFormWebUiConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(FormWebUiConfig.class)
                .setSessionTimeout(new Duration(1, TimeUnit.DAYS))
                .setSharedSecret(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("web-ui.session-timeout", "33s")
                .put("web-ui.shared-secret", "test-secret")
                .buildOrThrow();

        FormWebUiConfig expected = new FormWebUiConfig()
                .setSessionTimeout(new Duration(33, TimeUnit.SECONDS))
                .setSharedSecret("test-secret");

        assertFullMapping(properties, expected);
    }
}
