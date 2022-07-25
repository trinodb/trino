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
package io.trino.plugin.pinot.auth;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.trino.plugin.pinot.auth.PinotAuthenticationType.NONE;
import static io.trino.plugin.pinot.auth.PinotAuthenticationType.PASSWORD;

public class TestPinotAuthenticationTypeConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(
                recordDefaults(PinotAuthenticationTypeConfig.class)
                        .setControllerAuthenticationType(NONE)
                        .setBrokerAuthenticationType(NONE));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("pinot.controller.authentication.type", "password")
                .put("pinot.broker.authentication.type", "password")
                .buildOrThrow();

        PinotAuthenticationTypeConfig expected = new PinotAuthenticationTypeConfig()
                .setControllerAuthenticationType(PASSWORD)
                .setBrokerAuthenticationType(PASSWORD);

        assertFullMapping(properties, expected);
    }
}
