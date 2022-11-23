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
package io.trino.plugin.pinot;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static io.trino.plugin.pinot.auth.PinotAuthenticationType.PASSWORD;

public class TestSecuredPinotIntegrationConnectorSmokeTest
        extends BasePinotIntegrationConnectorSmokeTest
{
    @Override
    protected boolean isSecured()
    {
        return true;
    }

    @Override
    protected Map<String, String> additionalPinotProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("pinot.controller.authentication.type", PASSWORD.name())
                .put("pinot.controller.authentication.user", "admin")
                .put("pinot.controller.authentication.password", "verysecret")
                .put("pinot.broker.authentication.type", PASSWORD.name())
                .put("pinot.broker.authentication.user", "query")
                .put("pinot.broker.authentication.password", "secret")
                .buildOrThrow();
    }
}
