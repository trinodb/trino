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

import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

import static java.util.concurrent.TimeUnit.DAYS;

public class FormWebUiConfig
{
    private Optional<String> sharedSecret = Optional.empty();
    private Duration sessionTimeout = new Duration(1, DAYS);

    @NotNull
    public Optional<String> getSharedSecret()
    {
        return sharedSecret;
    }

    @Config("web-ui.shared-secret")
    public FormWebUiConfig setSharedSecret(String sharedSecret)
    {
        this.sharedSecret = Optional.ofNullable(sharedSecret);
        return this;
    }

    @NotNull
    public Duration getSessionTimeout()
    {
        return sessionTimeout;
    }

    @Config("web-ui.session-timeout")
    public FormWebUiConfig setSessionTimeout(Duration sessionTimeout)
    {
        this.sessionTimeout = sessionTimeout;
        return this;
    }
}
