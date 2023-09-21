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
package io.trino.execution;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import jakarta.validation.constraints.NotNull;

import static java.util.concurrent.TimeUnit.MINUTES;

public class FailureInjectionConfig
{
    private Duration expirationPeriod = new Duration(10, MINUTES);
    private Duration requestTimeout = new Duration(2, MINUTES);

    @NotNull
    public Duration getExpirationPeriod()
    {
        return expirationPeriod;
    }

    @Config("failure-injection.expiration-period")
    @ConfigDescription("Period after which an injected failure is considered expired and will no longer be triggering a failure")
    public FailureInjectionConfig setExpirationPeriod(Duration expirationPeriod)
    {
        this.expirationPeriod = expirationPeriod;
        return this;
    }

    @NotNull
    public Duration getRequestTimeout()
    {
        return requestTimeout;
    }

    @Config("failure-injection.request-timeout")
    @ConfigDescription("Period after which requests blocked to emulate a timeout are released")
    public FailureInjectionConfig setRequestTimeout(Duration requestTimeout)
    {
        this.requestTimeout = requestTimeout;
        return this;
    }
}
