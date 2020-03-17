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
package io.prestosql.plugin.jdbc;

import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

public class RetryJdbcConfig
{
    private Duration delay = new Duration(1, TimeUnit.SECONDS);
    private Duration maxDelay = new Duration(1, TimeUnit.MINUTES);
    private int maxRetries = 10;
    private double delayFactor = 2.0;

    @NotNull
    @MinDuration("0ms")
    public Duration getDelay()
    {
        return delay;
    }

    @Config("connect.retry-delay")
    public RetryJdbcConfig setDelay(Duration delay)
    {
        this.delay = delay;
        return this;
    }

    @NotNull
    @MinDuration("0ms")
    public Duration getMaxDelay()
    {
        return maxDelay;
    }

    @Config("connect.max-retry-delay")
    public RetryJdbcConfig setMaxDelay(Duration maxDelay)
    {
        this.maxDelay = maxDelay;
        return this;
    }

    @Min(0)
    public int getMaxRetries()
    {
        return maxRetries;
    }

    @Config("connect.max-retries")
    public RetryJdbcConfig setMaxRetries(int maxRetries)
    {
        this.maxRetries = maxRetries;
        return this;
    }

    @Min(0)
    public double getDelayFactor()
    {
        return delayFactor;
    }

    @Config("connect.retry-delay-factor")
    public RetryJdbcConfig setDelayFactor(double delayFactor)
    {
        this.delayFactor = delayFactor;
        return this;
    }
}
