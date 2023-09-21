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
package io.trino.plugin.oracle;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.units.Duration;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.math.RoundingMode;
import java.util.Optional;

import static java.util.concurrent.TimeUnit.MINUTES;

@DefunctConfig("oracle.disable-automatic-fetch-size")
public class OracleConfig
{
    private boolean synonymsEnabled;
    private boolean remarksReportingEnabled;
    private Integer defaultNumberScale;
    private RoundingMode numberRoundingMode = RoundingMode.UNNECESSARY;
    private boolean connectionPoolEnabled = true;
    private int connectionPoolMinSize = 1;
    private int connectionPoolMaxSize = 30;
    private Duration inactiveConnectionTimeout = new Duration(20, MINUTES);

    @NotNull
    public boolean isSynonymsEnabled()
    {
        return synonymsEnabled;
    }

    @Config("oracle.synonyms.enabled")
    public OracleConfig setSynonymsEnabled(boolean enabled)
    {
        this.synonymsEnabled = enabled;
        return this;
    }

    public boolean isRemarksReportingEnabled()
    {
        return remarksReportingEnabled;
    }

    @Config("oracle.remarks-reporting.enabled")
    public OracleConfig setRemarksReportingEnabled(boolean enabled)
    {
        this.remarksReportingEnabled = enabled;
        return this;
    }

    public Optional<@Min(0) @Max(38) Integer> getDefaultNumberScale()
    {
        return Optional.ofNullable(defaultNumberScale);
    }

    @Config("oracle.number.default-scale")
    @ConfigDescription("Default Trino DECIMAL scale for Oracle NUMBER data type")
    public OracleConfig setDefaultNumberScale(Integer defaultNumberScale)
    {
        this.defaultNumberScale = defaultNumberScale;
        return this;
    }

    @NotNull
    public RoundingMode getNumberRoundingMode()
    {
        return numberRoundingMode;
    }

    @Config("oracle.number.rounding-mode")
    public OracleConfig setNumberRoundingMode(RoundingMode numberRoundingMode)
    {
        this.numberRoundingMode = numberRoundingMode;
        return this;
    }

    @NotNull
    public boolean isConnectionPoolEnabled()
    {
        return connectionPoolEnabled;
    }

    @Config("oracle.connection-pool.enabled")
    public OracleConfig setConnectionPoolEnabled(boolean connectionPoolEnabled)
    {
        this.connectionPoolEnabled = connectionPoolEnabled;
        return this;
    }

    @Min(0)
    public int getConnectionPoolMinSize()
    {
        return connectionPoolMinSize;
    }

    @Config("oracle.connection-pool.min-size")
    public OracleConfig setConnectionPoolMinSize(int connectionPoolMinSize)
    {
        this.connectionPoolMinSize = connectionPoolMinSize;
        return this;
    }

    @Min(1)
    public int getConnectionPoolMaxSize()
    {
        return connectionPoolMaxSize;
    }

    @Config("oracle.connection-pool.max-size")
    public OracleConfig setConnectionPoolMaxSize(int connectionPoolMaxSize)
    {
        this.connectionPoolMaxSize = connectionPoolMaxSize;
        return this;
    }

    @NotNull
    public Duration getInactiveConnectionTimeout()
    {
        return inactiveConnectionTimeout;
    }

    @Config("oracle.connection-pool.inactive-timeout")
    @ConfigDescription("How long a connection in the pool can remain idle before it is closed")
    public OracleConfig setInactiveConnectionTimeout(Duration inactiveConnectionTimeout)
    {
        this.inactiveConnectionTimeout = inactiveConnectionTimeout;
        return this;
    }

    @AssertTrue(message = "Pool min size cannot be larger than max size")
    public boolean isPoolSizedProperly()
    {
        return getConnectionPoolMaxSize() >= getConnectionPoolMinSize();
    }
}
