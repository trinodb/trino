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
package io.trino.plugin.kingbaseoracle;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.units.Duration;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.math.RoundingMode;
import java.util.Optional;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

@DefunctConfig("oracle.disable-automatic-fetch-size")
public class KingbaseOracleConfig
{
    private boolean synonymsEnabled;
    private boolean remarksReportingEnabled;
    private Integer defaultNumberScale;
    private RoundingMode numberRoundingMode = RoundingMode.UNNECESSARY;
    private boolean connectionPoolEnabled = true;
    private int connectionPoolMinSize = 1;
    private int connectionPoolMaxSize = 30;
    private Duration inactiveConnectionTimeout = new Duration(20, MINUTES);
    private Integer fetchSize;
    /** 登录超时，对应 KES JDBC loginTimeout，单位秒。 */
    private Duration loginTimeout = new Duration(30, SECONDS);
    /** Socket 读取超时，对应 KES JDBC socketTimeout，单位秒。超过后查询会报读超时。设为 0 表示不限制（允许长查询）。 */
    private Duration socketTimeout = new Duration(300, SECONDS);

    public boolean isSynonymsEnabled()
    {
        return synonymsEnabled;
    }

    @Config("oracle.synonyms.enabled")
    public KingbaseOracleConfig setSynonymsEnabled(boolean enabled)
    {
        this.synonymsEnabled = enabled;
        return this;
    }

    public boolean isRemarksReportingEnabled()
    {
        return remarksReportingEnabled;
    }

    @Config("oracle.remarks-reporting.enabled")
    public KingbaseOracleConfig setRemarksReportingEnabled(boolean enabled)
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
    public KingbaseOracleConfig setDefaultNumberScale(Integer defaultNumberScale)
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
    public KingbaseOracleConfig setNumberRoundingMode(RoundingMode numberRoundingMode)
    {
        this.numberRoundingMode = numberRoundingMode;
        return this;
    }

    public Optional<@Min(0) Integer> getFetchSize()
    {
        return Optional.ofNullable(fetchSize);
    }

    @Config("oracle.fetch-size")
    @ConfigDescription("Oracle fetch size, trino specific heuristic is applied if empty")
    public KingbaseOracleConfig setFetchSize(Integer fetchSize)
    {
        this.fetchSize = fetchSize;
        return this;
    }

    @NotNull
    public Duration getLoginTimeout()
    {
        return loginTimeout;
    }

    @Config("oracle.login-timeout")
    @ConfigDescription("Login timeout. Maps to KES JDBC loginTimeout (seconds)")
    public KingbaseOracleConfig setLoginTimeout(Duration loginTimeout)
    {
        this.loginTimeout = loginTimeout;
        return this;
    }

    @NotNull
    public Duration getSocketTimeout()
    {
        return socketTimeout;
    }

    @Config("oracle.socket-timeout")
    @ConfigDescription("Socket read timeout (seconds). Queries longer than this may fail. Set to 0 for no limit. Maps to KES JDBC socketTimeout")
    public KingbaseOracleConfig setSocketTimeout(Duration socketTimeout)
    {
        this.socketTimeout = socketTimeout;
        return this;
    }

//    @AssertTrue(message = "Pool min size cannot be larger than max size")
//    public boolean isPoolSizedProperly()
//    {
//        return getConnectionPoolMaxSize() >= getConnectionPoolMinSize();
//    }
}
