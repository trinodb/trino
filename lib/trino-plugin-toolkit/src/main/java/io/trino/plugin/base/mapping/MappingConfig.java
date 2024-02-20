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
package io.trino.plugin.base.mapping;

import io.airlift.configuration.Config;
import io.airlift.configuration.validation.FileExists;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

import static java.util.concurrent.TimeUnit.MINUTES;

public class MappingConfig
{
    public static final String CASE_INSENSITIVE_NAME_MATCHING = "case-insensitive-name-matching";

    private boolean caseInsensitiveNameMatching;
    private Duration caseInsensitiveNameMatchingCacheTtl = new Duration(1, MINUTES);
    private String configFile;
    private Duration refreshPeriod;

    public boolean isCaseInsensitiveNameMatching()
    {
        return caseInsensitiveNameMatching;
    }

    @Config(CASE_INSENSITIVE_NAME_MATCHING)
    public MappingConfig setCaseInsensitiveNameMatching(boolean caseInsensitiveNameMatching)
    {
        this.caseInsensitiveNameMatching = caseInsensitiveNameMatching;
        return this;
    }

    @NotNull
    @MinDuration("0ms")
    public Duration getCaseInsensitiveNameMatchingCacheTtl()
    {
        return caseInsensitiveNameMatchingCacheTtl;
    }

    @Config("case-insensitive-name-matching.cache-ttl")
    public MappingConfig setCaseInsensitiveNameMatchingCacheTtl(Duration caseInsensitiveNameMatchingCacheTtl)
    {
        this.caseInsensitiveNameMatchingCacheTtl = caseInsensitiveNameMatchingCacheTtl;
        return this;
    }

    public Optional<@FileExists String> getCaseInsensitiveNameMatchingConfigFile()
    {
        return Optional.ofNullable(configFile);
    }

    @Config("case-insensitive-name-matching.config-file")
    public MappingConfig setCaseInsensitiveNameMatchingConfigFile(String authToLocalConfigFile)
    {
        this.configFile = authToLocalConfigFile;
        return this;
    }

    public Optional<@MinDuration("1ms") Duration> getCaseInsensitiveNameMatchingConfigFileRefreshPeriod()
    {
        return Optional.ofNullable(refreshPeriod);
    }

    @Config("case-insensitive-name-matching.config-file.refresh-period")
    public MappingConfig setCaseInsensitiveNameMatchingConfigFileRefreshPeriod(Duration refreshPeriod)
    {
        this.refreshPeriod = refreshPeriod;
        return this;
    }
}
