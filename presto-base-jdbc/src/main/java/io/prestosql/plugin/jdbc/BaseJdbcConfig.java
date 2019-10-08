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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.NotNull;

import java.util.Set;

import static com.google.common.base.Strings.nullToEmpty;
import static java.util.concurrent.TimeUnit.MINUTES;

public class BaseJdbcConfig
{
    private String connectionUrl;
    private boolean caseInsensitiveNameMatching;
    private Duration caseInsensitiveNameMatchingCacheTtl = new Duration(1, MINUTES);
    private Set<String> jdbcTypesMappedToVarchar = ImmutableSet.of();

    @NotNull
    public String getConnectionUrl()
    {
        return connectionUrl;
    }

    @Config("connection-url")
    public BaseJdbcConfig setConnectionUrl(String connectionUrl)
    {
        this.connectionUrl = connectionUrl;
        return this;
    }

    public boolean isCaseInsensitiveNameMatching()
    {
        return caseInsensitiveNameMatching;
    }

    @Config("case-insensitive-name-matching")
    public BaseJdbcConfig setCaseInsensitiveNameMatching(boolean caseInsensitiveNameMatching)
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
    public BaseJdbcConfig setCaseInsensitiveNameMatchingCacheTtl(Duration caseInsensitiveNameMatchingCacheTtl)
    {
        this.caseInsensitiveNameMatchingCacheTtl = caseInsensitiveNameMatchingCacheTtl;
        return this;
    }

    public Set<String> getJdbcTypesMappedToVarchar()
    {
        return jdbcTypesMappedToVarchar;
    }

    @Config("jdbc-types-mapped-to-varchar")
    public BaseJdbcConfig setJdbcTypesMappedToVarchar(String jdbcTypesMappedToVarchar)
    {
        this.jdbcTypesMappedToVarchar = ImmutableSet.copyOf(Splitter.on(",").omitEmptyStrings().trimResults().split(nullToEmpty(jdbcTypesMappedToVarchar)));
        return this;
    }
}
