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
package io.prestosql.plugin.password.salesforce;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MaxDuration;

import javax.validation.constraints.NotNull;

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class SalesforceConfig
{
    private int cacheSize = 4096;
    private Duration cacheExpireSeconds = Duration.succinctDuration(120, TimeUnit.SECONDS);
    private String allowedOrganizations;

    @NotNull(message = "Must set salesforce.allowed-organization with one or more Salesforce 18 char Organization Ids, or \"all\"")
    public String getAllowedOrganizations()
    {
        return allowedOrganizations;
    }

    public Set<String> getOrgSet()
    {
        Set<String> tmp = new HashSet<>();
        if (allowedOrganizations == null) {
            allowedOrganizations = "";
        }
        String[] orgsSplit = allowedOrganizations.split("[,;]");
        for (String s : orgsSplit) {
            // The organizationId is always in Locale.US, regardless of the user's locale and language.
            tmp.add(s.toLowerCase(Locale.US).trim());
        }

        return tmp;
    }

    @Config("salesforce.allowed-organizations")
    @ConfigDescription("Comma separated list of Salesforce 18 Character Organization Ids.")
    public SalesforceConfig setAllowedOrganizations(String allowedOrganizations)
    {
        this.allowedOrganizations = allowedOrganizations;
        return this;
    }

    public int getCacheSize()
    {
        return cacheSize;
    }

    @Config("salesforce.cache-size")
    @ConfigDescription("Maximum size of the cache that holds authenticated users.  Default is 4096 entries.")
    public SalesforceConfig setCacheSize(int cacheSize)
    {
        this.cacheSize = cacheSize;
        return this;
    }

    @MaxDuration(value = "3600s")
    public Duration getCacheExpireSeconds()
    {
        return cacheExpireSeconds;
    }

    @Config("salesforce.cache-expire-seconds")
    @ConfigDescription("Expire time in seconds for an entry in cache since last write.  Default is 120 seconds, max is 3600 seconds.")
    public SalesforceConfig setCacheExpireSeconds(Duration cacheExpireSeconds)
    {
        this.cacheExpireSeconds = cacheExpireSeconds;
        return this;
    }
}
