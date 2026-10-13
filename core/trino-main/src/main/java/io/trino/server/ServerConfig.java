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
package io.trino.server;

import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

import java.util.Optional;
import java.util.Set;

import static java.util.concurrent.TimeUnit.MINUTES;

public class ServerConfig
{
    private boolean coordinator = true;
    private boolean concurrentStartup;
    private boolean includeExceptionInResponse = true;
    private Duration gracePeriod = new Duration(2, MINUTES);
    private boolean queryResultsCompressionEnabled = true;
    private Optional<String> queryInfoUrlTemplate = Optional.empty();
    private Set<String> nodeGroups = ImmutableSet.of();

    public boolean isCoordinator()
    {
        return coordinator;
    }

    @Config("coordinator")
    public ServerConfig setCoordinator(boolean coordinator)
    {
        this.coordinator = coordinator;
        return this;
    }

    public boolean isConcurrentStartup()
    {
        return concurrentStartup;
    }

    @Config("experimental.concurrent-startup")
    @ConfigDescription("Parallelize work during server startup")
    public ServerConfig setConcurrentStartup(boolean concurrentStartup)
    {
        this.concurrentStartup = concurrentStartup;
        return this;
    }

    public boolean isIncludeExceptionInResponse()
    {
        return includeExceptionInResponse;
    }

    @Config("http.include-exception-in-response")
    public ServerConfig setIncludeExceptionInResponse(boolean includeExceptionInResponse)
    {
        this.includeExceptionInResponse = includeExceptionInResponse;
        return this;
    }

    public Duration getGracePeriod()
    {
        return gracePeriod;
    }

    @Config("shutdown.grace-period")
    public ServerConfig setGracePeriod(Duration gracePeriod)
    {
        this.gracePeriod = gracePeriod;
        return this;
    }

    public boolean isQueryResultsCompressionEnabled()
    {
        return queryResultsCompressionEnabled;
    }

    @Config("query-results.compression-enabled")
    public ServerConfig setQueryResultsCompressionEnabled(boolean queryResultsCompressionEnabled)
    {
        this.queryResultsCompressionEnabled = queryResultsCompressionEnabled;
        return this;
    }

    @NotNull
    public Optional<String> getQueryInfoUrlTemplate()
    {
        return queryInfoUrlTemplate;
    }

    @Config("query.info-url-template")
    public ServerConfig setQueryInfoUrlTemplate(String queryInfoUrlTemplate)
    {
        this.queryInfoUrlTemplate = Optional.ofNullable(queryInfoUrlTemplate);
        return this;
    }

    @NotNull
    public Set<@Pattern(regexp = "[A-Za-z0-9][_A-Za-z0-9-]*") String> getNodeGroups()
    {
        return nodeGroups;
    }

    @Config("node.groups")
    @ConfigDescription("Names of the node groups this server belongs to")
    public ServerConfig setNodeGroups(Set<String> nodeGroups)
    {
        this.nodeGroups = ImmutableSet.copyOf(nodeGroups);
        return this;
    }
}
