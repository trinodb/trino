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
package io.prestosql.dispatcher;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.SessionRepresentation;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.spi.resourcegroups.ResourceGroupId;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class QuerySubmission
{
    private final SessionRepresentation session;
    private final Map<String, String> extraCredentials;
    private final String slug;
    private final String query;
    private final Duration queryElapsedTime;
    private final Duration queryQueueDuration;
    private final ResourceGroupId resourceGroupId;

    @JsonCreator
    public QuerySubmission(
            @JsonProperty("session") SessionRepresentation session,
            @JsonProperty("extraCredentials") Map<String, String> extraCredentials,
            @JsonProperty("slug") String slug,
            @JsonProperty("query") String query,
            @JsonProperty("queryElapsedTime") Duration queryElapsedTime,
            @JsonProperty("queryQueueDuration") Duration queryQueueDuration,
            @JsonProperty("resourceGroupId") ResourceGroupId resourceGroupId)
    {
        this.session = requireNonNull(session, "session is null");
        this.extraCredentials = ImmutableMap.copyOf(requireNonNull(extraCredentials, "extraCredentials is null"));
        this.slug = requireNonNull(slug, "slug is null");
        this.query = requireNonNull(query, "query is null");
        this.queryElapsedTime = requireNonNull(queryElapsedTime, "queryElapsedTime is null");
        this.queryQueueDuration = requireNonNull(queryQueueDuration, "queryQueueDuration is null");
        this.resourceGroupId = requireNonNull(resourceGroupId, "resourceGroupId is null");
    }

    @JsonProperty
    public SessionRepresentation getSession()
    {
        return session;
    }

    public Session getSession(SessionPropertyManager sessionPropertyManager)
    {
        return session.toSession(sessionPropertyManager, extraCredentials);
    }

    @JsonProperty
    public Map<String, String> getExtraCredentials()
    {
        return extraCredentials;
    }

    @JsonProperty
    public String getSlug()
    {
        return slug;
    }

    @JsonProperty
    public String getQuery()
    {
        return query;
    }

    @JsonProperty
    public Duration getQueryElapsedTime()
    {
        return queryElapsedTime;
    }

    @JsonProperty
    public Duration getQueryQueueDuration()
    {
        return queryQueueDuration;
    }

    @JsonProperty
    public ResourceGroupId getResourceGroupId()
    {
        return resourceGroupId;
    }
}
