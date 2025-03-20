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
package io.trino.spi.resourcegroups;

import io.trino.spi.session.ResourceEstimates;

import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;

import static java.util.Objects.requireNonNull;

public final class SelectionCriteria
{
    private final boolean authenticated;
    private final String user;
    private final Set<String> userGroups;
    private final String originalUser;
    private final Optional<String> authenticatedUser;
    private final Optional<String> source;
    private final Set<String> clientTags;
    private final ResourceEstimates resourceEstimates;
    private final Optional<String> queryType;

    public SelectionCriteria(
            boolean authenticated,
            String user,
            Set<String> userGroups,
            String originalUser,
            Optional<String> authenticatedUser,
            Optional<String> source,
            Set<String> clientTags,
            ResourceEstimates resourceEstimates,
            Optional<String> queryType)
    {
        this.authenticated = authenticated;
        this.user = requireNonNull(user, "user is null");
        this.userGroups = requireNonNull(userGroups, "userGroups is null");
        this.originalUser = requireNonNull(originalUser, "originalUser is null");
        this.authenticatedUser = requireNonNull(authenticatedUser, "authenticatedUser is null");
        this.source = requireNonNull(source, "source is null");
        this.clientTags = Set.copyOf(requireNonNull(clientTags, "clientTags is null"));
        this.resourceEstimates = requireNonNull(resourceEstimates, "resourceEstimates is null");
        this.queryType = requireNonNull(queryType, "queryType is null");
    }

    /**
     * @deprecated Use {@link #SelectionCriteria(boolean, String, Set, String, Optional, Optional, Set, ResourceEstimates, Optional)} instead.
     */
    @Deprecated(since = "474", forRemoval = true)
    public SelectionCriteria(
            boolean authenticated,
            String user,
            Set<String> userGroups,
            Optional<String> source,
            Set<String> clientTags,
            ResourceEstimates resourceEstimates,
            Optional<String> queryType)
    {
        this(
                authenticated,
                user,
                userGroups,
                user,
                Optional.empty(),
                source,
                clientTags,
                resourceEstimates,
                queryType);
    }

    public boolean isAuthenticated()
    {
        return authenticated;
    }

    public String getUser()
    {
        return user;
    }

    public Set<String> getUserGroups()
    {
        return userGroups;
    }

    public String getOriginalUser()
    {
        return originalUser;
    }

    public Optional<String> getAuthenticatedUser()
    {
        return authenticatedUser;
    }

    public Optional<String> getSource()
    {
        return source;
    }

    public Set<String> getTags()
    {
        return clientTags;
    }

    public ResourceEstimates getResourceEstimates()
    {
        return resourceEstimates;
    }

    public Optional<String> getQueryType()
    {
        return queryType;
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", SelectionCriteria.class.getSimpleName() + "[", "]")
                .add("authenticated=" + authenticated)
                .add("user='" + user + "'")
                .add("userGroups=" + userGroups)
                .add("originalUser=" + originalUser)
                .add("authenticatedUser=" + authenticatedUser)
                .add("source=" + source)
                .add("clientTags=" + clientTags)
                .add("resourceEstimates=" + resourceEstimates)
                .add("queryType=" + queryType)
                .toString();
    }
}
