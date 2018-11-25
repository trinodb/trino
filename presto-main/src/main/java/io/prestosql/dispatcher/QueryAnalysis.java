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
import io.prestosql.Session;
import io.prestosql.SessionRepresentation;
import io.prestosql.metadata.SessionPropertyManager;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class QueryAnalysis
{
    private final SessionRepresentation session;
    private final Map<String, String> extraCredentials;
    private final String query;

    @JsonCreator
    public QueryAnalysis(
            @JsonProperty("session") SessionRepresentation session,
            @JsonProperty("extraCredentials") Map<String, String> extraCredentials,
            @JsonProperty("query") String query)
    {
        this.session = requireNonNull(session, "session is null");
        this.extraCredentials = ImmutableMap.copyOf(requireNonNull(extraCredentials, "extraCredentials is null"));
        this.query = requireNonNull(query, "query is null");
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
    public String getQuery()
    {
        return query;
    }
}
