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
package io.trino.plugin.neo4j;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import jakarta.validation.constraints.NotNull;

import java.net.URI;

public class Neo4jConnectorConfig
{
    private URI uri;
    private String authType = "basic";
    private String basicAuthUser = "neo4j";
    private String basicAuthPassword = "";
    private String bearerAuthToken;

    @NotNull
    public URI getURI()
    {
        return uri;
    }

    @Config("neo4j.uri")
    @ConfigDescription("URI for Neo4j instance")
    public Neo4jConnectorConfig setURI(URI uri)
    {
        this.uri = uri;
        return this;
    }

    @NotNull
    public String getAuthType()
    {
        return this.authType;
    }

    @Config("neo4j.auth.type")
    @ConfigDescription("Authentication type")
    public Neo4jConnectorConfig setAuthType(String type)
    {
        this.authType = type;
        return this;
    }

    @NotNull
    public String getBasicAuthUser()
    {
        return this.basicAuthUser;
    }

    @Config("neo4j.auth.basic.user")
    @ConfigDescription("User for basic auth")
    public Neo4jConnectorConfig setBasicAuthUser(String user)
    {
        this.basicAuthUser = user;
        return this;
    }

    @NotNull
    public String getBasicAuthPassword()
    {
        return this.basicAuthPassword;
    }

    @Config("neo4j.auth.basic.password")
    @ConfigDescription("Password for basic auth")
    @ConfigSecuritySensitive
    public Neo4jConnectorConfig setBasicAuthPassword(String password)
    {
        this.basicAuthPassword = password;
        return this;
    }

    public String getBearerAuthToken()
    {
        return this.bearerAuthToken;
    }

    @Config("neo4j.auth.bearer.token")
    @ConfigDescription("Bearer auth token")
    @ConfigSecuritySensitive
    public Neo4jConnectorConfig setBearerAuthToken(String token)
    {
        this.bearerAuthToken = token;
        return this;
    }
}
