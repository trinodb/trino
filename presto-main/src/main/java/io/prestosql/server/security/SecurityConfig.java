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
package io.prestosql.server.security;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;

import javax.validation.constraints.NotNull;

import java.util.List;
import java.util.Optional;

@DefunctConfig({
        "http.server.authentication.enabled",
        "http-server.authentication.allow-forwarded-https",
        "dispatcher.forwarded-header"})
public class SecurityConfig
{
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private List<String> authenticationTypes = ImmutableList.of();

    @NotNull
    public List<String> getAuthenticationTypes()
    {
        return authenticationTypes;
    }

    public SecurityConfig setAuthenticationTypes(List<String> authenticationTypes)
    {
        this.authenticationTypes = ImmutableList.copyOf(authenticationTypes);
        return this;
    }

    @Config("http-server.authentication.type")
    @ConfigDescription("Ordered list of authentication types")
    public SecurityConfig setAuthenticationTypes(String types)
    {
        authenticationTypes = Optional.ofNullable(types).map(SPLITTER::splitToList).orElse(null);
        return this;
    }
}
