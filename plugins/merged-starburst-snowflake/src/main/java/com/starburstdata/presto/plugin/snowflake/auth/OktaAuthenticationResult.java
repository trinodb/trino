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
package com.starburstdata.presto.plugin.snowflake.auth;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class OktaAuthenticationResult
{
    private final Optional<String> stateToken;
    private final String sessionToken;
    private final String user;

    public OktaAuthenticationResult(String stateToken, String sessionToken, String user)
    {
        this.stateToken = Optional.of(requireNonNull(stateToken, "stateToken is null"));
        this.sessionToken = requireNonNull(sessionToken, "sessionToken is null");
        this.user = requireNonNull(user, "user is null");
    }

    public OktaAuthenticationResult(String sessionToken, String user)
    {
        this.stateToken = Optional.empty();
        this.sessionToken = requireNonNull(sessionToken, "sessionToken is null");
        this.user = requireNonNull(user, "user is null");
    }

    public Optional<String> getStateToken()
    {
        return stateToken;
    }

    public String getSessionToken()
    {
        return sessionToken;
    }

    public String getUser()
    {
        return user;
    }
}
