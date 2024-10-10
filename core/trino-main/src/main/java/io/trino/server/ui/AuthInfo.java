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
package io.trino.server.ui;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

import java.util.Optional;

@Immutable
public record AuthInfo(String authType, boolean isPasswordAllowed, boolean isAuthenticated, Optional<String> username)
{
    @Override
    @JsonProperty
    public String authType()
    {
        return authType;
    }

    @Override
    @JsonProperty
    public boolean isPasswordAllowed()
    {
        return isPasswordAllowed;
    }

    @Override
    @JsonProperty
    public boolean isAuthenticated()
    {
        return isAuthenticated;
    }

    @Override
    @JsonProperty
    public Optional<String> username()
    {
        return username;
    }
}
