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
package io.trino.plugin.base.security;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ExpressionEnvironment
{
    private final Optional<String> user;

    @JsonCreator
    public ExpressionEnvironment(
            @JsonProperty("user") Optional<String> user)
    {
        this.user = requireNonNull(user, "user is null");
    }

    public Optional<String> getUser()
    {
        return user;
    }
}
