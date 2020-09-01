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
package io.prestosql.plugin.base.security;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;

public class ColumnConstraint
{
    private final String name;
    private final boolean allowed;

    @JsonCreator
    public ColumnConstraint(
            @JsonProperty("name") String name,
            @JsonProperty("allow") Optional<Boolean> allow)
    {
        this.name = name;
        this.allowed = allow.orElse(true);
    }

    public String getName()
    {
        return name;
    }

    public boolean isAllowed()
    {
        return allowed;
    }
}
