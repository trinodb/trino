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

public class ColumnConstraint
{
    private final String name;
    private final boolean allowed;
    private final Optional<String> mask;
    private final Optional<ExpressionEnvironment> maskEnvironment;

    @JsonCreator
    public ColumnConstraint(
            @JsonProperty("name") String name,
            @JsonProperty("allow") Optional<Boolean> allow,
            @JsonProperty("mask") Optional<String> mask,
            @JsonProperty("mask_environment") Optional<ExpressionEnvironment> maskEnvironment)
    {
        this.name = name;
        this.allowed = allow.orElse(true);
        this.mask = mask;
        this.maskEnvironment = maskEnvironment;
    }

    public String getName()
    {
        return name;
    }

    public boolean isAllowed()
    {
        return allowed;
    }

    public Optional<String> getMask()
    {
        return mask;
    }

    public Optional<ExpressionEnvironment> getMaskEnvironment()
    {
        return maskEnvironment;
    }
}
