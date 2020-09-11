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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.MoreObjects.firstNonNull;
import static java.lang.Boolean.TRUE;
import static java.util.Objects.requireNonNull;

public class ImpersonationRule
{
    private final Pattern originalUserPattern;
    private final Pattern newUserPattern;
    private final boolean allow;

    @JsonCreator
    public ImpersonationRule(
            @JsonProperty("originalUser") Pattern originalUserPattern,
            @JsonProperty("newUser") Pattern newUserPattern,
            @JsonProperty("allow") Boolean allow)
    {
        this.originalUserPattern = requireNonNull(originalUserPattern, "originalUserPattern is null");
        this.newUserPattern = requireNonNull(newUserPattern, "newUserPattern is null");
        this.allow = firstNonNull(allow, TRUE);
    }

    public Optional<Boolean> match(String originalUser, String newUser)
    {
        Matcher matcher = originalUserPattern.matcher(originalUser);

        if (matcher.matches() && newUserPattern.matcher(newUser).matches()) {
            return Optional.of(allow);
        }
        return Optional.empty();
    }
}
