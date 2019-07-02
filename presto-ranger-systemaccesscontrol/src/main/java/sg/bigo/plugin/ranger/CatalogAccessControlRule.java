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
package sg.bigo.plugin.ranger;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

public class CatalogAccessControlRule
{
    private final boolean allow;
    private final boolean readonly;
    private final Optional<Pattern> userRegex;
    private final Optional<Pattern> catalogRegex;

    static class MatchResult
    {
        private final Optional<Boolean> allow;
        private final Optional<Boolean> readonly;

        MatchResult(Optional<Boolean> allow, Optional<Boolean> readonly)
        {
            this.allow = allow;
            this.readonly = readonly;
        }

        public Optional<Boolean> isAllow()
        {
            return allow;
        }

        public Optional<Boolean> isReadOnly()
        {
            return readonly;
        }
    }

    @JsonCreator
    public CatalogAccessControlRule(
            @JsonProperty("allow") boolean allow,
            @JsonProperty("readonly") boolean readonly,
            @JsonProperty("user") Optional<Pattern> userRegex,
            @JsonProperty("catalog") Optional<Pattern> catalogRegex)
    {
        this.allow = allow;
        this.readonly = readonly;
        this.userRegex = requireNonNull(userRegex, "userRegex is null");
        this.catalogRegex = requireNonNull(catalogRegex, "catalogRegex is null");
    }

    public MatchResult match(String user, String catalog)
    {
        if (userRegex.map(regex -> regex.matcher(user).matches()).orElse(true) &&
                catalogRegex.map(regex -> regex.matcher(catalog).matches()).orElse(true)) {
            return new MatchResult(Optional.of(allow), Optional.of(readonly));
        }
        return new MatchResult(Optional.empty(), Optional.empty());
    }
}
