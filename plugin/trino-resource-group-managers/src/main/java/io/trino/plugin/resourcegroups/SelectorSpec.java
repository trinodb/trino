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
package io.trino.plugin.resourcegroups;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class SelectorSpec
{
    private final Optional<Pattern> userRegex;
    private final Optional<Pattern> userGroupRegex;
    private final Optional<Pattern> originalUserRegex;
    private final Optional<Pattern> authenticatedUserRegex;
    private final Optional<Pattern> sourceRegex;
    private final Optional<String> queryType;
    private final Optional<List<String>> clientTags;
    private final Optional<SelectorResourceEstimate> selectorResourceEstimate;
    private final ResourceGroupIdTemplate group;

    @JsonCreator
    public SelectorSpec(
            @JsonProperty("user") Optional<Pattern> userRegex,
            @JsonProperty("userGroup") Optional<Pattern> userGroupRegex,
            @JsonProperty("originalUser") Optional<Pattern> originalUserRegex,
            @JsonProperty("authenticatedUser") Optional<Pattern> authenticatedUserRegex,
            @JsonProperty("source") Optional<Pattern> sourceRegex,
            @JsonProperty("queryType") Optional<String> queryType,
            @JsonProperty("clientTags") Optional<List<String>> clientTags,
            @JsonProperty("selectorResourceEstimate") Optional<SelectorResourceEstimate> selectorResourceEstimate,
            @JsonProperty("group") ResourceGroupIdTemplate group)
    {
        this.userRegex = requireNonNull(userRegex, "userRegex is null");
        this.userGroupRegex = requireNonNull(userGroupRegex, "userGroupRegex is null");
        this.originalUserRegex = requireNonNull(originalUserRegex, "originalUserRegex is null");
        this.authenticatedUserRegex = requireNonNull(authenticatedUserRegex, "authenticatedUserRegex is null");
        this.sourceRegex = requireNonNull(sourceRegex, "sourceRegex is null");
        this.queryType = requireNonNull(queryType, "queryType is null");
        this.clientTags = requireNonNull(clientTags, "clientTags is null");
        this.selectorResourceEstimate = requireNonNull(selectorResourceEstimate, "selectorResourceEstimate is null");
        this.group = requireNonNull(group, "group is null");
    }

    public Optional<Pattern> getUserRegex()
    {
        return userRegex;
    }

    public Optional<Pattern> getUserGroupRegex()
    {
        return userGroupRegex;
    }

    public Optional<Pattern> getOriginalUserRegex()
    {
        return originalUserRegex;
    }

    public Optional<Pattern> getAuthenticatedUserRegex()
    {
        return authenticatedUserRegex;
    }

    public Optional<Pattern> getSourceRegex()
    {
        return sourceRegex;
    }

    public Optional<String> getQueryType()
    {
        return queryType;
    }

    public Optional<List<String>> getClientTags()
    {
        return clientTags;
    }

    public Optional<SelectorResourceEstimate> getResourceEstimate()
    {
        return selectorResourceEstimate;
    }

    public ResourceGroupIdTemplate getGroup()
    {
        return group;
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == this) {
            return true;
        }
        if (!(other instanceof SelectorSpec that)) {
            return false;
        }
        return (group.equals(that.group) &&
                userRegex.map(Pattern::pattern).equals(that.userRegex.map(Pattern::pattern)) &&
                userRegex.map(Pattern::flags).equals(that.userRegex.map(Pattern::flags)) &&
                userGroupRegex.map(Pattern::pattern).equals(that.userGroupRegex.map(Pattern::pattern)) &&
                userGroupRegex.map(Pattern::flags).equals(that.userGroupRegex.map(Pattern::flags)) &&
                originalUserRegex.map(Pattern::pattern).equals(that.originalUserRegex.map(Pattern::pattern)) &&
                originalUserRegex.map(Pattern::flags).equals(that.originalUserRegex.map(Pattern::flags)) &&
                authenticatedUserRegex.map(Pattern::pattern).equals(that.authenticatedUserRegex.map(Pattern::pattern)) &&
                authenticatedUserRegex.map(Pattern::flags).equals(that.authenticatedUserRegex.map(Pattern::flags)) &&
                sourceRegex.map(Pattern::pattern).equals(that.sourceRegex.map(Pattern::pattern))) &&
                sourceRegex.map(Pattern::flags).equals(that.sourceRegex.map(Pattern::flags)) &&
                queryType.equals(that.queryType) &&
                clientTags.equals(that.clientTags);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                group,
                userRegex.map(Pattern::pattern),
                userRegex.map(Pattern::flags),
                userGroupRegex.map(Pattern::pattern),
                userGroupRegex.map(Pattern::flags),
                originalUserRegex.map(Pattern::pattern),
                originalUserRegex.map(Pattern::flags),
                authenticatedUserRegex.map(Pattern::pattern),
                authenticatedUserRegex.map(Pattern::flags),
                sourceRegex.map(Pattern::pattern),
                sourceRegex.map(Pattern::flags),
                queryType,
                clientTags);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("resourceGroup", group)
                .add("userRegex", userRegex)
                .add("userFlags", userRegex.map(Pattern::flags))
                .add("userGroupRegex", userGroupRegex)
                .add("userGroupFlags", userGroupRegex.map(Pattern::flags))
                .add("originalUserRegex", originalUserRegex)
                .add("originalUserFlags", originalUserRegex.map(Pattern::flags))
                .add("authenticatedUserRegex", authenticatedUserRegex)
                .add("authenticatedUserFlags", authenticatedUserRegex.map(Pattern::flags))
                .add("sourceRegex", sourceRegex)
                .add("sourceFlags", sourceRegex.map(Pattern::flags))
                .add("queryType", queryType)
                .add("clientTags", clientTags)
                .toString();
    }
}
