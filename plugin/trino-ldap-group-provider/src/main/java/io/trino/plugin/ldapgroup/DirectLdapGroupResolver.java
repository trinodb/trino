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
package io.trino.plugin.ldapgroup;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;

import javax.naming.NamingException;

import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

final class DirectLdapGroupResolver
        implements LdapGroupResolver
{
    private static final Logger log = Logger.get(DirectLdapGroupResolver.class);

    private final LdapGroupSearch groupSearch;
    private final String groupSearchMemberPredicate;

    @Inject
    public DirectLdapGroupResolver(
            LdapGroupSearch groupSearch,
            LdapFilteringGroupProviderConfig filteringConfig)
    {
        this.groupSearch = requireNonNull(groupSearch, "groupSearch is null");
        this.groupSearchMemberPredicate = String.format("%s={0}", filteringConfig.getLdapGroupsSearchMemberAttribute());
    }

    @Override
    public Set<String> resolveGroups(String memberDistinguishedName)
    {
        try {
            return groupSearch.searchGroups(memberDistinguishedName, groupSearchMemberPredicate, "search").stream()
                    .map(LdapGroup::name)
                    .collect(toImmutableSet());
        }
        catch (NamingException e) {
            log.error(e, "LDAP group search for member [%s] failed", memberDistinguishedName);
            return ImmutableSet.of();
        }
    }
}
